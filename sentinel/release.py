"""Releases: open_run, publish_release and clean_up (spec: Publishing, "Run lifecycle").

A day's data becomes visible only as a release: a set of table snapshots audited together.

- open_run recreates the run's branch on every silver and gold table from main, so a rerun
  starts clean. Bronze has no branches: it is append-only and the run reads it from the last
  published watermark.
- The jobs write silver and gold on that branch (jobs/context.py, on_run_branch).
- publish_release fast-forwards each table's main to its branch (dimensions first, facts last),
  tags each snapshot, and records them in metrics.releases. The last write marks the release
  published: that single update is the moment the release exists.
- clean_up drops the run's branches once the release is served; the tags keep the snapshots.

A rerun of a date that is already published republishes nothing (docs/decisions.md): open_run
reports the release, and the run stops there successfully.
"""
import json
import logging
from datetime import date

import psycopg
from psycopg.types.json import Jsonb

from jobs import build_dimensions, build_facts, recompute_aggregates
from jobs.batches import mark_published
from jobs.bronze_to_silver import TABLES as SILVER_TABLES
from jobs.bronze_to_silver import ensure_silver_tables
from jobs.context import Context, on_branch, release_id, run_branch, run_id, run_time
from jobs.db import ensure_metrics_schema
from jobs.merge import create_tables

log = logging.getLogger(__name__)


# How long release tags keep their snapshots, so the restatement report can read any release
# (spec: Publishing, "Read any release"). Snapshot expiry never removes a tagged snapshot.
TAG_RETAIN_DAYS = 3650

# Publish order (spec: "dimensions first and facts last"). Silver goes first: it is the input
# every gold table was built from. The event ledger is last of all, because it holds gold's mark.
GOLD_ORDER = (
    list(build_dimensions.TABLES)
    + list(recompute_aggregates.TABLES)
    + ["dirty_periods"]
    + [t for t in build_facts.TABLES if t not in ("dirty_periods", "fact_order_events")]
    + ["fact_order_events"]
)


def tables(ctx: Context) -> list[str]:
    """Every table a run writes, in publish order."""
    return ([ctx.table("silver", t) for t in SILVER_TABLES]
            + [ctx.table("gold", t) for t in GOLD_ORDER])


def published_release(ctx: Context, business_date: date) -> str | None:
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        row = conn.execute("SELECT release_id FROM metrics.releases WHERE namespace = %s "
                           "AND business_date = %s AND status = 'published'",
                           (ctx.namespace_prefix, business_date)).fetchone()
    return row[0] if row else None


def ensure_all_tables(ctx: Context) -> None:
    """Create every silver and gold table on main, so each can get the run's branch."""
    ensure_silver_tables(ctx)
    create_tables(ctx, "gold", build_dimensions.TABLES)
    create_tables(ctx, "gold", build_facts.TABLES)
    create_tables(ctx, "gold", recompute_aggregates.TABLES)


def refs(ctx: Context, table: str) -> dict[str, int]:
    return {r.name: r.snapshot_id for r in ctx.spark.sql(
        f"SELECT name, snapshot_id FROM {table}.refs").collect()}


class MissingBranch(RuntimeError):
    """A table has no branch for the run being published."""


def run_refs(ctx: Context, table: str, branch: str) -> dict[str, int]:
    """The table's refs, which must include the run's branch.

    The evaluation once read a table's refs without the branch its own open_run had created
    (12 Jun 2018); read straight afterwards, the branch was there. So a missing branch is read
    once more after REFRESH TABLE, which drops any cached copy of the table. If it is still
    missing, the publish stops with the reason rather than a KeyError. The release stays
    'publishing', and rerunning publish_release finishes it (docs/runbook.md).
    """
    r = refs(ctx, table)
    if branch not in r:
        log.warning("%s: branch %s not in its refs; refreshing and reading again", table, branch)
        ctx.spark.sql(f"REFRESH TABLE {table}")
        r = refs(ctx, table)
    if branch not in r:
        raise MissingBranch(f"{table} has no branch {branch}: was open_run run for this date?")
    return r


class PublishUnfinished(RuntimeError):
    """A publish died between fast-forwards: main is half moved, so no run may branch from it."""


def unfinished_publish(ctx: Context) -> str | None:
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        row = conn.execute("SELECT release_id FROM metrics.releases WHERE namespace = %s "
                           "AND status = 'publishing' ORDER BY business_date LIMIT 1",
                           (ctx.namespace_prefix,)).fetchone()
    return row[0] if row else None


def open_run(ctx: Context, business_date: date) -> dict:
    """Recreate the run's branches from main; or report that the date is already published.

    Refuses to start while a release is still 'publishing': rerun publish_release for that date
    first (docs/runbook.md). That is the one failure that must stop later dates.
    """
    done = published_release(ctx, business_date)
    if done is not None:
        log.info("%s already published in release %s; nothing to do", business_date, done)
        return {"already_published": done}
    stuck = unfinished_publish(ctx)
    if stuck is not None:
        raise PublishUnfinished(f"{stuck} is still publishing: rerun publish_release for its "
                                f"date before running {business_date}")
    branch = run_branch(business_date)
    with on_branch(ctx, None):
        ensure_all_tables(ctx)
        for t in tables(ctx):
            ctx.spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('write.wap.enabled' = 'true')")
            if branch in refs(ctx, t):
                ctx.spark.sql(f"ALTER TABLE {t} DROP BRANCH `{branch}`")
            # A table with no snapshot yet has no main to branch from: give it an empty commit.
            if "main" not in refs(ctx, t):
                ctx.spark.sql(f"DELETE FROM {t} WHERE false")
            ctx.spark.sql(f"ALTER TABLE {t} CREATE BRANCH `{branch}`")
    return {"already_published": None, "branch": branch}


def run_warnings(ctx: Context, business_date: date) -> list[str]:
    """The audit's warnings for this run, for the release manifest."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        rows = conn.execute("""SELECT table_name || ' ' || check_name FROM metrics.check_results
                               WHERE namespace = %s AND run_id = %s AND status = 'fail'
                                 AND severity <> 'block' ORDER BY 1""",
                            (ctx.namespace_prefix, run_id(business_date))).fetchall()
    return [r[0] for r in rows]


# Failed warnings that mean the day is published but not complete (spec: Checks table).
INCOMPLETE_CHECKS = ("freshness", "completeness:missing_files")


def is_complete(warnings: list[str]) -> bool:
    return not any(w.split(" ", 1)[1] in INCOMPLETE_CHECKS for w in warnings)


def publish_release(ctx: Context, business_date: date) -> dict:
    """Fast-forward every table to the run's branch, tag it, then mark the release published.

    Safe to rerun after a crash midway: a table whose main already points at the branch head is
    skipped, and the manifest is written whole at the end. Dashboards keep the previous release
    until load_serving switches them.
    """
    rid, branch = release_id(business_date), run_branch(business_date)
    warnings = run_warnings(ctx, business_date)
    # Recorded before main moves: if the publish dies midway, open_run sees 'publishing' and
    # refuses to branch from a half-moved main.
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        conn.execute("""
            INSERT INTO metrics.releases (namespace, release_id, business_date, run_id, status,
                                          warnings)
            VALUES (%s, %s, %s, %s, 'publishing', %s)
            ON CONFLICT (namespace, release_id) DO UPDATE SET status = 'publishing',
                warnings = EXCLUDED.warnings""",
            (ctx.namespace_prefix, rid, business_date, run_id(business_date), Jsonb(warnings)))
    snapshots = {}
    with on_branch(ctx, None):
        for t in tables(ctx):
            r = run_refs(ctx, t, branch)
            if r.get("main") != r[branch]:
                ctx.spark.sql(f"CALL {t.split('.')[0]}.system.fast_forward("
                              f"'{t.split('.', 1)[1]}', 'main', '{branch}')")
            snapshot = refs(ctx, t)["main"]
            ctx.spark.sql(f"ALTER TABLE {t} CREATE OR REPLACE TAG `{rid}` AS OF VERSION "
                          f"{snapshot} RETAIN {TAG_RETAIN_DAYS} DAYS")
            snapshots[t] = snapshot
        watermark = ctx.spark.sql(
            f"SELECT max(ingested_at) FROM {ctx.table('silver', 'events')}").collect()[0][0]
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        with conn.transaction():
            conn.execute("""
                INSERT INTO metrics.releases (namespace, release_id, business_date, run_id,
                                              status, warnings, complete, watermark, snapshots,
                                              published_at)
                VALUES (%s, %s, %s, %s, 'published', %s, %s, %s, %s, %s)
                ON CONFLICT (namespace, release_id) DO UPDATE SET status = 'published',
                    warnings = EXCLUDED.warnings, complete = EXCLUDED.complete,
                    watermark = EXCLUDED.watermark,
                    snapshots = EXCLUDED.snapshots, published_at = EXCLUDED.published_at""",
                (ctx.namespace_prefix, rid, business_date, run_id(business_date),
                 Jsonb(warnings), is_complete(warnings), watermark,
                 Jsonb(snapshots), run_time(business_date)))
            mark_published(conn, ctx, business_date)
    return {"release_id": rid, "tables": len(snapshots), "warnings": warnings,
            "complete": is_complete(warnings)}


def record_blocked(ctx: Context, business_date: date, failures: list) -> None:
    """A blocked run publishes nothing; the manifest says why, and the branches stay."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        conn.execute("""
            INSERT INTO metrics.releases (namespace, release_id, business_date, run_id, status,
                                          warnings)
            VALUES (%s, %s, %s, %s, 'blocked', %s)
            ON CONFLICT (namespace, release_id) DO UPDATE SET status = 'blocked',
                warnings = EXCLUDED.warnings
            WHERE metrics.releases.status <> 'published'""",
            (ctx.namespace_prefix, release_id(business_date), business_date,
             run_id(business_date), Jsonb(failures)))


def release_snapshots(ctx: Context, business_date: date) -> dict[str, int]:
    """The published release for a date: table -> snapshot id."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        row = conn.execute("SELECT snapshots FROM metrics.releases WHERE namespace = %s AND "
                           "business_date = %s AND status = 'published'",
                           (ctx.namespace_prefix, business_date)).fetchone()
    if row is None:
        raise LookupError(f"no published release for {business_date}")
    return row[0] if isinstance(row[0], dict) else json.loads(row[0])


def clean_up(ctx: Context, business_date: date) -> dict:
    """Drop the run's branches once its release is served; the release tags keep the data."""
    branch = run_branch(business_date)
    dropped = 0
    with on_branch(ctx, None):
        for t in tables(ctx):
            if branch in refs(ctx, t):
                ctx.spark.sql(f"ALTER TABLE {t} DROP BRANCH `{branch}`")
                dropped += 1
    return {"branches_dropped": dropped}
