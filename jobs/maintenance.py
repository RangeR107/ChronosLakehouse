"""maintenance: compact small files, expire snapshots nobody needs, remove orphan files (spec:
Orchestration, maintenance_weekly).

Why it exists: every daily run commits to every table, so each table gains small data files and
a metadata file a day. Full-table reads, the audit's above all, slowed down day by day (15 Feb:
27.6 s a run). Compacting halved the key checks (docs/decisions.md).

What it never does:
  - Commit to main while a run is in progress (a blocked run's kept branches don't count). fast_forward can only move main along the
    branch's own history, so a compaction commit in between would make that run's publish fail.
    So it refuses while any table has a run branch or a release is still 'publishing', and in
    Airflow it shares a one-slot pool with open_run, so no run can open while it works.
  - Lose a release. Every release is a tag kept for TAG_RETAIN_DAYS, and expiry keeps every
    snapshot a tag or branch points at, so every published release stays readable.

Iceberg stamps snapshots with wall-clock time, and a replay runs months of business dates in an
hour, so an age cut-off would expire nothing. Expiry removes every snapshot that no tag or
branch needs, keeping main's latest one.
"""
from datetime import date, datetime

import psycopg

from jobs.context import Context, on_branch
from jobs.db import ensure_metrics_schema

LAYERS = ("bronze", "silver", "gold")
# Fewer than this many small files in a table: nothing to gain from rewriting them.
MIN_INPUT_FILES = 2


class LakeBusy(RuntimeError):
    """A run's branches are open, or a publish is unfinished: maintenance must wait."""


def lake_tables(ctx: Context) -> list[str]:
    out = []
    for layer in LAYERS:
        ns = ctx.namespace(layer)
        if ctx.spark.sql(f"SHOW NAMESPACES IN local LIKE '{ns.split('.', 1)[1]}'").count():
            out += [f"{ns}.{r.tableName}" for r in ctx.spark.sql(f"SHOW TABLES IN {ns}").collect()]
    return out


def branches(ctx: Context, tables: list[str]) -> list[tuple[str, str]]:
    """(table, branch) for every branch besides main."""
    return [(t, r.name) for t in tables for r in ctx.spark.sql(
        f"SELECT name FROM {t}.refs WHERE type = 'BRANCH' AND name <> 'main'").collect()]


def blocked_runs(ctx: Context) -> dict[str, bool]:
    """run_id -> whether every batch it held has been decided (released, rejected, published)."""
    if not ctx.source_conninfo:
        return {}
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        return dict(conn.execute("""
            SELECT r.run_id, coalesce(bool_and(h.state <> 'held'), true)
            FROM metrics.releases r
            LEFT JOIN metrics.batch_holds h ON h.namespace = r.namespace AND h.run_id = r.run_id
            WHERE r.namespace = %s AND r.status = 'blocked' GROUP BY r.run_id""",
            (ctx.namespace_prefix,)).fetchall())


def ensure_idle(ctx: Context, tables: list[str]) -> list[tuple[str, str]]:
    """Refuse while a run is in progress; return the branches of decided blocked runs.

    A blocked run keeps its branches for inspection, but they are dead ends: never
    fast-forwarded, and the next run makes its own. So they don't make the lake busy. Once
    every batch the run held is decided, inspection is over and the branches can go.
    """
    from sentinel.release import unfinished_publish  # sentinel imports jobs, not the reverse
    blocked = blocked_runs(ctx)
    found = branches(ctx, tables)
    busy = [f"{t}@{b}" for t, b in found if b not in blocked]
    stuck = unfinished_publish(ctx)
    if busy or stuck:
        raise LakeBusy(f"refusing to maintain: open branches {busy[:3]}"
                       f"{' ...' if len(busy) > 3 else ''}, unfinished publish {stuck}")
    return [(t, b) for t, b in found if blocked.get(b)]


def compact(ctx: Context, table: str) -> dict:
    name = table.split(".", 1)[1]
    r = ctx.spark.sql(f"CALL local.system.rewrite_data_files(table => '{name}', "
                      f"options => map('min-input-files', '{MIN_INPUT_FILES}'))").collect()[0]
    ctx.spark.sql(f"CALL local.system.rewrite_manifests('{name}')").collect()
    return {"rewritten": r.rewritten_data_files_count, "added": r.added_data_files_count}


def expire(ctx: Context, table: str, now: datetime) -> dict:
    name = table.split(".", 1)[1]
    r = ctx.spark.sql(f"CALL local.system.expire_snapshots(table => '{name}', "
                      f"older_than => TIMESTAMP '{now.isoformat(sep=' ')}', "
                      f"retain_last => 1)").collect()[0]
    return {"deleted_data_files": r.deleted_data_files_count,
            "deleted_manifests": r.deleted_manifest_files_count}


def remove_orphans(ctx: Context, table: str) -> int:
    # Default age limit (3 days of wall-clock time): a file younger than that may belong to a
    # write still in progress, which is exactly what must not be deleted.
    name = table.split(".", 1)[1]
    return len(ctx.spark.sql(
        f"CALL local.system.remove_orphan_files(table => '{name}')").collect())


def maintain(ctx: Context, business_date: date | None = None) -> dict:
    """Compact, expire and clean every lake table; report what each job changed per table."""
    tables = lake_tables(ctx)
    finished = ensure_idle(ctx, tables)
    now = datetime.now()  # snapshots carry wall-clock commit times, so the cut-off must too
    report = {}
    with on_branch(ctx, None):  # main only
        for t, b in finished:
            ctx.spark.sql(f"ALTER TABLE {t} DROP BRANCH `{b}`")
        for t in tables:
            report[t] = {**compact(ctx, t), **expire(ctx, t, now),
                         "orphans_removed": remove_orphans(ctx, t)}
    total = {k: sum(v[k] for v in report.values()) for k in next(iter(report.values()), {})}
    return {"business_date": str(business_date) if business_date else None,
            "tables": len(tables), "blocked_branches_dropped": len(finished), "total": total, "per_table": report}
