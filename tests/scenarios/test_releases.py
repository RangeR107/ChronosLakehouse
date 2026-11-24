"""Releases: the release consistency test and a blocked audit (spec: Publishing, "Failure
handling"; Testing and evaluation, "Release consistency test").
"""
import uuid
from datetime import date
from pathlib import Path

import psycopg
import pytest
from conftest import throwaway_database  # tests/ is on sys.path under pytest
from pyspark.sql import functions as F

import sentinel.release as release_module
from generator.replay import replay
from generator.source_db import conninfo
from jobs import maintenance
from jobs.batches import RELEASED, decide
from jobs.context import Context, release_id
from jobs.run_day import run_day
from sentinel.gate import SentinelBlocked
from sentinel.release import PublishUnfinished, open_run, publish_release

MINI = Path(__file__).resolve().parents[1] / "fixtures" / "olist_mini"
START = date(2017, 1, 1)

pytestmark = pytest.mark.postgres


@pytest.fixture
def lake(spark, tmp_path):
    """An empty lake and source; yields (ctx, conn). Days are written one at a time (write)."""
    with throwaway_database() as conn:
        ctx = Context(spark, tmp_path / "landing", namespace_prefix=f"rel{uuid.uuid4().hex[:6]}_",
                      source_conninfo=conninfo(conn.info.dbname), reference_dir=MINI)
        yield ctx, conn


def write(ctx: Context, conn: psycopg.Connection, d: date) -> None:
    """The producer writes a date just before the pipeline runs it, as a real source would.

    Writing every date first would let day 1's extract see day 10's customer addresses: the
    source only keeps the current row (docs/decisions.md, "Two bugs the fixture truth test found").
    """
    replay(MINI, ctx.landing_dir, d, d, purchased_from=START, conn=conn)


def read(conn: psycopg.Connection, query: str, args=()) -> list[tuple]:
    """Read and end the transaction: an open read would block the pipeline's schema changes."""
    with conn.transaction():
        return conn.execute(query, args).fetchall()


def releases(conn: psycopg.Connection, ctx: Context) -> dict:
    return dict(read(conn, "SELECT release_id, status FROM metrics.releases WHERE namespace = %s",
                     (ctx.namespace_prefix,)))


def served(conn: psycopg.Connection) -> str:
    return read(conn, "SELECT release_id FROM serving.load_state")[0][0]


def main_snapshots(ctx: Context) -> dict:
    return {t: ctx.spark.sql(f"SELECT snapshot_id FROM {t}.refs WHERE name = 'main'")
            .collect()[0][0] for t in release_module.tables(ctx)}


def run_days(ctx, conn, first, last):
    d = first
    while d <= last:
        write(ctx, conn, d)
        run_day(ctx, d)
        d = date.fromordinal(d.toordinal() + 1)


def test_a_crash_between_fast_forwards_leaves_serving_and_completes_on_rerun(lake, monkeypatch):
    ctx, conn = lake
    run_days(ctx, conn, START, date(2017, 1, 5))
    assert served(conn) == release_id(date(2017, 1, 5))

    # Publish dies after a few tables have moved: the next fast_forward call raises.
    real_sql, calls = ctx.spark.sql, []

    def dying_sql(query, *args, **kwargs):
        if "system.fast_forward" in query:
            calls.append(query)
            if len(calls) == 4:
                raise RuntimeError("simulated crash during publish")
        return real_sql(query, *args, **kwargs)

    monkeypatch.setattr(ctx.spark, "sql", dying_sql)
    day = date(2017, 1, 6)
    write(ctx, conn, day)
    with pytest.raises(RuntimeError, match="during publish"):
        run_day(ctx, day)
    monkeypatch.setattr(ctx.spark, "sql", real_sql)

    # Some tables moved, the release isn't published, and the dashboards still show 5 Jan.
    assert len(calls) == 4
    assert releases(conn, ctx)[release_id(day)] == "publishing"
    assert served(conn) == release_id(date(2017, 1, 5))
    # No later date may branch from the half-moved main until the publish is finished.
    with pytest.raises(PublishUnfinished, match="still publishing"):
        open_run(ctx, date(2017, 1, 7))

    # Rerunning publish skips the tables already advanced and finishes the release.
    result = publish_release(ctx, day)
    assert result["release_id"] == release_id(day)
    assert releases(conn, ctx)[release_id(day)] == "published"
    snapshots = read(conn, "SELECT snapshots FROM metrics.releases WHERE namespace = %s "
                     "AND release_id = %s", (ctx.namespace_prefix, release_id(day)))[0][0]
    # Every table's main is now where the release says it is.
    assert {t: int(s) for t, s in snapshots.items()} == main_snapshots(ctx)


def test_a_blocked_day_is_held_until_released(lake):
    ctx, conn = lake
    run_days(ctx, conn, START, date(2017, 1, 4))
    main_before = main_snapshots(ctx)
    served_before = served(conn)

    # A file nobody listed appears in 5 Jan's folder: the completeness check blocks the run.
    write(ctx, conn, date(2017, 1, 5))
    stray = ctx.landing_dir / "date=2017-01-05" / "events_2017-01-05T03.jsonl"
    stray.write_text('{"event_id": "not-from-the-producer"}\n')
    with pytest.raises(SentinelBlocked, match="unlisted_files"):
        run_day(ctx, date(2017, 1, 5))
    assert releases(conn, ctx)[release_id(date(2017, 1, 5))] == "blocked"
    assert main_snapshots(ctx) == main_before
    assert served(conn) == served_before
    assert hold_states(conn, ctx) == ["held"]

    # The next date publishes its own data only: 5 Jan's batch waits for a decision. The stray
    # is still there, but it was already reported, so it doesn't block again.
    write(ctx, conn, date(2017, 1, 6))
    run_day(ctx, date(2017, 1, 6))
    assert releases(conn, ctx)[release_id(date(2017, 1, 6))] == "published"
    assert bought_on(ctx, date(2017, 1, 5)) == 0
    # Maintenance still runs: the blocked run's kept branches are not a run in progress.
    assert maintenance.maintain(ctx)["blocked_branches_dropped"] == 0

    # Someone checks it was only the stray file and releases the batch: the next run takes it in.
    assert decide(ctx, date(2017, 1, 5), RELEASED) == 1
    write(ctx, conn, date(2017, 1, 7))
    run_day(ctx, date(2017, 1, 7))
    assert bought_on(ctx, date(2017, 1, 5)) == 2  # o_delivered and o_undated
    assert hold_states(conn, ctx) == ["published"]
    # Decided, so inspection is over: maintenance drops the blocked run's branches.
    assert maintenance.maintain(ctx)["blocked_branches_dropped"] > 0
    assert not [b for b in maintenance.branches(ctx, maintenance.lake_tables(ctx))
                if b[1] == "run_2017-01-05"]


def hold_states(conn, ctx) -> list[str]:
    return [r[0] for r in read(conn, "SELECT state FROM metrics.batch_holds WHERE namespace = %s",
                               (ctx.namespace_prefix,))]


def bought_on(ctx, day) -> int:
    orders = ctx.spark.table(ctx.table("gold", "fact_order_fulfillment"))
    return orders.where(F.to_date("order_purchase_timestamp") == F.lit(day)).count()


def test_maintenance_keeps_every_release_and_refuses_during_a_run(lake):
    ctx, conn = lake
    run_days(ctx, conn, START, date(2017, 1, 7))
    tables = maintenance.lake_tables(ctx)
    rows = lambda t, at="": ctx.spark.sql(f"SELECT * FROM {t} {at}").sort(
        *ctx.spark.table(t).columns).collect()
    before = {t: rows(t) for t in tables}
    fact = ctx.table("gold", "fact_order_fulfillment")
    tag = f"VERSION AS OF '{release_id(date(2017, 1, 5))}'"
    release_before = rows(fact, tag)
    files_before = ctx.spark.sql(f"SELECT count(*) FROM {fact}.files").collect()[0][0]

    report = maintenance.maintain(ctx)
    assert report["total"]["rewritten"] > 0
    # Same rows everywhere, an old release still readable as published, fewer files.
    assert {t: rows(t) for t in tables} == before
    assert rows(fact, tag) == release_before
    assert ctx.spark.sql(f"SELECT count(*) FROM {fact}.files").collect()[0][0] <= files_before

    # While a run's branches are open, maintenance refuses; the run then publishes normally.
    write(ctx, conn, date(2017, 1, 8))
    open_run(ctx, date(2017, 1, 8))
    with pytest.raises(maintenance.LakeBusy, match="open branches"):
        maintenance.maintain(ctx)
    run_day(ctx, date(2017, 1, 8))
    assert releases(conn, ctx)[release_id(date(2017, 1, 8))] == "published"
