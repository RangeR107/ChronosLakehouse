"""Batches: what one run landed in bronze, and what happens to it when the audit blocks.

Every line a run lands carries that run's replay time as ingested_at, so one ingested_at value
names one batch. A run processes every batch after the last published one, up to its own start.

A blocked run's batches are *held* (docs/decisions.md, 2026-11-24). Without that, a blocked
batch's bad data stays in bronze and the next run takes it in again, with two bad endings: the
checks fire again on every later run (a broken schema is 50% of two days, 33% of three, ...), or
they don't, and the bad data is published a day late. A held batch waits until someone decides:

    python -m jobs.batches release 2017-01-05   # a false alarm, or the cause is fixed
    python -m jobs.batches reject 2017-01-05    # the data is bad: never process it

A released batch is taken in by the next run, then marked published with that run's release. A
rejected batch is never processed. Without a database (unit tests) there are no holds.
"""
import argparse
from datetime import date, datetime

import psycopg
from psycopg.types.json import Jsonb
from pyspark.sql import Column
from pyspark.sql import functions as F

from jobs.context import Context, run_id, run_time
from jobs.db import conninfo, ensure_metrics_schema

HELD, RELEASED, REJECTED, PUBLISHED = "held", "released", "rejected", "published"


def ts(t: datetime) -> Column:
    return F.lit(t.isoformat(sep=" ")).cast("timestamp_ntz")


def holds(ctx: Context) -> dict[datetime, str]:
    """ingested_at -> state, for every batch an audit ever held."""
    if not ctx.source_conninfo:
        return {}
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        return dict(conn.execute("SELECT ingested_at, state FROM metrics.batch_holds "
                                 "WHERE namespace = %s", (ctx.namespace_prefix,)).fetchall())


def in_batch(ctx: Context, business_date: date, after: datetime | None,
             column: str = "ingested_at") -> Column:
    """The rows a run for business_date processes.

    after: the ingestion time already published (None at first). Held and rejected batches are
    left out; released ones are taken in even though they are older.
    """
    c = F.col(column)
    state = holds(ctx)
    newer = c > ts(after) if after is not None else F.lit(True)
    released = [ts(t) for t, s in state.items() if s == RELEASED]
    if released:
        newer = newer | c.isin(*released)
    cond = (c <= ts(run_time(business_date))) & newer
    excluded = [ts(t) for t, s in state.items() if s in (HELD, REJECTED)]
    return cond & ~c.isin(*excluded) if excluded else cond


def hold(ctx: Context, business_date: date, batches: list[datetime], reasons: list[str]) -> None:
    """A blocked run holds every batch it processed (a released one is held again)."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        conn.cursor().executemany("""
            INSERT INTO metrics.batch_holds (namespace, ingested_at, run_id, business_date,
                                             state, reasons, decided_at)
            VALUES (%s, %s, %s, %s, 'held', %s, NULL)
            ON CONFLICT (namespace, ingested_at) DO UPDATE SET state = 'held',
                run_id = EXCLUDED.run_id, business_date = EXCLUDED.business_date,
                reasons = EXCLUDED.reasons, decided_at = NULL""",
            [(ctx.namespace_prefix, b, run_id(business_date), business_date, Jsonb(reasons))
             for b in batches])


def decide(ctx: Context, business_date: date, state: str) -> int:
    """Release or reject what the run for business_date held; returns how many batches."""
    assert state in (RELEASED, REJECTED)
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        return conn.execute("""UPDATE metrics.batch_holds SET state = %s, decided_at = now()
                               WHERE namespace = %s AND run_id = %s AND state = 'held'""",
                            (state, ctx.namespace_prefix, run_id(business_date))).rowcount


def mark_published(conn: psycopg.Connection, ctx: Context, business_date: date) -> None:
    """Released batches this run took in are now in a release: never take them in again."""
    conn.execute("""UPDATE metrics.batch_holds SET state = 'published'
                    WHERE namespace = %s AND state = 'released' AND ingested_at <= %s""",
                 (ctx.namespace_prefix, run_time(business_date)))


def main(argv=None):
    p = argparse.ArgumentParser(description="Release or reject a blocked run's batches.")
    p.add_argument("decision", choices=["release", "reject"])
    p.add_argument("business_date", type=date.fromisoformat)
    p.add_argument("--namespace", default="", help="namespace prefix ('' is the main lake)")
    a = p.parse_args(argv)
    # Only the database is needed: no Spark session.
    ctx = Context(spark=None, landing_dir=None, namespace_prefix=a.namespace,  # type: ignore[arg-type]
                  source_conninfo=conninfo())
    n = decide(ctx, a.business_date, RELEASED if a.decision == "release" else REJECTED)
    print(f"{a.decision}: {n} batch(es) held by {run_id(a.business_date)}")


if __name__ == "__main__":
    main()
