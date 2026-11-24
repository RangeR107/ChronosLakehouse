"""sentinel_audit: decide whether a run becomes a release (spec: chronos_daily step 10).

Runs every check on the run's branch, stores each decision in metrics.check_results, and either
returns (the run may publish, with its warnings) or raises SentinelBlocked (nothing is published,
the branches are kept for inspection, the release is recorded as blocked, and the batches the run
processed are held until someone releases or rejects them: jobs/batches.py). The Airflow task
never retries automatically: a failed audit needs a person to look at it.
"""
import logging
from datetime import date

import psycopg
from psycopg.types.json import Jsonb

from jobs.batches import hold
from jobs.context import Context, on_run_branch, run_id, run_time
from jobs.db import ensure_metrics_schema
from sentinel.checks import anomalies, rules
from sentinel.checks.batch import bronze_lines, window
from sentinel.checks.result import Result
from sentinel.release import record_blocked

log = logging.getLogger(__name__)


class SentinelBlocked(Exception):
    """A blocking check failed: the run must not publish."""


def save_results(conn: psycopg.Connection, namespace: str, business_date: date,
                 results: list[Result]) -> None:
    with conn.cursor() as cur:
        # A rerun of the same run replaces its earlier decisions instead of adding to them.
        cur.execute("DELETE FROM metrics.check_results WHERE namespace = %s AND run_id = %s",
                    (namespace, run_id(business_date)))
        cur.executemany("""
            INSERT INTO metrics.check_results (namespace, run_id, business_date, table_name,
                partition_key, check_name, method, severity, observed, expected, status, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            [(namespace, run_id(business_date), business_date, r.table, r.partition, r.check, r.method,
              r.severity, r.observed, r.expected, r.status, Jsonb(r.detail)) for r in results])
    conn.commit()


# The console shows a sample of each run's quarantined lines; silver.quarantine keeps them all.
QUARANTINE_SAMPLE = 500


def save_quarantine(conn: psycopg.Connection, ctx: Context, business_date: date) -> None:
    rows = (window(ctx, business_date, ctx.spark.table(ctx.table("silver", "quarantine")))
            .orderBy("source_file", "line_number").limit(QUARANTINE_SAMPLE)
            .select("source_file", "line_number", "reasons", "raw").collect())
    with conn.cursor() as cur:
        cur.execute("DELETE FROM metrics.quarantine WHERE namespace = %s AND run_id = %s",
                    (ctx.namespace_prefix, run_id(business_date)))
        cur.executemany("""
            INSERT INTO metrics.quarantine (namespace, run_id, business_date, source_file,
                                            line_number, reasons, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            [(ctx.namespace_prefix, run_id(business_date), business_date, r.source_file,
              r.line_number, Jsonb(list(r.reasons)), r.raw) for r in rows])
    conn.commit()


@on_run_branch
def sentinel_audit(ctx: Context, business_date: date) -> dict:
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        results = (rules.keys(ctx) + rules.referential(ctx) + rules.reconciliation(ctx)
                   + rules.contract_rates(ctx, business_date) + rules.new_fields(ctx, business_date)
                   + rules.landing(ctx, business_date)
                   + anomalies.anomaly_checks(ctx, conn, business_date))
        save_results(conn, ctx.namespace_prefix, business_date, results)
        save_quarantine(conn, ctx, business_date)
    failed = [r for r in results if r.status == "fail"]
    blocking = [f"{r.table} {r.check}" for r in failed if r.blocks]
    warnings = [f"{r.table} {r.check}" for r in failed if not r.blocks]
    if blocking:
        record_blocked(ctx, business_date, blocking)
        # Hold what this run processed, so the next run doesn't take the bad data in again
        # (jobs/batches.py). Nothing landed is still a decision worth recording.
        batches = [r[0] for r in bronze_lines(ctx, business_date).select("ingested_at")
                   .distinct().collect()] or [run_time(business_date)]
        hold(ctx, business_date, batches, blocking)
        log.error("%s blocked by %s", business_date, blocking)
        raise SentinelBlocked(f"{business_date}: {', '.join(blocking)}")
    return {"checks": len(results), "warnings": warnings}
