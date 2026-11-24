"""What a run processed: the bronze lines after the last published watermark, up to run time,
without held or rejected batches (jobs/batches.py).

Every per-day check and profile is about this batch, not about whole tables, so a day is judged on
what arrived that day. After a blocked day, the next run's batch holds both days (spec:
Publishing, "What a run processes"), and so do its profiles.
"""
from datetime import date

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.batches import in_batch
from jobs.context import Context, run_time

TS = "timestamp_ntz"


def published_watermark(ctx: Context, business_date: date):
    """The watermark of the latest release published before this date, or None."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        row = conn.execute("""SELECT watermark FROM metrics.releases
                              WHERE namespace = %s AND status = 'published'
                                AND business_date < %s
                              ORDER BY business_date DESC LIMIT 1""",
                           (ctx.namespace_prefix, business_date)).fetchone()
    return row[0] if row else None


def window(ctx: Context, business_date: date, df: DataFrame, column: str = "ingested_at"):
    """The rows of this run's batch: the same rule bronze_to_silver used (jobs/batches.py)."""
    return df.where(in_batch(ctx, business_date, published_watermark(ctx, business_date), column))


def bronze_lines(ctx: Context, business_date: date) -> DataFrame:
    return window(ctx, business_date, ctx.spark.table(ctx.table("bronze", "events")))


def ingest_day_lines(ctx: Context, business_date: date) -> DataFrame:
    """Only what this run landed itself: volume is judged per ingest day (spec: Checks table),
    so a released batch taken in on the same run doesn't look like a spike."""
    landed = F.lit(run_time(business_date).isoformat(sep=" ")).cast(TS)
    return ctx.spark.table(ctx.table("bronze", "events")).where(F.col("ingested_at") == landed)


def silver_events(ctx: Context, business_date: date) -> DataFrame:
    return window(ctx, business_date, ctx.spark.table(ctx.table("silver", "events")))


def batch_rows(ctx: Context, business_date: date, silver_table: str) -> DataFrame:
    """A typed silver table's rows for the events in this run's batch."""
    ids = silver_events(ctx, business_date).select("event_id")
    return ctx.spark.table(ctx.table("silver", silver_table)).join(ids, "event_id")
