"""recompute_aggregates: the dashboard's metric tables in gold (spec: chronos_daily step 9).

Questions 1 and 2, lateness and delivery promises by delivery month and delivery region, plus an
"all" slice, at the grain of measure_spec.py's q1_q2_delivery_by_month_region.csv.

Only the months build_facts recorded as dirty for this run are recomputed, each one whole, for
every region and the all slice (spec: Late-arriving data, "Dirty periods"): shares and
percentiles depend on every order in the month, so a month is never patched row by row.

Each recomputed period is also appended to fact_daily_metrics_published, once per run (Phase 3
makes that once per release), and the restatement report (question 8) is derived from it: first
published value against latest, with numerators and denominators.
"""
from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from jobs.context import Context, run_id, run_time
from jobs.merge import create_tables, merge_changed

# Spec: "Reliable window", purchases from 1 Jan 2017 up to (not including) 1 Sep 2018.
WINDOW_START, WINDOW_END = "2017-01-01 00:00:00", "2018-09-01 00:00:00"

TABLES = {
    "agg_delivery_by_month_region": """
        delivery_month STRING COMMENT 'yyyy-MM of the delivery date',
        region STRING COMMENT 'IBGE region of the delivery address, or all',
        delivered_orders BIGINT, late_orders BIGINT, late_rate DOUBLE,
        median_days_early DOUBLE, p10_days_early DOUBLE, p90_days_early DOUBLE,
        share_7_days_early DOUBLE""",
    "fact_daily_metrics_published": """
        metric_id STRING, period_type STRING, period_start DATE, period_end DATE,
        slice_type STRING, slice_value STRING, release_id STRING,
        value DOUBLE, numerator DOUBLE, denominator DOUBLE,
        published_at TIMESTAMP_NTZ COMMENT 'replay time of the publishing run'""",
    "agg_restatements": """
        metric_id STRING, period_type STRING, period_start DATE, slice_type STRING,
        slice_value STRING, releases BIGINT,
        first_release STRING, first_value DOUBLE, first_numerator DOUBLE,
        first_denominator DOUBLE, latest_release STRING, latest_value DOUBLE,
        latest_numerator DOUBLE, latest_denominator DOUBLE,
        restatement DOUBLE COMMENT 'latest minus first; a pipeline metric (simulated arrival)'""",
}
PUBLISHED_GRAIN = ["metric_id", "period_type", "period_start", "slice_type", "slice_value",
                   "release_id"]
RESTATEMENT_GRAIN = ["metric_id", "period_type", "period_start", "slice_type", "slice_value"]

# Measures published per row of the aggregate: (metric_id, value, numerator, denominator).
MEASURES = [
    ("q1_late_delivery_rate.late_rate", "late_rate", "late_orders", "delivered_orders"),
    ("q2_delivery_promise.median_days_early", "median_days_early", None, None),
    ("q2_delivery_promise.p10_days_early", "p10_days_early", None, None),
    ("q2_delivery_promise.p90_days_early", "p90_days_early", None, None),
    ("q2_delivery_promise.share_7_days_early", "share_7_days_early", "_early_7",
     "delivered_orders"),
]


def delivered_in_window(ctx: Context) -> DataFrame:
    """Spec "Delivered order": delivered with a delivery date, purchased in the reliable window."""
    f = ctx.spark.table(ctx.table("gold", "fact_order_fulfillment"))
    g = ctx.spark.table(ctx.table("gold", "dim_geography")).select(
        F.col("geography_key").alias("delivery_geography_key"), "region")
    ts = "timestamp_ntz"
    return (f.where((F.col("final_status") == "delivered")
                    & F.col("order_delivered_customer_date").isNotNull()
                    & (F.col("order_purchase_timestamp") >= F.lit(WINDOW_START).cast(ts))
                    & (F.col("order_purchase_timestamp") < F.lit(WINDOW_END).cast(ts)))
            .join(g, "delivery_geography_key", "left")
            .withColumn("delivery_month", F.date_format("order_delivered_customer_date", "yyyy-MM")))


def delivery_stats(df: DataFrame, *group: str) -> DataFrame:
    # percentile() interpolates linearly between ranks, as pandas' quantile does.
    early = F.col("days_early")
    return df.groupBy(*group).agg(
        F.count("*").alias("delivered_orders"),
        F.sum(F.col("is_late").cast("long")).alias("late_orders"),
        F.avg(F.col("is_late").cast("double")).alias("late_rate"),
        F.percentile(early, 0.5).alias("median_days_early"),
        F.percentile(early, 0.1).alias("p10_days_early"),
        F.percentile(early, 0.9).alias("p90_days_early"),
        F.avg((early >= 7).cast("double")).alias("share_7_days_early"))


def delivery_by_month_region(ctx: Context, months: DataFrame | None = None) -> DataFrame:
    d = delivered_in_window(ctx)
    if months is not None:
        d = d.join(months, "delivery_month")
    by_region = delivery_stats(d, "delivery_month", "region")
    overall = delivery_stats(d, "delivery_month").withColumn("region", F.lit("all"))
    return by_region.unionByName(overall).select(
        "delivery_month", "region", "delivered_orders", "late_orders", "late_rate",
        "median_days_early", "p10_days_early", "p90_days_early", "share_7_days_early")


def dirty_months(ctx: Context, business_date: date) -> DataFrame:
    return (ctx.spark.table(ctx.table("gold", "dirty_periods"))
            .where((F.col("run_id") == run_id(business_date))
                   & (F.col("metric_id") == "q1_late_delivery_rate"))
            .select(F.date_format("period_start", "yyyy-MM").alias("delivery_month")).distinct())


def published_rows(agg: DataFrame, business_date: date) -> DataFrame:
    """The aggregate's rows in the published-metrics shape, one row per measure."""
    start = F.to_date(F.concat(F.col("delivery_month"), F.lit("-01")))
    base = agg.withColumn("_early_7", F.col("share_7_days_early") * F.col("delivered_orders"))
    rows = [base.select(
        F.lit(metric).alias("metric_id"), F.lit("month").alias("period_type"),
        start.alias("period_start"), F.last_day(start).alias("period_end"),
        F.when(F.col("region") == "all", "all").otherwise("region").alias("slice_type"),
        F.col("region").alias("slice_value"), F.lit(run_id(business_date)).alias("release_id"),
        F.col(value).cast("double").alias("value"),
        (F.round(F.col(num), 6) if num else F.lit(None)).cast("double").alias("numerator"),
        (F.col(den) if den else F.lit(None)).cast("double").alias("denominator"),
        F.lit(run_time(business_date).isoformat(sep=" ")).cast("timestamp_ntz")
         .alias("published_at"))
        for metric, value, num, den in MEASURES]
    return reduce(DataFrame.unionByName, rows)


def restatements(ctx: Context) -> DataFrame:
    p = ctx.spark.table(ctx.table("gold", "fact_daily_metrics_published"))
    key = Window.partitionBy(*RESTATEMENT_GRAIN)
    first = Window.partitionBy(*RESTATEMENT_GRAIN).orderBy("published_at")
    latest = Window.partitionBy(*RESTATEMENT_GRAIN).orderBy(F.col("published_at").desc())
    ranked = (p.withColumn("_first", F.row_number().over(first))
              .withColumn("_latest", F.row_number().over(latest))
              .withColumn("releases", F.count("*").over(key)))
    f = ranked.where("_first = 1").select(
        *RESTATEMENT_GRAIN, "releases", F.col("release_id").alias("first_release"),
        F.col("value").alias("first_value"), F.col("numerator").alias("first_numerator"),
        F.col("denominator").alias("first_denominator"))
    last = ranked.where("_latest = 1").select(
        *RESTATEMENT_GRAIN, F.col("release_id").alias("latest_release"),
        F.col("value").alias("latest_value"), F.col("numerator").alias("latest_numerator"),
        F.col("denominator").alias("latest_denominator"))
    return f.join(last, RESTATEMENT_GRAIN).withColumn(
        "restatement", F.col("latest_value") - F.col("first_value"))


def recompute_aggregates(ctx: Context, business_date: date) -> dict[str, int]:
    create_tables(ctx, "gold", TABLES)
    name = "agg_delivery_by_month_region"
    months = dirty_months(ctx, business_date).localCheckpoint()
    listed = ", ".join(f"'{r.delivery_month}'" for r in months.collect())
    if not listed:
        return {name: 0, "fact_daily_metrics_published": 0, "agg_restatements": 0}
    agg = delivery_by_month_region(ctx, months).localCheckpoint()
    result = {name: merge_changed(ctx, ctx.table("gold", name), agg, ["delivery_month", "region"],
                                  delete_missing=True,
                                  delete_within=f"delivery_month IN ({listed})")}
    # Append-only: a retry of the same run adds nothing, because release_id is in the grain.
    result["fact_daily_metrics_published"] = merge_changed(
        ctx, ctx.table("gold", "fact_daily_metrics_published"), published_rows(agg, business_date),
        PUBLISHED_GRAIN)
    result["agg_restatements"] = merge_changed(ctx, ctx.table("gold", "agg_restatements"),
                                               restatements(ctx), RESTATEMENT_GRAIN,
                                               delete_missing=True)
    return result
