"""Rule checks: keys, referential integrity, reconciliation, contract rates, landing completeness
(spec: DataSentinel, "Checks"). Each returns Results; none changes data.

They read the run's branch (sentinel_audit runs on it), so they judge exactly what would be
published.
"""
from datetime import date

import great_expectations as gx
import psycopg
from great_expectations.data_context.types.base import ProgressBarsConfig
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs import build_dimensions, build_facts, recompute_aggregates
from jobs.bronze_to_silver import KEYS as SILVER_KEYS
from jobs.context import Context, run_id
from jobs.contract import load_contracts, unknown_fields
from jobs.land_to_bronze import register_files
from sentinel import config
from sentinel.checks.batch import bronze_lines, window
from sentinel.checks.result import Result

GOLD_GRAINS = {
    **{t: [k] for t, k in build_dimensions.KEYS.items()},
    **build_facts.GRAIN,
    "agg_delivery_by_month_region": ["delivery_month", "region"],
    "fact_daily_metrics_published": recompute_aggregates.PUBLISHED_GRAIN,
    "agg_restatements": recompute_aggregates.RESTATEMENT_GRAIN,
}


def grain_checks(df: DataFrame, table: str, grain: list[str]) -> list[Result]:
    """GX not-null on each grain column and uniqueness on the grain (Completeness and keys)."""
    context = gx.get_context(mode="ephemeral")
    # GX draws a progress bar for every validation; in Airflow logs and the demo's output it
    # buries the lines that matter.
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    # persist=False matters: GX otherwise caches the DataFrame and never releases it, and Spark
    # then answers later reads of the same table from that cache, whatever branch or snapshot
    # they ask for. The next run's jobs would see this run's data and build nothing new.
    asset = context.data_sources.add_spark(name="chronos", persist=False) \
                                .add_dataframe_asset(name=table)
    batch = asset.add_batch_definition_whole_dataframe("all").get_batch(
        batch_parameters={"dataframe": df})
    # One suite per table, validated once: GX then works out the metrics together. Validating
    # each expectation on its own took about 2.4 times as long (docs/decisions.md).
    suite = gx.ExpectationSuite(name=table)
    for c in grain:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=c))
    suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=grain)
                          if len(grain) > 1 else
                          gx.expectations.ExpectColumnValuesToBeUnique(column=grain[0]))
    report = batch.validate(suite)
    return [Result(table, check_name(r.expectation_config), "gx", "block",
                   "pass" if r.success else "fail",
                   observed=r.result.get("unexpected_count", 0), expected="0")
            for r in report.results]


def check_name(config) -> str:
    """'ExpectColumnValuesToNotBeNull:order_id', as the checks have always been named."""
    kind = "".join(w.capitalize() for w in config.type.split("_"))
    kwargs = config.kwargs
    return f"{kind}:{kwargs.get('column') or '+'.join(kwargs['column_list'])}"


def keys(ctx: Context) -> list[Result]:
    out = []
    for name, grain in SILVER_KEYS.items():
        out += grain_checks(ctx.spark.table(ctx.table("silver", name)), f"silver.{name}", grain)
    for name, grain in GOLD_GRAINS.items():
        out += grain_checks(ctx.spark.table(ctx.table("gold", name)), f"gold.{name}", grain)
    return out


def orphans(fact: DataFrame, column: str, dim: DataFrame, key: str,
            nullable: bool = False) -> int:
    rows = fact.where(F.col(column).isNotNull()) if nullable else fact
    return rows.join(dim.select(F.col(key).alias(column)), column, "left_anti").count()


def referential(ctx: Context) -> list[Result]:
    """Every fact key exists in its dimension; inferred members count as present."""
    g = lambda t: ctx.spark.table(ctx.table("gold", t))
    f, items = g("fact_order_fulfillment"), g("fact_order_items")
    dims = {"customer": g("dim_customer"), "geo": g("dim_geography"), "date": g("dim_date"),
            "product": g("dim_product"), "seller": g("dim_seller")}
    pairs = [
        ("fact_order_fulfillment", f, "customer_key", "customer", "customer_key", False),
        ("fact_order_fulfillment", f, "delivery_geography_key", "geo", "geography_key", False),
        *[("fact_order_fulfillment", f, c, "date", "date_key", c != "purchase_date_key")
          for c in ("purchase_date_key", "approved_date_key", "carrier_date_key",
                    "delivered_date_key", "estimated_delivery_date_key")],
        ("fact_order_items", items, "product_key", "product", "product_key", False),
        ("fact_order_items", items, "seller_key", "seller", "seller_key", False),
        # Empty while the seller is an inferred member without a location.
        ("fact_order_items", items, "seller_geography_key", "geo", "geography_key", True),
        ("fact_order_items", items, "delivery_geography_key", "geo", "geography_key", False),
    ]
    out = []
    for table, fact, column, dim, key, nullable in pairs:
        n = orphans(fact, column, dims[dim], key, nullable)
        out.append(Result(f"gold.{table}", f"referential:{column}", "anti_join", "block",
                          "pass" if n == 0 else "fail", observed=n, expected="0"))
    return out


def reconciliation(ctx: Context) -> list[Result]:
    """Orders and merchandise per purchase date match exactly between silver and gold."""
    s = lambda t: ctx.spark.table(ctx.table("silver", t))
    g = lambda t: ctx.spark.table(ctx.table("gold", t))
    # Silver's current order_placed version per order: the highest version.
    placed = (s("events").where("event_type = 'order_placed'")
              .groupBy("order_id").agg(F.max_by("event_id", "version").alias("event_id")))
    orders = s("orders").join(placed, ["event_id", "order_id"])
    by_day_s = orders.groupBy(F.to_date("order_purchase_timestamp").alias("d")).agg(
        F.count("*").alias("orders"))
    by_day_g = g("fact_order_fulfillment").groupBy(
        F.to_date("order_purchase_timestamp").alias("d")).agg(F.count("*").alias("orders"))
    items_s = (s("order_items").join(placed, ["event_id", "order_id"])
               .join(orders.select("order_id", "order_purchase_timestamp"), "order_id")
               .groupBy(F.to_date("order_purchase_timestamp").alias("d"))
               .agg(F.sum("price").alias("price")))
    items_g = (g("fact_order_items").join(g("fact_order_fulfillment").select(
                   "order_id", "order_purchase_timestamp"), "order_id")
               .groupBy(F.to_date("order_purchase_timestamp").alias("d"))
               .agg(F.sum("price").alias("price")))
    out = []
    for name, a, b, col in (("orders", by_day_s, by_day_g, "orders"),
                            ("merchandise", items_s, items_g, "price")):
        diff = (a.alias("a").join(b.alias("b"), "d", "full_outer")
                .where(~F.col(f"a.{col}").eqNullSafe(F.col(f"b.{col}"))).count())
        out.append(Result("gold.fact_order_fulfillment" if name == "orders"
                          else "gold.fact_order_items", f"reconciliation:{name}",
                          "silver_vs_gold_by_purchase_date", "block",
                          "pass" if diff == 0 else "fail", observed=diff,
                          expected="0 dates differ"))
    return out


def contract_rates(ctx: Context, business_date: date) -> list[Result]:
    """Quarantined lines as a share of the batch: unknown schema or missing fields, bad values."""
    read = bronze_lines(ctx, business_date).count()
    q = window(ctx, business_date, ctx.spark.table(ctx.table("silver", "quarantine")))
    reasons = q.select(F.explode("reasons").alias("r")).select(
        F.split("r", ":")[0].alias("kind")).groupBy("kind").count().collect()
    by_kind = {r.kind: r["count"] for r in reasons}
    schema = sum(v for k, v in by_kind.items()
                 if k in ("missing", "unknown_schema_version", "invalid_json", "bad_type"))
    values = sum(v for k, v in by_kind.items() if k in ("bad_value",))
    out = []
    for name, n in (("schema_contract", schema), ("valid_values", values)):
        share = n / read if read else 0.0
        out.append(Result("silver.quarantine", f"{name}:quarantine_share", "threshold", "block",
                          "fail" if share > config.QUARANTINE_BLOCK_SHARE else "pass",
                          observed=round(share, 6),
                          expected=f"<= {config.QUARANTINE_BLOCK_SHARE:.1%} of {read} lines",
                          detail={"quarantined": n, "lines": read}))
    return out


# How many unknown field names a warning lists; the count covers them all.
UNKNOWN_FIELDS_SHOWN = 10


def new_fields(ctx: Context, business_date: date) -> list[Result]:
    """Fields the producer sent that the line's contract doesn't list: a warning, not a block.

    An optional field nobody announced is harmless to the pipeline (silver ignores it) but
    means the contract and the producer disagree, so a person should add it or ask why
    (spec: Checks, "Warn on a new optional field"). An announced field is in the contract and
    raises nothing. A required field renamed without a new version also shows up here, and is
    quarantined by the schema contract check as missing.
    """
    unknown = (bronze_lines(ctx, business_date)
               .select(F.explode(unknown_fields(F.col("raw"), load_contracts())).alias("f")))
    counts = {r.f: r["count"] for r in unknown.groupBy("f").count().collect()}
    shown = sorted(counts, key=lambda f: -counts[f])[:UNKNOWN_FIELDS_SHOWN]
    return [Result("bronze.events", "schema_contract:new_optional_field", "contract", "warn",
                   "fail" if counts else "pass", float(sum(counts.values())),
                   "every payload field listed in the line's contract",
                   detail={"fields": {f: counts[f] for f in shown}})]


def reported_unlisted(ctx: Context, business_date: date) -> set[str]:
    """Unlisted files an earlier run already blocked on."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        rows = conn.execute("""SELECT detail->'files' FROM metrics.check_results
                               WHERE namespace = %s AND check_name = 'completeness:unlisted_files'
                                 AND status = 'fail' AND run_id <> %s""",
                            (ctx.namespace_prefix, run_id(business_date))).fetchall()
    return {f for (files,) in rows for f in (files or [])}


def landing(ctx: Context, business_date: date) -> list[Result]:
    """Files against their markers: unlisted blocks; listed but missing warns, then alerts.

    Each problem is reported once, when it is new (docs/decisions.md, 2026-11-24). The check
    looks back over several days of folders, so reporting everything it sees would block every
    run for days after one stray file, and mark every later release incomplete for one missing
    file:
      - an unlisted file blocks the run that first sees it (its batch is then held);
      - a missing listed file warns on its own date, whose release is marked incomplete;
      - a file still missing MISSING_FILE_ALERT_DAYS later alerts once, that day.
    """
    r = register_files(ctx, business_date)

    def day_of(path: str) -> date:
        return date.fromisoformat(path.split("/")[0].removeprefix("date="))

    unlisted = [f for f in r["unlisted"] if f not in reported_unlisted(ctx, business_date)]
    missing = [m for m in r["missing"] if day_of(m) == business_date]
    late = [m for m in r["missing"]
            if (business_date - day_of(m)).days == config.MISSING_FILE_ALERT_DAYS]
    return [
        Result("landing", "completeness:unlisted_files", "marker_vs_folder", "block",
               "fail" if unlisted else "pass", observed=len(unlisted), expected="0",
               detail={"files": unlisted, "seen_before": sorted(set(r["unlisted"]) - set(unlisted))}),
        Result("landing", "completeness:missing_files", "marker_vs_folder", "warn",
               "fail" if missing else "pass", observed=len(missing), expected="0",
               detail={"files": missing}),
        Result("landing", "completeness:missing_after_2_days", "marker_vs_folder", "alert",
               "fail" if late else "pass", observed=len(late), expected="0",
               detail={"files": late}),
    ]
