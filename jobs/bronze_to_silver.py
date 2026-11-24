"""bronze_to_silver: type, check, quarantine and deduplicate events (spec: chronos_daily step 6;
DataSentinel "Schema contract"; late-data scenario 1).

Each run takes the bronze lines ingested after the latest ingested_at silver holds, up to the
run's replay time (jobs/batches.py). The run works on its own branch, which starts from main, so
that is the watermark of the last published release. Batches a blocked run held are left out
until someone releases them.

Every line ends in exactly one place:
  - silver.quarantine, with the reasons it breaks the contract, or
  - silver.events (plus the typed order, item, payment and review tables), once per event_id.
Duplicates (the same event_id, possibly in another file) are dropped within the batch and against
silver before every MERGE, as the spec requires: Iceberg's MERGE fails if two source rows match
one target row.
"""
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from jobs.batches import in_batch
from jobs.context import Context, on_run_branch, run_id
from jobs.contract import load_contracts, problems, raw_schema, to_earliest_names

TS = "timestamp_ntz"
MONEY = "decimal(12,2)"

TABLES = {
    "events": """
        event_id STRING, event_type STRING, order_id STRING, review_id STRING, version INT,
        supersedes_event_id STRING, schema_version INT,
        event_time TIMESTAMP_NTZ COMMENT 'when it happened; null when Olist has no time',
        emitted_at TIMESTAMP_NTZ COMMENT 'when the source sent it (simulated)',
        ingested_at TIMESTAMP_NTZ COMMENT 'when bronze received it (replay time)',
        source_file STRING, line_number INT, business_date DATE""",
    "orders": """
        event_id STRING, order_id STRING, customer_id STRING, customer_unique_id STRING,
        customer_zip_code_prefix STRING, customer_city STRING, customer_state STRING,
        order_purchase_timestamp TIMESTAMP_NTZ, order_estimated_delivery_date TIMESTAMP_NTZ,
        version INT""",
    "order_items": """
        event_id STRING, order_id STRING, order_item_id INT, product_id STRING,
        seller_id STRING, shipping_limit_date TIMESTAMP_NTZ, price DECIMAL(12,2),
        freight_value DECIMAL(12,2), product_category_name STRING""",
    "order_payments": """
        event_id STRING, order_id STRING, payment_sequential INT, payment_type STRING,
        payment_installments INT, payment_value DECIMAL(12,2)""",
    "reviews": """
        event_id STRING, order_id STRING, review_id STRING, review_score INT,
        review_creation_date TIMESTAMP_NTZ, review_answer_timestamp TIMESTAMP_NTZ, version INT""",
    "quarantine": """
        source_file STRING, line_number INT, raw STRING, reasons ARRAY<STRING>, event_id STRING,
        ingested_at TIMESTAMP_NTZ, business_date DATE, run_id STRING""",
}

# The grain of each table: what makes a row unique, and so what each MERGE matches on.
KEYS = {
    "events": ["event_id"],
    "orders": ["event_id"],
    "order_items": ["event_id", "order_item_id"],
    "order_payments": ["event_id", "payment_sequential"],
    "reviews": ["event_id"],
    "quarantine": ["source_file", "line_number"],
}


# Columns added to a table after it was first created, in order: tables created before a
# contract change get them by ALTER TABLE. Iceberg adds a column as a metadata change only, and
# older rows read it as null; nothing is rewritten.
ADDED_COLUMNS = {
    # The announced optional field (contracts/events_v1.json), sent from 2017-02-01.
    "order_items": [("product_category_name", "STRING")],
}


def ensure_silver_tables(ctx: Context) -> None:
    ctx.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ctx.namespace('silver')}")
    for name, columns in TABLES.items():
        table = ctx.table("silver", name)
        ctx.spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} ({columns})
                          USING iceberg TBLPROPERTIES ('format-version' = '2')""")
        have = set(ctx.spark.table(table).columns)
        for column, kind in ADDED_COLUMNS.get(name, []):
            if column not in have:
                ctx.spark.sql(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")


def processed_up_to(ctx: Context):
    """The latest ingestion time in silver.events (None at first).

    Only silver.events counts, because it is written last, in one commit. Had the quarantine
    counted too, a run dying after writing quarantine but before writing events would move the
    mark past events that never reached silver. A batch re-read after such a crash is harmless:
    every MERGE inserts only what is missing.
    """
    return ctx.spark.sql(
        f"SELECT max(ingested_at) FROM {ctx.table('silver', 'events')}").collect()[0][0]


def read_batch(ctx: Context, business_date: date) -> DataFrame:
    """Bronze lines after what silver holds, up to run time, without held or rejected batches."""
    return ctx.spark.table(ctx.table("bronze", "events")).where(
        in_batch(ctx, business_date, processed_up_to(ctx)))


def check(batch: DataFrame, contracts: dict[int, dict]) -> DataFrame:
    """Parse each line and attach the reasons it breaks the contract (empty when it is fine).

    Each line is judged by its own schema_version's contract: after a version change, late and
    resent events still arrive in the old one. Renamed fields are then copied back to their
    earlier names, so the row builders below read one set of names for every version.
    """
    parsed = batch.withColumn("e", F.from_json("raw", raw_schema(*contracts.values())))
    reasons = F.when(F.get_json_object("raw", "$").isNull(), F.array(F.lit("invalid_json")))
    for version, contract in sorted(contracts.items()):
        reasons = reasons.when(F.col("e.schema_version") == version,
                               problems(F.col("e"), contract))
    reasons = reasons.otherwise(F.array(F.lit("unknown_schema_version")))
    return (parsed.withColumn("reasons", reasons)
            .withColumn("e", to_earliest_names(F.col("e"), contracts)))


def first_per_event_id(accepted: DataFrame) -> DataFrame:
    """Keep the first copy of each event_id received; resent copies are identical by design."""
    first = Window.partitionBy("e.event_id").orderBy("ingested_at", "source_file", "line_number")
    return accepted.withColumn("_n", F.row_number().over(first)).where("_n = 1").drop("_n")


def events_rows(d: DataFrame) -> DataFrame:
    return d.select(
        F.col("e.event_id").alias("event_id"), F.col("e.event_type").alias("event_type"),
        F.col("e.payload.order_id").alias("order_id"), F.col("e.payload.review_id").alias("review_id"),
        F.col("e.version").alias("version"), F.col("e.supersedes_event_id").alias("supersedes_event_id"),
        F.col("e.schema_version").alias("schema_version"),
        F.col("e.event_time").cast(TS).alias("event_time"),
        F.col("e.emitted_at").cast(TS).alias("emitted_at"),
        "ingested_at", "source_file", "line_number", "business_date")


def orders_rows(d: DataFrame) -> DataFrame:
    p = "e.payload."
    return d.where("e.event_type = 'order_placed'").select(
        F.col("e.event_id").alias("event_id"),
        *(F.col(p + c).alias(c) for c in ("order_id", "customer_id", "customer_unique_id",
                                          "customer_zip_code_prefix", "customer_city",
                                          "customer_state")),
        F.col(p + "order_purchase_timestamp").cast(TS).alias("order_purchase_timestamp"),
        F.col(p + "order_estimated_delivery_date").cast(TS).alias("order_estimated_delivery_date"),
        F.col("e.version").alias("version"))


def items_rows(d: DataFrame) -> DataFrame:
    return (d.where("e.event_type = 'order_placed'")
            .select(F.col("e.event_id").alias("event_id"), F.col("e.payload.order_id").alias("order_id"),
                    F.explode("e.payload.items").alias("i"))
            .select("event_id", "order_id",
                    F.col("i.order_item_id").cast("int").alias("order_item_id"),
                    F.col("i.product_id").alias("product_id"), F.col("i.seller_id").alias("seller_id"),
                    F.col("i.shipping_limit_date").cast(TS).alias("shipping_limit_date"),
                    F.col("i.price").cast(MONEY).alias("price"),
                    F.col("i.freight_value").cast(MONEY).alias("freight_value"),
                    F.col("i.product_category_name").alias("product_category_name")))


def payments_rows(d: DataFrame) -> DataFrame:
    # Payments ride on order_placed: all 160 never-approved orders still have payment rows.
    return (d.where("e.event_type = 'order_placed'")
            .select(F.col("e.event_id").alias("event_id"), F.col("e.payload.order_id").alias("order_id"),
                    F.explode("e.payload.payments").alias("p"))
            .select("event_id", "order_id",
                    F.col("p.payment_sequential").cast("int").alias("payment_sequential"),
                    F.col("p.payment_type").alias("payment_type"),
                    F.col("p.payment_installments").cast("int").alias("payment_installments"),
                    F.col("p.payment_value").cast(MONEY).alias("payment_value")))


def reviews_rows(d: DataFrame) -> DataFrame:
    p = "e.payload."
    return d.where("e.event_type = 'review_submitted'").select(
        F.col("e.event_id").alias("event_id"), F.col(p + "order_id").alias("order_id"),
        F.col(p + "review_id").alias("review_id"),
        F.col(p + "review_score").cast("int").alias("review_score"),
        F.col(p + "review_creation_date").cast(TS).alias("review_creation_date"),
        F.col(p + "review_answer_timestamp").cast(TS).alias("review_answer_timestamp"),
        F.col("e.version").alias("version"))


def quarantine_rows(rejected: DataFrame, business_date: date) -> DataFrame:
    return rejected.select(
        "source_file", "line_number", "raw", "reasons",
        F.get_json_object("raw", "$.event_id").alias("event_id"), "ingested_at",
        "business_date", F.lit(run_id(business_date)).alias("run_id"))


def merge_new(ctx: Context, name: str, rows: DataFrame) -> int:
    """Insert rows whose key silver doesn't have yet; returns how many were new.

    Insert-only on purpose: an event_id names one immutable event version, so a row already in
    silver is never changed. A correction arrives as a new version with its own event_id.
    """
    table = ctx.table("silver", name)
    keys = KEYS[name]
    new = rows.join(ctx.spark.table(table).select(*keys), keys, "left_anti").localCheckpoint()
    count = new.count()
    if count:
        view = f"_new_{name}"
        new.createOrReplaceTempView(view)
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        ctx.spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} "
                      f"WHEN NOT MATCHED THEN INSERT *")
    return count


@on_run_branch
def bronze_to_silver(ctx: Context, business_date: date) -> dict:
    ensure_silver_tables(ctx)
    checked = check(read_batch(ctx, business_date), load_contracts()).localCheckpoint()
    accepted = checked.where(F.size("reasons") == 0)
    unique = first_per_event_id(accepted).localCheckpoint()

    result = {"read": checked.count(), "duplicates_in_batch": accepted.count() - unique.count()}
    # Everything else first, events last: processed_up_to() reads only silver.events, so if a run
    # dies midway the batch is read again and the insert-only MERGEs fill in whatever is missing.
    for name, rows in (("orders", orders_rows(unique)), ("order_items", items_rows(unique)),
                       ("order_payments", payments_rows(unique)), ("reviews", reviews_rows(unique))):
        result[name] = merge_new(ctx, name, rows)
    result["quarantined"] = merge_new(ctx, "quarantine",
                                      quarantine_rows(checked.where(F.size("reasons") > 0),
                                                      business_date))
    result["events"] = merge_new(ctx, "events", events_rows(unique))
    return result
