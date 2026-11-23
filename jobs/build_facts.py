"""build_facts: the event ledger, the order accumulating snapshot, items, payments and reviews
(spec: Data model; chronos_daily step 8; late-data scenarios 2 to 4).

Each run rebuilds only the orders touched by events silver received since gold last looked: an
order's whole row is derived again from all of its current events, so a late milestone fills its
column whatever order events arrived in, and nothing depends on the arrival sequence.

"Since gold last looked" is the latest ingested_at in fact_order_events, which is written last,
the same crash rule as silver: a run that dies midway leaves the mark where it was, and the rerun
rebuilds the same orders and merges only what is missing.
"""
from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from jobs.build_dimensions import geography_key
from jobs.context import Context, run_id, run_time
from jobs.keys import surrogate_key
from jobs.merge import changed_rows, create_tables, merge_changed

TS = "timestamp_ntz"

TABLES = {
    "fact_order_events": """
        event_id STRING, event_type STRING, order_id STRING, review_id STRING, version INT,
        event_time TIMESTAMP_NTZ, emitted_at TIMESTAMP_NTZ, ingested_at TIMESTAMP_NTZ,
        supersedes_event_id STRING, is_current_version BOOLEAN""",
    "fact_order_fulfillment": """
        order_id STRING, customer_key BIGINT, delivery_geography_key BIGINT,
        purchase_date_key INT, approved_date_key INT, carrier_date_key INT,
        delivered_date_key INT, estimated_delivery_date_key INT,
        order_purchase_timestamp TIMESTAMP_NTZ, order_approved_at TIMESTAMP_NTZ,
        order_delivered_carrier_date TIMESTAMP_NTZ, order_delivered_customer_date TIMESTAMP_NTZ,
        order_estimated_delivery_date TIMESTAMP_NTZ,
        purchase_to_approval_days DOUBLE, approval_to_carrier_days DOUBLE,
        carrier_to_delivery_days DOUBLE, purchase_to_delivery_days DOUBLE,
        days_early INT COMMENT 'estimated minus delivered, calendar days; negative is late',
        is_late BOOLEAN COMMENT 'null unless delivered with a date',
        final_status STRING COMMENT 'delivered, canceled or unavailable; null while open',
        is_open BOOLEAN, item_count INT, dq_flags ARRAY<STRING>""",
    "fact_order_items": """
        order_id STRING, order_item_id INT, product_key BIGINT, seller_key BIGINT,
        seller_geography_key BIGINT, delivery_geography_key BIGINT, customer_key BIGINT,
        purchase_date_key INT, shipping_limit_date TIMESTAMP_NTZ, price DECIMAL(12,2),
        freight_value DECIMAL(12,2)""",
    "fact_payments": """
        order_id STRING, payment_sequential INT, payment_type STRING, payment_installments INT,
        payment_value DECIMAL(12,2), purchase_date_key INT, dq_flags ARRAY<STRING>""",
    "fact_reviews": """
        review_id STRING, order_id STRING, review_score INT, review_creation_date TIMESTAMP_NTZ,
        review_answer_timestamp TIMESTAMP_NTZ, creation_date_key INT,
        is_order_review BOOLEAN COMMENT 'the review that counts as the order score'""",
    "dirty_periods": """
        run_id STRING, metric_id STRING, period_type STRING, period_start DATE""",
}

# Metrics whose periods a changed fulfillment row dirties, and the date that picks the period
# (spec: Late-arriving data, "Dirty periods"; questions 1 and 2: the delivery month).
DIRTIED_BY_DELIVERY = ("q1_late_delivery_rate", "q2_delivery_promise")
GRAIN = {
    "fact_order_events": ["event_id"],
    "fact_order_fulfillment": ["order_id"],
    "fact_order_items": ["order_id", "order_item_id"],
    "fact_payments": ["order_id", "payment_sequential"],
    "fact_reviews": ["review_id", "order_id"],
    "dirty_periods": ["run_id", "metric_id", "period_type", "period_start"],
}


def ensure_fact_tables(ctx: Context) -> None:
    create_tables(ctx, "gold", TABLES)


def date_key(c: Column) -> Column:
    return F.date_format(c, "yyyyMMdd").cast("int")


def days_between(start: Column, end: Column) -> Column:
    """Elapsed days, with the time of day (unix_timestamp reads NTZ values as they are)."""
    return (F.unix_timestamp(end) - F.unix_timestamp(start)) / 86400.0


def flags(*pairs: tuple[str, Column]) -> Column:
    """An array of the flag names whose condition is true, in a fixed order."""
    return F.array_compact(F.array(*(F.when(cond, F.lit(name)) for name, cond in pairs)))


def touched_orders(ctx: Context, business_date: date) -> DataFrame:
    """Orders with events silver received after the ledger's mark, up to this run's replay time."""
    mark = ctx.spark.sql(
        f"SELECT max(ingested_at) FROM {ctx.table('gold', 'fact_order_events')}").collect()[0][0]
    upper = F.lit(run_time(business_date).isoformat(sep=" ")).cast(TS)
    events = ctx.spark.table(ctx.table("silver", "events")).where(F.col("ingested_at") <= upper)
    if mark is not None:
        # As text, cast back to NTZ: F.lit() of a Python datetime would shift it through the
        # local time zone, and the mark would no longer equal the time it came from.
        events = events.where(F.col("ingested_at") > F.lit(mark.isoformat(sep=" ")).cast(TS))
    return events.select("order_id").distinct()


def stale_orders(ctx: Context) -> DataFrame:
    """Orders whose fact keys no longer match the dimensions, so they must be re-keyed.

    Two cases (spec: Data model, "Late dimension change" and "Inferred members"): an address
    version arrived late, so a different customer version is now effective at purchase; or a
    seller placeholder became the real seller, whose location is now known.
    """
    f = ctx.spark.table(ctx.table("gold", "fact_order_fulfillment")).select(
        "order_id", "order_purchase_timestamp", F.col("customer_key").alias("_had"))
    people = ctx.spark.table(ctx.table("silver", "orders")).select(
        "order_id", "customer_unique_id").distinct()
    now = with_customer_key(ctx, f.join(people, "order_id"))
    customers = now.where(~F.col("customer_key").eqNullSafe(F.col("_had"))).select("order_id")
    items = ctx.spark.table(ctx.table("gold", "fact_order_items")).select(
        "order_id", "seller_key", F.col("seller_geography_key").alias("_had"))
    places = ctx.spark.table(ctx.table("gold", "dim_seller")).select(
        "seller_key", geography_key("seller_zip_code_prefix", "seller_city", "seller_state")
                      .alias("_now"))
    sellers = (items.join(places, "seller_key", "left")
               .where(~F.col("_now").eqNullSafe(F.col("_had"))).select("order_id"))
    return customers.unionByName(sellers).distinct()


def dirty_periods(ctx: Context, snapshot: DataFrame, business_date: date) -> DataFrame:
    """The delivery months a run's changed snapshot rows touch, before and after the change.

    Both sides count: an order whose delivery date is corrected from 31 Jan to 1 Feb changes
    January and February.
    """
    table = ctx.table("gold", "fact_order_fulfillment")
    changed = changed_rows(ctx, table, snapshot)
    before = ctx.spark.table(table).join(changed.select("order_id"), "order_id")
    months = (changed.select("order_delivered_customer_date")
              .unionByName(before.select("order_delivered_customer_date"))
              .where(F.col("order_delivered_customer_date").isNotNull())
              .select(F.trunc("order_delivered_customer_date", "month").alias("period_start"))
              .distinct())
    metrics = ctx.spark.createDataFrame([(m,) for m in DIRTIED_BY_DELIVERY], "metric_id string")
    return months.crossJoin(metrics).select(
        F.lit(run_id(business_date)).alias("run_id"), "metric_id",
        F.lit("month").alias("period_type"), "period_start")


def ledger(ctx: Context, orders: DataFrame, business_date: date) -> DataFrame:
    """Every accepted event version of the touched orders; the highest version is current."""
    upper = F.lit(run_time(business_date).isoformat(sep=" ")).cast(TS)
    events = (ctx.spark.table(ctx.table("silver", "events")).where(F.col("ingested_at") <= upper)
              .join(orders, "order_id"))
    latest = Window.partitionBy("event_type", "order_id", "review_id")
    return events.select(
        "event_id", "event_type", "order_id", "review_id", "version", "event_time", "emitted_at",
        "ingested_at", "supersedes_event_id",
        (F.col("version") == F.max("version").over(latest)).alias("is_current_version"))


def milestones(current: DataFrame) -> DataFrame:
    """One row per order: each milestone from the current version of its own event type.

    Status is derived from which events exist, never from the last one received: cancelled and
    unavailable win, then delivered, else the order is still open (docs/decisions.md).
    """
    def time_of(event_type):
        return F.max(F.when(F.col("event_type") == event_type, F.col("event_time")))

    def has(event_type):
        return F.max(F.col("event_type") == event_type)

    m = current.groupBy("order_id").agg(
        F.max(F.when(F.col("event_type") == "order_placed", F.col("event_id")))
         .alias("placed_event_id"),
        time_of("payment_approved").alias("order_approved_at"),
        time_of("handed_to_carrier").alias("order_delivered_carrier_date"),
        time_of("delivered").alias("order_delivered_customer_date"),
        has("delivered").alias("_delivered"), has("order_cancelled").alias("_cancelled"),
        has("order_unavailable").alias("_unavailable"))
    status = (F.when(F.col("_cancelled"), "canceled").when(F.col("_unavailable"), "unavailable")
              .when(F.col("_delivered"), "delivered"))
    return m.withColumn("final_status", status).drop("_delivered", "_cancelled", "_unavailable")


def fulfillment(ctx: Context, current: DataFrame) -> DataFrame:
    orders = ctx.spark.table(ctx.table("silver", "orders"))
    items = ctx.spark.table(ctx.table("silver", "order_items"))
    payments = ctx.spark.table(ctx.table("silver", "order_payments"))
    m = milestones(current)
    # Only orders whose order_placed has arrived get a row: the snapshot starts at the purchase.
    o = m.join(orders.withColumnRenamed("event_id", "placed_event_id"),
               ["placed_event_id", "order_id"])
    item_totals = items.groupBy(F.col("event_id").alias("placed_event_id")).agg(
        F.count("*").alias("item_count"),
        F.sum(F.col("price") + F.col("freight_value")).alias("_items_total"))
    pay_totals = payments.groupBy(F.col("event_id").alias("placed_event_id")).agg(
        F.sum("payment_value").alias("_paid"))
    o = o.join(item_totals, "placed_event_id", "left").join(pay_totals, "placed_event_id", "left")
    o = with_customer_key(ctx, o)

    purchase, approved = F.col("order_purchase_timestamp"), F.col("order_approved_at")
    carrier, delivered = F.col("order_delivered_carrier_date"), F.col("order_delivered_customer_date")
    estimated, status = F.col("order_estimated_delivery_date"), F.col("final_status")
    is_delivered = status == "delivered"
    # Real source defects (spec: Data sources, "Real defects"): flagged, never dropped or fixed.
    carrier_before_approval = carrier < approved
    carrier_before_purchase = carrier < purchase
    delivered_before_carrier = delivered < carrier
    dq = flags(
        ("carrier_before_approval", carrier_before_approval),
        ("carrier_before_purchase", carrier_before_purchase),
        ("delivered_before_carrier", delivered_before_carrier),
        ("delivered_without_date", is_delivered & delivered.isNull()),
        ("delivered_never_approved", is_delivered & approved.isNull()),
        ("canceled_with_delivery_date", (status == "canceled") & delivered.isNotNull()),
        ("no_items", F.col("item_count").isNull()
         & ~F.coalesce(status.isin("canceled", "unavailable"), F.lit(False))),
        ("no_payments", F.col("_paid").isNull()),
        ("payment_mismatch", F.abs(F.col("_paid") - F.col("_items_total")) > 1))
    # A lag that uses a timestamp a flag says is out of order is left empty, not negative.
    return o.select(
        "order_id", "customer_key",
        geography_key("customer_zip_code_prefix", "customer_city", "customer_state")
         .alias("delivery_geography_key"),
        date_key(purchase).alias("purchase_date_key"), date_key(approved).alias("approved_date_key"),
        date_key(carrier).alias("carrier_date_key"), date_key(delivered).alias("delivered_date_key"),
        date_key(estimated).alias("estimated_delivery_date_key"),
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
        days_between(purchase, approved).alias("purchase_to_approval_days"),
        F.when(~carrier_before_approval & ~carrier_before_purchase,
               days_between(approved, carrier)).alias("approval_to_carrier_days"),
        F.when(~delivered_before_carrier & ~carrier_before_purchase,
               days_between(carrier, delivered)).alias("carrier_to_delivery_days"),
        days_between(purchase, delivered).alias("purchase_to_delivery_days"),
        # Calendar days, because the estimate has no time of day (spec: question 2).
        F.datediff(F.to_date(estimated), F.to_date(delivered)).alias("days_early"),
        F.when(is_delivered & delivered.isNotNull(),
               F.to_date(delivered) > F.to_date(estimated)).alias("is_late"),
        "final_status", status.isNull().alias("is_open"),
        F.coalesce("item_count", F.lit(0)).cast("int").alias("item_count"),
        dq.alias("dq_flags"))


def with_customer_key(ctx: Context, orders: DataFrame) -> DataFrame:
    """The customer version effective at purchase: effective_from <= purchase < effective_to."""
    c = ctx.spark.table(ctx.table("gold", "dim_customer")).select(
        "customer_key", F.col("customer_unique_id").alias("_person"), "effective_from",
        "effective_to")
    on = ((F.col("customer_unique_id") == F.col("_person"))
          & (F.col("effective_from") <= F.col("order_purchase_timestamp"))
          & (F.col("effective_to").isNull()
             | (F.col("order_purchase_timestamp") < F.col("effective_to"))))
    return orders.join(c, on, "left").drop("_person", "effective_from", "effective_to")


def order_items(ctx: Context, snapshot: DataFrame, current: DataFrame) -> DataFrame:
    placed = current.where("event_type = 'order_placed'").select("event_id", "order_id")
    items = ctx.spark.table(ctx.table("silver", "order_items")).join(placed, ["event_id", "order_id"])
    # Keys come from the natural ids, not from a dimension lookup, so they are right even before
    # the dimension row exists (Phase 2's inferred members take the same key).
    seller_places = ctx.spark.table(ctx.table("gold", "dim_seller")).select(
        "seller_id", geography_key("seller_zip_code_prefix", "seller_city", "seller_state")
                     .alias("seller_geography_key"))
    return (items.join(seller_places, "seller_id", "left")
            .join(snapshot.select("order_id", "delivery_geography_key", "customer_key",
                                  "purchase_date_key"), "order_id")
            .select("order_id", "order_item_id",
                    surrogate_key(F.col("product_id")).alias("product_key"),
                    surrogate_key(F.col("seller_id")).alias("seller_key"),
                    "seller_geography_key", "delivery_geography_key", "customer_key",
                    "purchase_date_key", "shipping_limit_date", "price", "freight_value"))


def payments(ctx: Context, snapshot: DataFrame, current: DataFrame) -> DataFrame:
    placed = current.where("event_type = 'order_placed'").select("event_id", "order_id")
    p = ctx.spark.table(ctx.table("silver", "order_payments")).join(placed, ["event_id", "order_id"])
    return p.join(snapshot.select("order_id", "purchase_date_key"), "order_id").select(
        "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value",
        "purchase_date_key",
        flags(("zero_value", F.col("payment_value") == 0),
              ("zero_installments", F.col("payment_installments") == 0),
              ("type_not_defined", F.col("payment_type") == "not_defined")).alias("dq_flags"))


def reviews(ctx: Context, current: DataFrame) -> DataFrame:
    """Current version of each review; one per order is the order's score.

    The order's review is the latest by creation date, then by answer time, then the highest
    review_id (spec: "Order review score"). Creation dates usually have no time of day, so
    the tie-breakers decide often.
    """
    ids = current.where("event_type = 'review_submitted'").select("event_id")
    r = ctx.spark.table(ctx.table("silver", "reviews")).join(ids, "event_id")
    pick = Window.partitionBy("order_id").orderBy(
        F.col("review_creation_date").desc(), F.col("review_answer_timestamp").desc(),
        F.col("review_id").desc())
    return r.select(
        "review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp",
        date_key(F.col("review_creation_date")).alias("creation_date_key"),
        (F.row_number().over(pick) == 1).alias("is_order_review"))


def build_facts(ctx: Context, business_date: date) -> dict[str, int]:
    """Rows inserted or updated per fact table, plus how many orders were rebuilt."""
    ensure_fact_tables(ctx)
    touched = (touched_orders(ctx, business_date).unionByName(stale_orders(ctx))
               .distinct().localCheckpoint())
    events = ledger(ctx, touched, business_date).localCheckpoint()
    current = events.where("is_current_version")
    snapshot = fulfillment(ctx, current).localCheckpoint()

    def merge(name, rows):
        return merge_changed(ctx, ctx.table("gold", name), rows, GRAIN[name])

    result = {"orders_rebuilt": touched.count()}
    # Dirty periods first: they need the rows as they were, and a crash after this point leaves
    # them recorded for the rerun, which finds the facts already changed.
    result["dirty_periods"] = merge("dirty_periods", dirty_periods(ctx, snapshot, business_date))
    result["fact_order_fulfillment"] = merge("fact_order_fulfillment", snapshot)
    result["fact_order_items"] = merge("fact_order_items", order_items(ctx, snapshot, current))
    result["fact_payments"] = merge("fact_payments", payments(ctx, snapshot, current))
    result["fact_reviews"] = merge("fact_reviews", reviews(ctx, current))
    result["fact_order_events"] = merge("fact_order_events", events)  # last: it holds the mark
    return result
