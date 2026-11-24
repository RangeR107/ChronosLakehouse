"""build_facts, recompute_aggregates and load_serving on the hand-built fixture.

Spec: Data model (fact grains, accumulating snapshot, customer version at purchase), the real
defects table (dq_flags), "Order review score", late-data scenarios 2 (late milestone) and 7
(retry). Expected values are worked out by hand from tests/fixtures/olist_mini.
"""
import uuid
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from generator.replay import replay
from generator.source_db import conninfo
from jobs.bronze_to_silver import bronze_to_silver
from jobs.build_dimensions import build_dimensions
from jobs.build_facts import build_facts
from jobs.context import Context
from jobs.extract_dimensions import MASTERS, bronze_table, ensure_table
from jobs.keys import surrogate_key_py
from jobs.land_to_bronze import land_to_bronze, register_files, wait_for_landing
from jobs.load_serving import load_serving
from jobs.recompute_aggregates import recompute_aggregates
from sentinel.release import open_run, publish_release

MINI = Path(__file__).parent.parent / "fixtures" / "olist_mini"
FIRST, LAST = date(2017, 1, 1), date(2017, 1, 15)


@pytest.fixture
def ctx(spark, tmp_path):
    landing = tmp_path / "landing"
    replay(MINI, landing, FIRST, LAST, purchased_from=FIRST)
    c = Context(spark, landing, namespace_prefix=f"t{uuid.uuid4().hex[:8]}_", reference_dir=MINI)
    for name, m in MASTERS.items():
        ensure_table(c, name, m)
    add_person_a(c)
    add_sellers(c)
    return c


def add_person_a(ctx):
    """person_a's two addresses, as extract_dimensions would have written them by 8 Jan."""
    rows = [("person_a", "01310", "sao paulo", "SP", "2017-01-05 10:15:00"),
            ("person_a", "80010", "curitiba", "PR", "2017-01-07 09:00:00")]
    (ctx.spark.createDataFrame(rows, "customer_unique_id string, customer_zip_code_prefix string,"
                                     " customer_city string, customer_state string, t string")
     .select("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
             *(F.col("t").cast("timestamp_ntz").alias(c)
               for c in ("address_effective_from", "created_at", "updated_at")),
             F.lit("2017-01-08 00:00:00").cast("timestamp_ntz").alias("extracted_at"),
             F.lit("run_test").alias("run_id"))
     .writeTo(bronze_table(ctx, "customers")).append())


def add_sellers(ctx):
    rows = [("s_1", "13023", "campinas", "SP"), ("s_2", "88010", "florianopolis", "SC")]
    (ctx.spark.createDataFrame(rows, "seller_id string, seller_zip_code_prefix string,"
                                     " seller_city string, seller_state string")
     .select("*", *(F.lit("2017-01-01 00:00:00").cast("timestamp_ntz").alias(c)
                    for c in ("created_at", "updated_at", "extracted_at")),
             F.lit("run_test").alias("run_id"))
     .writeTo(bronze_table(ctx, "sellers")).append())


def run_through(ctx, last, start=FIRST):
    """Land every day up to `last`, then one silver, dimensions and facts run for `last`."""
    d = start
    while d <= last:
        wait_for_landing(ctx, d)
        land_to_bronze(ctx, d, register_files(ctx, d)["new"])
        d += timedelta(days=1)
    bronze_to_silver(ctx, last)
    build_dimensions(ctx, last)
    return build_facts(ctx, last)


def rows(ctx, table, cols, order="order_id"):
    df = ctx.spark.table(ctx.table("gold", table))
    return {r[0]: tuple(r)[1:] for r in df.select(*cols).orderBy(order).collect()}


def test_snapshot_status_lateness_and_flags(ctx):
    run_through(ctx, LAST)
    got = rows(ctx, "fact_order_fulfillment",
               ["order_id", "final_status", "is_open", "is_late", "days_early", "item_count",
                "dq_flags"])
    assert got == {
        "o_cancel_after_delivery": ("canceled", False, None, 11, 1, ["canceled_with_delivery_date"]),
        "o_delivered": ("delivered", False, False, 8, 2, []),
        "o_never_approved": ("canceled", False, None, None, 0, []),
        "o_open": (None, True, None, None, 1, []),
        "o_unavailable": ("unavailable", False, None, None, 0, []),
        "o_undated": ("delivered", False, None, None, 1, ["delivered_without_date"]),
    }


def test_snapshot_keys_and_lags(ctx):
    run_through(ctx, LAST)
    got = rows(ctx, "fact_order_fulfillment",
               ["order_id", "customer_key", "delivery_geography_key", "purchase_date_key",
                "delivered_date_key", "purchase_to_approval_days"])
    # person_a's first order uses the first address version, the second order the second.
    assert got["o_delivered"][:4] == (surrogate_key_py("person_a", "2017-01-05 10:15:00"),
                                      surrogate_key_py("01310", "sao paulo", "SP"),
                                      20170105, 20170112)
    assert got["o_open"][0] == surrogate_key_py("person_a", "2017-01-07 09:00:00")
    assert got["o_delivered"][4] == pytest.approx(15 / (60 * 24))
    assert got["o_never_approved"][4] is None


def test_items_payments_and_order_review(ctx):
    run_through(ctx, LAST)
    items = ctx.spark.table(ctx.table("gold", "fact_order_items"))
    assert items.count() == 5  # o_2016's item is outside the slice
    item = items.where("order_id = 'o_delivered' AND order_item_id = 1").collect()[0]
    assert (item.price, item.seller_key, item.seller_geography_key) == (
        Decimal("58.90"), surrogate_key_py("s_1"), surrogate_key_py("13023", "campinas", "SP"))
    pays = ctx.spark.table(ctx.table("gold", "fact_payments"))
    assert pays.count() == 7
    assert pays.agg(F.sum("payment_value")).collect()[0][0] == Decimal("506.40")
    reviews = rows(ctx, "fact_reviews", ["review_id", "is_order_review"], order="review_id")
    # r_1 and r_2 share a creation date; r_2 was answered later, so it is the order's score.
    assert reviews == {"r_1": (False,), "r_2": (True,), "r_3": (True,)}


def test_late_milestone_fills_the_snapshot_row(ctx):
    run_through(ctx, date(2017, 1, 10))
    before = rows(ctx, "fact_order_fulfillment", ["order_id", "final_status", "is_open"])
    assert before["o_delivered"] == (None, True)  # delivered on 12 Jan, not known yet
    result = run_through(ctx, LAST, start=date(2017, 1, 11))
    after = rows(ctx, "fact_order_fulfillment", ["order_id", "final_status", "is_late"])
    assert after["o_delivered"] == ("delivered", False)
    # Only orders with new events changed: o_delivered, and o_cancel_after_delivery, whose
    # cancellation is emitted a day after its last milestone (10 Jan), so on 11 Jan.
    assert result["fact_order_fulfillment"] == 2
    assert rows(ctx, "fact_order_fulfillment", ["order_id", "final_status"])[
        "o_cancel_after_delivery"] == ("canceled",)
    events = ctx.spark.table(ctx.table("gold", "fact_order_events"))
    assert events.where("event_type = 'delivered' AND order_id = 'o_delivered'").count() == 1


def test_rerun_commits_nothing(ctx):
    run_through(ctx, LAST)
    tables = ["fact_order_fulfillment", "fact_order_items", "fact_payments", "fact_reviews",
              "fact_order_events"]

    def snapshots():
        return {t: ctx.spark.sql(f"SELECT count(*) FROM {ctx.table('gold', t)}.snapshots")
                .collect()[0][0] for t in tables}

    seen = snapshots()
    again = build_facts(ctx, LAST)
    assert again == {"orders_rebuilt": 0, "dirty_periods": 0, **{t: 0 for t in tables}}
    assert snapshots() == seen


def test_aggregate_late_rate_by_region(ctx):
    run_through(ctx, LAST)
    recompute_aggregates(ctx, LAST)
    got = rows(ctx, "agg_delivery_by_month_region",
               ["region", "delivery_month", "delivered_orders", "late_orders", "median_days_early"],
               order="region")
    # Only o_delivered counts: delivered with a date (o_undated has none), sent to SP.
    assert got == {"Southeast": ("2017-01", 1, 0, 8.0), "all": ("2017-01", 1, 0, 8.0)}


@pytest.mark.postgres
def test_load_serving_replaces_in_one_transaction(ctx, source_db):
    served = Context(ctx.spark, ctx.landing_dir, ctx.namespace_prefix,
                     source_conninfo=conninfo(source_db.info.dbname), reference_dir=MINI)
    open_run(served, LAST)  # the steps below write to the run's branch
    run_through(served, LAST)
    recompute_aggregates(served, LAST)
    publish_release(served, LAST)  # the audit is tested elsewhere
    source_db.execute("CREATE SCHEMA serving")
    source_db.execute("CREATE TABLE serving.delivery_by_month_region (delivery_month text,"
                      " region text, delivered_orders bigint, late_orders bigint,"
                      " late_rate double precision, median_days_early double precision,"
                      " p10_days_early double precision, p90_days_early double precision,"
                      " share_7_days_early double precision,"
                      " PRIMARY KEY (delivery_month, region))")
    # A stale row gold doesn't have must disappear in the same load.
    source_db.execute("INSERT INTO serving.delivery_by_month_region VALUES"
                      " ('2016-12', 'North', 1, 1, 1, 0, 0, 0, 0)")
    source_db.commit()
    assert load_serving(served, LAST)["delivery_by_month_region"] == 2
    got = source_db.execute("SELECT delivery_month, region, delivered_orders"
                            " FROM serving.delivery_by_month_region").fetchall()
    assert set(got) == {("2017-01", "Southeast", 1), ("2017-01", "all", 1)}
    state = source_db.execute("SELECT run_id, business_date, release_id"
                              " FROM serving.load_state").fetchone()
    assert state == ("run_2017-01-15", LAST, "release_2017-01-15")
    # The dashboards never go back to an older release.
    source_db.execute("UPDATE serving.load_state SET business_date = '2017-01-20'")
    source_db.commit()
    assert "skipped" in load_serving(served, LAST)


def test_aggregate_row_goes_when_its_only_order_is_cancelled(ctx):
    # o_cancel_after_delivery (to Manaus, North) is delivered on 10 Jan; its cancellation is
    # emitted on 11 Jan. Until then the North has a row; after, the row must be gone, not stale.
    run_through(ctx, date(2017, 1, 10))
    recompute_aggregates(ctx, date(2017, 1, 10))
    regions = {r: v for r, *v in rows(ctx, "agg_delivery_by_month_region",
                                      ["region", "delivered_orders"], order="region").items()}
    assert "North" in regions
    run_through(ctx, LAST, start=date(2017, 1, 11))
    recompute_aggregates(ctx, LAST)
    assert set(rows(ctx, "agg_delivery_by_month_region", ["region"], order="region")) == {
        "Southeast", "all"}
