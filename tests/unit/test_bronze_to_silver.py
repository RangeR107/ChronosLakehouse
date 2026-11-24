"""bronze_to_silver: typing by the contract, quarantine with reasons, and deduplication.

Spec: chronos_daily step 6, DataSentinel "Schema contract", late-data scenarios 1 (duplicates)
and 7 (retry). Expected values come from the fixture rows (tests/fixtures/olist_mini).
"""
import json
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from generator.replay import replay
from jobs.bronze_to_silver import bronze_to_silver
from jobs.context import Context
from jobs.land_to_bronze import (
    LINES_SCHEMA,
    ensure_bronze_tables,
    land_to_bronze,
    register_files,
    wait_for_landing,
)

MINI = Path(__file__).parent.parent / "fixtures" / "olist_mini"
FIRST, LAST = date(2017, 1, 1), date(2017, 1, 15)


@pytest.fixture
def ctx(spark, tmp_path):
    landing = tmp_path / "landing"
    replay(MINI, landing, FIRST, LAST, purchased_from=FIRST)
    return Context(spark, landing, namespace_prefix=f"t{uuid.uuid4().hex[:8]}_")


def land(ctx, day):
    wait_for_landing(ctx, day)
    land_to_bronze(ctx, day, register_files(ctx, day)["new"])


def run(ctx, day):
    land(ctx, day)
    return bronze_to_silver(ctx, day)


def run_all(ctx):
    """Land every fixture day, then one silver run: it catches up on all of them at once, the
    same way a run after a gap would (and it keeps these tests fast)."""
    d = FIRST
    while d <= LAST:
        land(ctx, d)
        d = date.fromordinal(d.toordinal() + 1)
    return bronze_to_silver(ctx, LAST)


def count(ctx, table):
    return ctx.spark.table(ctx.table("silver", table)).count()


def add_bronze_lines(ctx, day, lines, ingested_at, source_file="date=x/resent.jsonl"):
    """Put lines straight into bronze, as if a file had delivered them."""
    ensure_bronze_tables(ctx)
    rows = [(source_file, n, line) for n, line in enumerate(lines, start=1)]
    (ctx.spark.createDataFrame(rows, LINES_SCHEMA)
     .withColumn("event_id", F.get_json_object("raw", "$.event_id"))
     .withColumn("event_type", F.get_json_object("raw", "$.event_type"))
     .withColumn("business_date", F.lit(day))
     .withColumn("ingested_at", F.lit(ingested_at).cast("timestamp_ntz"))
     .writeTo(ctx.table("bronze", "events")).append())


def a_real_line(ctx, event_type="order_placed", key="o_delivered"):
    for path in sorted(ctx.landing_dir.rglob("*.jsonl")):
        for line in path.read_text().splitlines():
            e = json.loads(line)
            if e["event_type"] == event_type and e["logical_key"] == key:
                return e
    raise AssertionError(f"no {event_type} for {key} in the fixture")


def test_every_fixture_event_reaches_silver_once(ctx):
    run_all(ctx)
    # 7 orders minus o_2016, which the slice drops; their items, payments and reviews.
    assert {t: count(ctx, t) for t in ("events", "orders", "order_items", "order_payments",
                                       "reviews", "quarantine")} == {
        "events": 24, "orders": 6, "order_items": 5, "order_payments": 7, "reviews": 3,
        "quarantine": 0}


def test_values_are_typed_exactly(ctx):
    run_all(ctx)
    o = ctx.spark.sql(f"""SELECT customer_zip_code_prefix, CAST(order_purchase_timestamp AS STRING),
                                 CAST(order_estimated_delivery_date AS STRING)
                          FROM {ctx.table('silver', 'orders')} WHERE order_id = 'o_delivered'""").collect()
    assert tuple(o[0]) == ("01310", "2017-01-05 10:15:00", "2017-01-20 00:00:00")
    items = ctx.spark.sql(f"""SELECT order_item_id, price, freight_value FROM {ctx.table('silver', 'order_items')}
                              WHERE order_id = 'o_delivered' ORDER BY 1""").collect()
    assert [tuple(r) for r in items] == [(1, Decimal("58.90"), Decimal("13.29")),
                                         (2, Decimal("10.00"), Decimal("0.00"))]
    pays = ctx.spark.sql(f"""SELECT payment_sequential, payment_type, payment_installments, payment_value
                             FROM {ctx.table('silver', 'order_payments')}
                             WHERE order_id = 'o_never_approved'""").collect()
    assert [tuple(r) for r in pays] == [(1, "credit_card", 1, Decimal("49.90"))]


def test_events_keep_their_three_clocks(ctx):
    run_all(ctx)
    rows = ctx.spark.sql(f"""
        SELECT event_type, CAST(event_time AS STRING), CAST(emitted_at AS STRING),
               CAST(ingested_at AS STRING)
        FROM {ctx.table('silver', 'events')} WHERE order_id = 'o_undated' ORDER BY emitted_at""").collect()
    assert [tuple(r) for r in rows][-1] == (
        "delivered", None, "2017-01-08 08:00:00", "2017-01-09 00:00:00")


def test_retry_changes_nothing(ctx):
    first = run(ctx, date(2017, 1, 5))
    assert first["events"] == 3
    again = bronze_to_silver(ctx, date(2017, 1, 5))
    assert again["read"] == 0 and again["events"] == 0
    assert count(ctx, "events") == 3


def test_a_resent_event_in_a_later_file_is_dropped(ctx):
    run_all(ctx)
    copy = json.dumps(a_real_line(ctx), separators=(",", ":"))
    add_bronze_lines(ctx, LAST, [copy], "2017-01-20 00:00:00")
    result = bronze_to_silver(ctx, date(2017, 1, 19))
    assert result["read"] == 1 and result["events"] == 0 and result["orders"] == 0
    assert count(ctx, "events") == 24 and count(ctx, "order_items") == 5


def test_two_copies_in_one_batch_become_one_row(ctx):
    copy = json.dumps(a_real_line(ctx), separators=(",", ":"))
    add_bronze_lines(ctx, date(2017, 1, 5), [copy, copy], "2017-01-06 00:00:00")
    result = bronze_to_silver(ctx, date(2017, 1, 5))
    assert result["duplicates_in_batch"] == 1 and result["events"] == 1
    assert result["order_items"] == 2  # the order's two items, once each


def test_lines_breaking_the_contract_are_quarantined_with_reasons(ctx):
    good = a_real_line(ctx)
    def variant(**changes):
        e = json.loads(json.dumps(good))
        for path, value in changes.items():
            target = e
            *parents, last = path.split("__")
            for p in parents:
                target = target[p]
            target[last] = value
        e["event_id"] = uuid.uuid4().hex  # distinct ids, so dedup can't hide a case
        return json.dumps(e)

    lines = ["this is not json",
             variant(schema_version=99),
             variant(event_type="order_returned"),
             variant(payload__customer_id=None),
             variant(payload__order_purchase_timestamp="yesterday"),
             json.dumps({**json.loads(variant()), "payload": {**good["payload"], "items": [
                 {**good["payload"]["items"][0], "price": "abc"}]}}),
             variant()]  # one valid line among them
    add_bronze_lines(ctx, date(2017, 1, 5), lines, "2017-01-06 00:00:00")
    result = bronze_to_silver(ctx, date(2017, 1, 5))
    assert result["quarantined"] == 6 and result["events"] == 1

    reasons = {r.line_number: list(r.reasons) for r in ctx.spark.table(
        ctx.table("silver", "quarantine")).collect()}
    assert reasons == {1: ["invalid_json"], 2: ["unknown_schema_version"],
                       3: ["unknown_event_type"], 4: ["missing:customer_id"],
                       5: ["bad_value:order_purchase_timestamp"], 6: ["bad_value:items.price"]}


def test_a_crash_before_the_events_commit_is_repaired_by_the_retry(ctx):
    run_all(ctx)
    # Simulate dying after the order tables were written but before silver.events was.
    ctx.spark.sql(f"DELETE FROM {ctx.table('silver', 'events')}")
    bronze_to_silver(ctx, LAST)
    assert count(ctx, "events") == 24
    assert count(ctx, "order_items") == 5  # not duplicated by the re-read


def test_the_announced_schema_change_lands_unchanged_in_silver(spark, tmp_path):
    """New optional field from 5 Jan, schema_version 2 from 8 Jan (spec: reversible defects,
    announced schema change). Silver holds the same orders, keeps the earlier field name, and
    stores the new field; nothing is quarantined."""
    from dataclasses import replace

    from generator.defects import Defects
    landing = tmp_path / "landing"
    replay(MINI, landing, FIRST, LAST, purchased_from=FIRST,
           defects=replace(Defects.only({}), new_field_from="2017-01-05",
                           schema_v2_from="2017-01-08"))
    ctx = Context(spark, landing, namespace_prefix=f"t{uuid.uuid4().hex[:8]}_")
    run_all(ctx)
    assert count(ctx, "orders") == 6 and count(ctx, "quarantine") == 0
    versions = {r.schema_version for r in ctx.spark.table(ctx.table("silver", "events"))
                .select("schema_version").distinct().collect()}
    assert versions == {1, 2}
    o = ctx.spark.sql(f"""SELECT customer_zip_code_prefix FROM {ctx.table('silver', 'orders')}
                          WHERE order_id = 'o_delivered'""").collect()
    assert o[0][0] == "01310"   # sent as customer_zip_code under version 2
    items = ctx.spark.table(ctx.table("silver", "order_items")).where(
        "product_category_name IS NOT NULL").count()
    assert items > 0


def test_fields_the_contract_doesnt_list_are_named(spark):
    from jobs.contract import load_contracts, unknown_fields
    base = {"event_id": "x", "event_type": "order_placed", "schema_version": 1,
            "payload": {"order_id": "o", "customer_zip_code_prefix": "01310",
                        "items": [{"order_item_id": "1", "product_category_name": "pet"}],
                        "payments": []}}
    def line(version=1, **payload):
        e = json.loads(json.dumps(base))
        e["schema_version"] = version
        e["payload"].update(payload)
        return (json.dumps(e),)
    extra_item = json.loads(json.dumps(base))
    extra_item["payload"]["items"][0]["discount"] = "5"
    rows = [line(), line(coupon="X"), (json.dumps(extra_item),),
            line(version=2, customer_zip_code="01310"), line(customer_zip_code="01310")]
    df = spark.createDataFrame(rows, "raw string")
    got = [sorted(r[0]) for r in df.select(unknown_fields(F.col("raw"), load_contracts())).collect()]
    # Version 2 renamed the zip field, so under version 2 the old name is the unknown one.
    assert got == [[], ["coupon"], ["items[].discount"], ["customer_zip_code_prefix"],
                   ["customer_zip_code"]]
