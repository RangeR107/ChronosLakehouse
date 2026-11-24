"""The nine late-data scenarios (spec: Late-arriving data and corrections), on the fixture.

Three replays share the work, so the suite stays fast enough for CI:

- clean: no defects, the expected end state.
- defected: one scripted defect of each kind, each on a different order, so their effects can be
  told apart. Every day runs twice (scenario 7) and one day crashes halfway (scenario 8).
- catch_up: the same defects, but no runs on 6 to 10 Jan; the 10 Jan run catches up (scenario 9).

Each test checks its scenario's own intermediate states, and every run must end with business
tables identical to the clean run's. Tests may read the scripted defects (the ground truth);
pipeline code never does.
"""
import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest
from conftest import throwaway_database  # tests/ is on sys.path under pytest
from pyspark.sql import functions as F

import jobs.build_facts as build_facts_module
from generator.defects import (
    CORRECTION,
    DUPLICATE,
    EARLY_FACT,
    HELD_SELLER,
    LATE,
    LATE_DIM_CHANGE,
    LATE_FILE,
    OUT_OF_ORDER,
    Defects,
)
from generator.replay import replay
from generator.source_db import conninfo
from jobs.context import Context, release_id
from jobs.keys import surrogate_key_py
from jobs.run_day import run_day

MINI = Path(__file__).resolve().parents[1] / "fixtures" / "olist_mini"
START, FEED_END, END = date(2017, 1, 1), date(2017, 1, 15), date(2017, 1, 18)
CRASH_DAY = date(2017, 1, 15)  # the late delivery lands: the snapshot changes
SKIPPED = {date(2017, 1, 6) + timedelta(days=n) for n in range(4)}  # 6 to 9 Jan: no runs

DEFECTS = Defects.only({
    (DUPLICATE, ("order_placed", "o_open")): 30,               # 1: resent 30 hours later
    (DUPLICATE, ("review_submitted", "o_delivered:r_1")): 5,
    (LATE, ("delivered", "o_delivered")): 3,                   # 2: delivered 12 Jan, lands 15 Jan
    (OUT_OF_ORDER, "o_cancel_after_delivery"): 24,             # 3: handover sent a day after
    (CORRECTION, ("order_placed", "o_undated")): 2,            # 4: payment wrong, fixed 2 days on
    (EARLY_FACT, "o_unavailable"): 2,                          # 5: person_d's row 2 days late
    (HELD_SELLER, "s_1"): 2,                                   # 5: seller s_1 2 days late
    (LATE_DIM_CHANGE, "o_open"): 2,                            # 6: person_a's move, 2 days late
    (LATE_FILE, "2017-01-06/14"): 1,                           # a file delivered a day late
})

BUSINESS = {
    "fact_order_fulfillment": ["order_id"],
    "fact_order_items": ["order_id", "order_item_id"],
    "fact_payments": ["order_id", "payment_sequential"],
    "fact_reviews": ["review_id", "order_id"],
    "dim_customer": ["customer_key"],
    "dim_geography": ["geography_key"],
    "dim_seller": ["seller_key"],
    "dim_product": ["product_key"],
    "agg_delivery_by_month_region": ["delivery_month", "region"],
}


def table(ctx, name, layer="gold"):
    return ctx.spark.table(ctx.table(layer, name))


def contents(ctx, name):
    return sorted(tuple(r) for r in table(ctx, name).collect())


def checksums(ctx):
    out = {}
    for layer in ("bronze", "silver", "gold"):
        for t in ctx.spark.sql(f"SHOW TABLES IN {ctx.namespace(layer)}").collect():
            df = table(ctx, t.tableName, layer)
            out[f"{layer}.{t.tableName}"] = tuple(df.agg(
                F.count("*"), F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)"))).collect()[0])
    return out


def state(ctx, conn):
    """What the tests look at after each day."""
    f = {r.order_id: r.asDict() for r in table(ctx, "fact_order_fulfillment").collect()}
    return {
        "fulfillment": f,
        "customers": {r.customer_key: r.asDict() for r in table(ctx, "dim_customer").collect()},
        "sellers": {r.seller_id: r.asDict() for r in table(ctx, "dim_seller").collect()},
        "items": {(r.order_id, r.order_item_id): r.asDict()
                  for r in table(ctx, "fact_order_items").collect()},
        "served": conn.execute("SELECT run_id FROM serving.load_state").fetchone()[0],
    }


def pipeline(spark, tmp, name, defects, runs, crash_on=None):
    """Replay START..END, running the pipeline on the dates in `runs`."""
    landing = tmp / name / "landing"
    with throwaway_database() as conn:
        ctx = Context(spark, landing, namespace_prefix=f"{name}{uuid.uuid4().hex[:6]}_",
                      source_conninfo=conninfo(conn.info.dbname), reference_dir=MINI)
        log = {"days": {}, "retries": {}, "results": {}, "crash": None}

        def run(d):
            if d not in runs:
                return
            if d == crash_on:
                log["crash"] = crash(ctx, conn, d)
            log["results"][d] = run_day(ctx, d)
            before = checksums(ctx)
            again = run_day(ctx, d)
            log["retries"][d] = (before, checksums(ctx), again)
            log["days"][d] = state(ctx, conn)

        replay(MINI, landing, START, END, purchased_from=START, conn=conn, after_day=run,
               defects=defects, feed_end=FEED_END)
        yield ctx, log


def crash(ctx, conn, d):
    """Run the day with build_facts dying after its first fact table; report what was seen."""
    served_before = conn.execute("SELECT run_id FROM serving.load_state").fetchone()[0]
    gold_before = {k: v for k, v in checksums(ctx).items() if k.startswith("gold.")}
    real = build_facts_module.merge_changed
    calls = []

    def dying(ctx_, table_, rows, keys, **kw):
        calls.append(table_)
        if table_.endswith("fact_order_items"):
            raise RuntimeError("simulated crash")
        return real(ctx_, table_, rows, keys, **kw)

    build_facts_module.merge_changed = dying
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            run_day(ctx, d)
    finally:
        build_facts_module.merge_changed = real
    return {"served_before": served_before,
            "served_after": conn.execute("SELECT run_id FROM serving.load_state").fetchone()[0],
            "tables_written": calls, "gold_before": gold_before,
            "gold_after": {k: v for k, v in checksums(ctx).items() if k.startswith("gold.")}}


def days(first=START, last=END):
    return {first + timedelta(days=n) for n in range((last - first).days + 1)}


@pytest.fixture(scope="module")
def clean(spark, tmp_path_factory):
    yield from pipeline(spark, tmp_path_factory.mktemp("s"), "clean", None, days())


@pytest.fixture(scope="module")
def defected(spark, tmp_path_factory):
    yield from pipeline(spark, tmp_path_factory.mktemp("s"), "defected", DEFECTS, days(),
                        crash_on=CRASH_DAY)


@pytest.fixture(scope="module")
def catch_up(spark, tmp_path_factory):
    yield from pipeline(spark, tmp_path_factory.mktemp("s"), "catchup", DEFECTS,
                        days() - SKIPPED)


pytestmark = pytest.mark.postgres


def assert_same_business_tables(a, b):
    for name in BUSINESS:
        assert contents(a, name) == contents(b, name), name


def test_defects_change_nothing_in_the_end(clean, defected):
    assert_same_business_tables(clean[0], defected[0])


def test_1_duplicates_are_dropped(clean, defected):
    ctx, _ = defected
    events = table(ctx, "events", "silver")
    assert events.count() == events.select("event_id").distinct().count()
    bronze = table(ctx, "events", "bronze").where("event_type = 'order_placed'")
    assert bronze.where("raw LIKE '%\"o_open\"%'").count() == 2  # both copies were received
    assert table(ctx, "fact_order_events").where(
        "order_id = 'o_open' AND event_type = 'order_placed'").count() == 1


def test_2_late_milestone_fills_the_row_when_it_lands(defected):
    ctx, log = defected
    on_12, on_15 = log["days"][date(2017, 1, 12)], log["days"][date(2017, 1, 15)]
    assert on_12["fulfillment"]["o_delivered"]["is_open"]  # happened on 12 Jan, not known yet
    assert on_15["fulfillment"]["o_delivered"]["final_status"] == "delivered"
    assert str(on_15["fulfillment"]["o_delivered"]["order_delivered_customer_date"]) == (
        "2017-01-12 09:05:00")
    # January is dirty for the run that received it. (Read from the table: on 15 Jan the first
    # attempt crashed after recording it, so the rerun rightly recorded nothing new.)
    dirty = {(r.metric_id, str(r.period_start)) for r in table(ctx, "dirty_periods")
             .where("run_id = 'run_2017-01-15'").collect()}
    assert ("q1_late_delivery_rate", "2017-01-01") in dirty


def test_3_out_of_order_arrival_gives_the_same_row(clean, defected):
    order = "o_cancel_after_delivery"
    for log in (clean[1], defected[1]):
        assert log["days"][END]["fulfillment"][order]["final_status"] == "canceled"
    assert clean[1]["days"][END]["fulfillment"][order] == defected[1]["days"][END][
        "fulfillment"][order]
    # On 10 Jan the delivery was known and the handover wasn't, yet.
    early = defected[1]["days"][date(2017, 1, 10)]["fulfillment"][order]
    assert early["order_delivered_customer_date"] is not None
    assert early["order_delivered_carrier_date"] is None


def test_4_a_correction_is_the_next_version_and_wins(defected):
    ctx, log = defected
    versions = table(ctx, "fact_order_events").where(
        "order_id = 'o_undated' AND event_type = 'order_placed'").orderBy("version").collect()
    assert [(v.version, v.is_current_version) for v in versions] == [(1, False), (2, True)]
    assert versions[1].supersedes_event_id == versions[0].event_id
    pays = table(ctx, "fact_payments").where("order_id = 'o_undated'").collect()
    assert [str(p.payment_value) for p in pays] == ["135.60"]  # Olist's value, not 149.16
    flags = log["days"][date(2017, 1, 6)]["fulfillment"]["o_undated"]["dq_flags"]
    assert "payment_mismatch" in flags  # the wrong value was visible until the correction
    assert "payment_mismatch" not in log["days"][END]["fulfillment"]["o_undated"]["dq_flags"]


def test_5_early_arriving_customer_uses_an_inferred_member(defected):
    _, log = defected
    early, final = log["days"][date(2017, 1, 6)], log["days"][END]
    key = surrogate_key_py("person_d", "2017-01-06 13:00:00")
    assert early["customers"][key]["is_inferred"]
    assert early["customers"][key]["customer_city"] is None
    assert not final["customers"][key]["is_inferred"]
    assert final["customers"][key]["customer_city"] == "salvador"
    # No fact rewrite: the placeholder has the key the real row has.
    assert early["fulfillment"]["o_unavailable"]["customer_key"] == key
    assert final["fulfillment"]["o_unavailable"]["customer_key"] == key


def test_5_early_arriving_seller_rekeys_only_that_sellers_rows(defected):
    # s_1's master row lands on 7 Jan (two days after its first order). Its items carried no
    # location until then; on 7 Jan they are re-keyed, through the dirty-period list.
    ctx, log = defected
    before, after = log["days"][date(2017, 1, 6)], log["days"][date(2017, 1, 7)]
    assert before["sellers"]["s_1"]["is_inferred"] and not after["sellers"]["s_1"]["is_inferred"]
    s_1 = surrogate_key_py("s_1")
    changed = {k for k in before["items"] if after["items"][k] != before["items"][k]}
    assert changed and {after["items"][k]["seller_key"] for k in changed} == {s_1}
    assert changed == {k for k, row in before["items"].items() if row["seller_key"] == s_1}
    for k in changed:
        assert before["items"][k]["seller_geography_key"] is None
        assert after["items"][k]["seller_geography_key"] == surrogate_key_py(
            "13023", "campinas", "SP")
    dirty = {(r.metric_id, str(r.period_start)) for r in table(ctx, "dirty_periods")
             .where("run_id = 'run_2017-01-07'").collect()}
    assert ("q3_seller_handover", "2017-01-01") in dirty  # the items' shipping-limit month


def test_6_late_dimension_change_rekeys_only_that_persons_facts(defected):
    _, log = defected
    before, after = log["days"][date(2017, 1, 8)], log["days"][date(2017, 1, 9)]
    first = surrogate_key_py("person_a", "2017-01-05 10:15:00")
    second = surrogate_key_py("person_a", "2017-01-07 09:00:00")
    assert before["fulfillment"]["o_open"]["customer_key"] == first  # the move isn't known
    assert after["fulfillment"]["o_open"]["customer_key"] == second
    changed = {o for o in after["fulfillment"]
               if after["fulfillment"][o] != before["fulfillment"].get(o)}
    assert changed == {"o_open"}
    assert after["fulfillment"]["o_delivered"]["customer_key"] == first


def test_scd2_version_at_purchase_has_the_orders_address(defected):
    ctx, _ = defected
    orders = table(ctx, "orders", "silver").select(
        "order_id", "customer_zip_code_prefix", "customer_city", "customer_state").distinct()
    f = table(ctx, "fact_order_fulfillment").select("order_id", "customer_key")
    c = table(ctx, "dim_customer").select(
        "customer_key", F.col("customer_zip_code_prefix").alias("z"),
        F.col("customer_city").alias("c"), F.col("customer_state").alias("s"))
    rows = orders.join(f, "order_id").join(c, "customer_key", "left").collect()
    assert rows and all((r.customer_zip_code_prefix, r.customer_city, r.customer_state)
                        == (r.z, r.c, r.s) for r in rows)


def test_7_rerunning_any_day_changes_nothing(defected):
    _, log = defected
    assert set(log["retries"]) == days()
    for d, (before, after, again) in log["retries"].items():
        assert after == before, d
        assert again["already_published"] == release_id(d), d


def test_8_a_crash_halfway_leaves_serving_alone_and_the_rerun_finishes(clean, defected):
    _, log = defected
    crash_ = log["crash"]
    assert crash_["tables_written"][-1].endswith("fact_order_items")
    assert crash_["served_before"] == crash_["served_after"] == "run_2017-01-14"
    assert log["days"][CRASH_DAY]["served"] == "run_2017-01-15"
    # The rerun after the crash ends the day exactly where an uninterrupted run would have.
    assert log["days"][CRASH_DAY]["items"] == clean[1]["days"][CRASH_DAY]["items"]


def test_8_a_crash_halfway_leaves_main_unchanged(defected):
    # Phase 3: the crashed run wrote only to its branch, so main didn't move.
    crash_ = defected[1]["crash"]
    # Guard: the crash day must have had something to write, or this could pass by accident.
    assert crash_["tables_written"][-1].endswith("fact_order_items")
    assert crash_["gold_after"] == crash_["gold_before"]


def test_9_catching_up_after_a_gap_gives_the_same_tables(clean, catch_up):
    assert_same_business_tables(clean[0], catch_up[0])


def test_the_late_file_was_reported_then_loaded(defected):
    _, log = defected
    on_6, on_7 = log["results"][date(2017, 1, 6)], log["results"][date(2017, 1, 7)]
    late = "date=2017-01-06/events_2017-01-06T14.jsonl"
    assert late in on_6["missing"]
    assert late not in on_7["missing"]



def test_a_full_rebuild_from_bronze_gives_the_same_tables(spark, clean, defected):
    """The runbook's fix for a logic bug: keep bronze, reset silver and gold, and run every date
    again in order. Done here in a fresh namespace holding a copy of the defected run's bronze,
    so the other tests' tables are untouched."""
    ctx, _ = defected
    rebuilt = Context(spark, ctx.landing_dir, namespace_prefix=f"rebuild{uuid.uuid4().hex[:6]}_",
                      source_conninfo=ctx.source_conninfo, reference_dir=MINI)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {rebuilt.namespace('bronze')}")
    for t in spark.sql(f"SHOW TABLES IN {ctx.namespace('bronze')}").collect():
        spark.sql(f"CREATE TABLE {rebuilt.table('bronze', t.tableName)} USING iceberg "
                  f"TBLPROPERTIES ('format-version' = '2') "
                  f"AS SELECT * FROM {ctx.table('bronze', t.tableName)}")
    for d in sorted(days()):
        result = run_day(rebuilt, d)
        assert result["files"] == 0 and result["events"] == 0  # bronze already has everything
    assert_same_business_tables(clean[0], rebuilt)
