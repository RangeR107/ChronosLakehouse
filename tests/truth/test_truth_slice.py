"""Phase 1 "Done when": the truth test passes on 60 replayed days, and rerunning any day gives
identical output (spec: Phased build plan; Testing and evaluation, truth test part 1 and the
idempotency test).

The slice is Jan and Feb 2017 orders, 2017-01-01 to 2017-03-01, with truth as known at the end of
day 60 (docs/decisions.md). The pipeline runs every day through run_day, the same function the
runner and the Phase 2 DAG call. The expected tables come from reference.py, DuckDB SQL over the
Olist CSVs. The real slice needs data/raw/olist and takes about ten minutes, so it runs locally;
CHRONOS_REQUIRE_OLIST=1 makes a missing data folder fail. The same comparison runs in CI on the
hand-built fixture (tests/fixtures/olist_mini, 15 days). Both need PostgreSQL.
"""
import math
import os
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from conftest import throwaway_database  # tests/ is on sys.path under pytest
from pyspark.sql import functions as F
from reference import build  # tests/truth/reference.py, beside this file

from generator.defects import DRAIN_DAYS, Defects
from generator.olist import load
from generator.replay import plan, replay
from generator.source_db import conninfo
from jobs.batches import RELEASED, decide
from jobs.context import Context, release_id, run_time
from jobs.run_day import run_day
from sentinel.gate import SentinelBlocked

OLIST = Path(__file__).resolve().parents[2] / "data" / "raw" / "olist"
MINI = Path(__file__).resolve().parents[1] / "fixtures" / "olist_mini"


@dataclass(frozen=True)
class Slice:
    name: str
    data: Path
    start: date
    end: date
    retried: tuple[date, ...]  # days run twice: first, middle and last
    defects: Defects

    @property
    def last_run(self) -> date:
        return self.end + timedelta(days=DRAIN_DAYS)  # the drain lets every delay land


# The fixture has 7 orders, so the spec's rates would inject nothing: it runs with rates high
# enough that every defect kind lands on it. The real slice runs the spec's suggested rates.
# The announced schema change lands inside the fixture's 15 days; the real slice uses the
# default dates (1 and 15 Feb 2017), inside its two months.
HEAVY = Defects(seed=11, late=0.5, out_of_order=0.5, duplicate=0.5, correction=0.3,
                early_fact=0.5, held_seller=0.5, late_dim_change=1.0, late_file=0.2,
                new_field_from="2017-01-05", schema_v2_from="2017-01-10")


# The real slice runs locally (it needs data/raw/olist); the fixture runs the same comparison
# on every push, in CI (docs/decisions.md, test split).
SLICES = [
    pytest.param(Slice("mini", MINI, date(2017, 1, 1), date(2017, 1, 15),
                       (date(2017, 1, 1), date(2017, 1, 8), date(2017, 1, 15)), HEAVY),
                 id="mini"),
    pytest.param(Slice("olist", OLIST, date(2017, 1, 1), date(2017, 3, 1),
                       (date(2017, 1, 1), date(2017, 1, 31), date(2017, 3, 1)), Defects()),
                 id="olist", marks=pytest.mark.olist),
]

# Business tables and their grain (spec: Data model). The event ledger is checked against the
# ground-truth manifest in Phase 2, once there are defects for it to record.
GRAIN = {
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

pytestmark = pytest.mark.postgres


def checksums(ctx):
    """Row count and an order-independent content hash of every table the pipeline writes."""
    out = {}
    for layer in ("bronze", "silver", "gold"):
        for t in ctx.spark.sql(f"SHOW TABLES IN {ctx.namespace(layer)}").collect():
            df = ctx.spark.table(ctx.table(layer, t.tableName))
            out[f"{layer}.{t.tableName}"] = tuple(df.agg(
                F.count("*"), F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)"))).collect()[0])
    return out


@pytest.fixture(scope="module", params=SLICES)
def slice_run(request, spark, tmp_path_factory):
    s = request.param
    if not (s.data / "olist_orders_dataset.csv").exists():
        if os.environ.get("CHRONOS_REQUIRE_OLIST") == "1":
            pytest.fail(f"Olist data required but missing from {s.data}")
        pytest.skip(f"Olist data not found in {s.data}")
    landing = tmp_path_factory.mktemp(s.name) / "landing"
    with throwaway_database() as conn:
        ctx = Context(spark, landing, namespace_prefix=f"{s.name}{uuid.uuid4().hex[:6]}_",
                      source_conninfo=conninfo(conn.info.dbname), reference_dir=s.data)
        retries, released = {}, []

        def run(d):
            try:
                run_day(ctx, d)
            except SentinelBlocked:
                # A false alarm on real data (1 Feb 2017: an unusual count of unavailable
                # orders). Do what the runbook says a person does: release the day's batches,
                # so the next run takes them in. The tables must still converge exactly.
                decide(ctx, d, RELEASED)
                released.append(d)
                return
            if d in s.retried:
                before = checksums(ctx)
                again = run_day(ctx, d)
                retries[d] = (before, checksums(ctx), again)

        # Each day runs as soon as the generator has written it, as in production.
        replay(s.data, landing, s.start, s.last_run, purchased_from=s.start, conn=conn,
               after_day=run, defects=s.defects, feed_end=s.end)
        served = conn.execute("SELECT * FROM serving.delivery_by_month_region").fetchall()
        yield s, ctx, retries, served


def normalise(value):
    """Doubles compare to 9 decimal places; everything else exactly."""
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 9)
    if isinstance(value, Decimal):
        return value.normalize()
    if isinstance(value, list):
        return tuple(value)
    return value


def keyed(rows, columns, grain):
    idx = [columns.index(c) for c in grain]
    out = {}
    for r in rows:
        r = tuple(normalise(v) for v in r)
        key = tuple(r[i] for i in idx)
        assert key not in out, f"duplicate grain {key}"
        out[key] = r
    return out


@pytest.fixture(scope="module")
def ref(slice_run):
    s = slice_run[0]
    return build(s.data, s.start, s.end)


@pytest.mark.parametrize("table", list(GRAIN))
def test_business_table_equals_reference(slice_run, ref, table):
    _, ctx, _, _ = slice_run
    gold = ctx.spark.table(ctx.table("gold", table))
    columns = gold.columns
    expected = keyed(ref.execute(f"SELECT {', '.join(columns)} FROM ref_{table}").fetchall(),
                     columns, GRAIN[table])
    got = keyed([tuple(r) for r in gold.collect()], columns, GRAIN[table])
    missing, extra = expected.keys() - got.keys(), got.keys() - expected.keys()
    differ = [k for k in expected.keys() & got.keys() if expected[k] != got[k]]
    detail = [dict(zip(columns, expected[k])) | {"_got": dict(zip(columns, got[k]))}
              for k in differ[:3]]
    assert not missing and not extra and not differ, (
        f"{table}: {len(expected)} expected, {len(got)} built; {len(missing)} missing "
        f"(e.g. {sorted(missing)[:3]}), {len(extra)} extra (e.g. {sorted(extra)[:3]}), "
        f"{len(differ)} differ, e.g. {detail}")


def test_slice_is_not_trivial(slice_run):
    s, ctx, _, _ = slice_run
    counts = {t: ctx.spark.table(ctx.table("gold", t)).count() for t in GRAIN}
    least = {"mini": (6, 3, 2), "olist": (2000, 1000, 6)}[s.name]
    assert (counts["fact_order_fulfillment"], counts["fact_reviews"],
            counts["agg_delivery_by_month_region"]) >= least


def test_rerunning_a_day_changes_nothing(slice_run):
    s, _, retries, _ = slice_run
    assert set(retries) == set(s.retried)
    for day, (before, after, again) in retries.items():
        assert after == before, day
        # Rerunning a published date republishes nothing (docs/runbook.md).
        assert again["already_published"] == release_id(day), day


def test_serving_holds_the_gold_aggregate(slice_run):
    _, ctx, _, served = slice_run
    gold = ctx.spark.table(ctx.table("gold", "agg_delivery_by_month_region"))
    columns = gold.columns
    assert keyed(served, columns, GRAIN["agg_delivery_by_month_region"]) == keyed(
        [tuple(r) for r in gold.collect()], columns, GRAIN["agg_delivery_by_month_region"])


def test_event_ledger_matches_the_ground_truth(slice_run):
    """Truth test part 2: every event version the generator sent, each once, with the arrival
    time of its first copy (a late file arrives a day after its date)."""
    s, ctx, _, _ = slice_run
    feed = plan(load(s.data), s.start, s.end, s.defects)
    first_arrival = {}
    for day, events in feed.days.items():
        arrives = date.fromisoformat(day)
        for e in events:
            hour_late = e["emitted_at"][11:13] in feed.late_hours.get(day, frozenset())
            when = arrives + timedelta(days=1 if hour_late else 0)
            if e["event_id"] not in first_arrival or when < first_arrival[e["event_id"]][1]:
                first_arrival[e["event_id"]] = (e["version"], when)
    expected = {eid: (v, run_time(d)) for eid, (v, d) in first_arrival.items()}
    got = {r.event_id: (r.version, r.ingested_at) for r in ctx.spark.table(
        ctx.table("gold", "fact_order_events")).collect()}
    assert got == expected
    kinds = {n["defect"] for n in feed.notes}
    assert {"late_event", "duplicate", "correction", "out_of_order", "new_optional_field",
            "schema_version_2"} <= kinds, kinds


def test_every_held_batch_was_released_and_published(slice_run):
    """A day the audit blocked (a false alarm here) was released, and the next run published
    it: nothing is left held, and the business tables above still equal the reference."""
    _, ctx, _, _ = slice_run
    with psycopg.connect(ctx.source_conninfo) as conn:
        states = conn.execute("SELECT state, count(*) FROM metrics.batch_holds "
                              "WHERE namespace = %s GROUP BY 1",
                              (ctx.namespace_prefix,)).fetchall()
    assert all(state == "published" for state, _ in states), states
