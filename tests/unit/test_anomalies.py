"""Anomaly decisions over profile numbers (no Spark, no database)."""
from datetime import date, timedelta

from sentinel.checks.anomalies import defects, volume_and_freshness
from sentinel.release import is_complete

ORDERS = ("gold.fact_order_fulfillment", "", "orders_placed")
NO_ITEMS = ("gold.fact_order_fulfillment", "no_items", "defect_count")
DAY = date(2017, 2, 8)


def baseline(orders_per_day: int, defects_on: dict[int, int]) -> dict:
    """28 published days before DAY with a steady order count and a few no-items orders."""
    days = [DAY - timedelta(days=k) for k in range(1, 29)]
    return {ORDERS: {d: orders_per_day for d in days},
            NO_ITEMS: {days[k]: v for k, v in defects_on.items()}}


def no_items(today_orders: int, today_defects: int, past: dict):
    # 28 days of history when there is a baseline: past warm-up.
    results = defects({ORDERS: today_orders, NO_ITEMS: today_defects}, past, 28 if past else 0)
    return next(r for r in results if r.check == "known_defect:no_items")


def test_a_few_unavailable_orders_is_an_ordinary_day():
    # The real slice on 8 Feb 2017: 5 of 95, against a baseline rate of about 2%.
    past = baseline(70, {0: 1, 1: 4, 2: 1, 4: 3, 5: 1, 6: 6, 7: 3, 8: 2, 12: 1, 15: 1})
    r = no_items(95, 5, past)
    assert r.method == "rate_limit" and r.status == "pass" and r.severity == "block"


def test_a_real_spike_blocks():
    past = baseline(70, {0: 1, 1: 4, 2: 1, 4: 3, 5: 1, 6: 6, 7: 3, 8: 2, 12: 1, 15: 1})
    r = no_items(95, 20, past)
    assert r.status == "fail" and r.blocks


def test_no_baseline_only_warns():
    r = no_items(95, 20, {})
    assert r.method == "warm_up" and r.status == "pass" and r.severity == "warn"


def test_stale_data_warns_and_marks_the_release_incomplete():
    key = ("silver.events", "", "freshness_hours")
    past = {key: {DAY - timedelta(weeks=k): 20.0 + k % 3 for k in range(1, 9)}}
    r = next(x for x in volume_and_freshness({key: 200.0}, past, DAY, sale_day=False)
             if x.check == "freshness")
    assert r.status == "fail" and r.severity == "warn"
    assert not is_complete([f"{r.table} {r.check}"])
    assert is_complete(["silver.reviews drift:score"])


VOLUME = ("silver.events", "", "orders_landed")


def volume(today: float, past_days: dict):
    return next(r for r in volume_and_freshness({VOLUME: today}, {VOLUME: past_days}, DAY,
                                                sale_day=False) if r.check == "volume")


def test_volume_uses_the_latest_eight_published_weekdays():
    # Week 3 was blocked, so it has no published profile: weeks 1-2 and 4-9 still make eight.
    past = {DAY - timedelta(weeks=k): 400.0 + 10 * (k % 3) for k in range(1, 10) if k != 3}
    r = volume(1600.0, past)       # a duplicated feed: four times the usual orders
    assert r.status == "fail" and r.severity == "block" and r.detail["weeks"] == 8
    assert volume(560.0, past).status == "pass"   # a busy holiday, +40%, is not a block


def test_a_sale_day_spike_only_warns():
    past = {DAY - timedelta(weeks=k): 400.0 + k for k in range(1, 9)}
    r = next(x for x in volume_and_freshness({VOLUME: 1600.0}, {VOLUME: past}, DAY,
                                             sale_day=True) if x.check == "volume")
    assert r.status == "fail" and r.severity == "warn"


def test_warm_up_counts_days_since_the_first_release_not_published_days_in_the_window():
    # Ten of the last 28 days published (the rest blocked): still past warm-up.
    past = {ORDERS: {DAY - timedelta(days=k): 70 for k in range(1, 11)},
            NO_ITEMS: {DAY - timedelta(days=1): 1}}
    r = next(x for x in defects({ORDERS: 95, NO_ITEMS: 20}, past, history_days=200)
             if x.check == "known_defect:no_items")
    assert r.status == "fail" and r.severity == "block"
    assert next(x for x in defects({ORDERS: 95, NO_ITEMS: 20}, past, history_days=20)
                if x.check == "known_defect:no_items").severity == "warn"
