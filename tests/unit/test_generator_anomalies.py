"""The evaluation's irreversible anomalies (generator/anomalies.py) on a hand-built day."""
from collections import Counter
from datetime import date

from generator import anomalies as an
from generator.events import make_event

DAY = "2017-10-02"
A = an.Anomalies(date(2017, 10, 1), date(2017, 10, 31), per_type=2)


def placed(order_id, hour, price="10.00", pay_type="credit_card"):
    payload = {"order_id": order_id, "customer_id": "c", "customer_unique_id": "p",
               "customer_zip_code_prefix": "01310", "customer_city": "sao paulo",
               "customer_state": "SP", "order_purchase_timestamp": f"{DAY} {hour}:00:00",
               "order_estimated_delivery_date": "2017-10-20 00:00:00",
               "items": [{"order_item_id": "1", "product_id": "x", "seller_id": "s",
                          "shipping_limit_date": "2017-10-05 00:00:00", "price": price,
                          "freight_value": "5.00"}],
               "payments": [{"payment_sequential": "1", "payment_type": pay_type,
                             "payment_installments": "1", "payment_value": "15.00"}]}
    return make_event("order_placed", [order_id], f"{DAY} {hour}:00:00", f"{DAY} {hour}:00:00",
                      payload)


EVENTS = [placed(f"o{n}", f"{n % 24:02d}") for n in range(40)]


def test_each_type_gets_its_share_of_distinct_days():
    s = A.schedule()
    assert Counter(s.values()) == {t: 2 for t in an.TYPES} and len(set(s)) == 12
    assert A.schedule() == s  # the same seed, the same days


def test_a_volume_spike_adds_copies_under_new_order_ids():
    d = an.apply(A, "volume_spike", DAY, EVENTS)
    extra = d.events[len(EVENTS):]
    assert len(extra) == 40 * d.note["copies"] and 2 <= d.note["copies"] <= 4
    assert all(e["payload"]["order_id"].startswith("spike") for e in extra)
    assert set(d.note["event_ids"]) == {e["event_id"] for e in extra}


def test_a_price_shift_keeps_payments_consistent():
    a = an.Anomalies(date(2017, 10, 1), date(2017, 10, 31), seed=1)
    for seed in range(20):  # find a seed that shifts prices
        a = an.Anomalies(date(2017, 10, 1), date(2017, 10, 31), seed=seed)
        d = an.apply(a, "mix_shift", DAY, EVENTS)
        if d.note["shift"].startswith("price"):
            break
    p = d.events[0]["payload"]
    factor = int(d.note["shift"].split("x")[1])
    assert p["items"][0]["price"] == f"{10 * factor}.00"
    assert p["payments"][0]["payment_value"] == f"{15 * factor}.00"  # still price + freight
    assert EVENTS[0]["payload"]["items"][0]["price"] == "10.00"  # the original is untouched


def test_a_schema_break_renames_a_required_field():
    for seed in range(40):  # find a seed whose rename hits order_placed, the only type here
        d = an.apply(an.Anomalies(date(2017, 10, 1), date(2017, 10, 31), seed=seed),
                     "schema_break", DAY, EVENTS)
        if d.note["renamed"].startswith("order_placed"):
            break
    old, new = d.note["renamed"].split(".", 1)[1].split(" -> ")
    assert len(d.note["event_ids"]) == 40
    assert all(old not in e["payload"] and new in e["payload"] for e in d.events)


def test_missing_files_are_listed_but_never_written(tmp_path):
    from generator.landing import write_day
    d = an.apply(A, "missing_files", DAY, EVENTS)
    marker = write_day(d.events, tmp_path, DAY, d.never_hours)
    listed = {f["file"] for f in marker["files"]}
    written = {p.name for p in (tmp_path / f"date={DAY}").glob("events_*")}
    assert listed - written == {f"events_{DAY}T{h}.jsonl" for h in d.never_hours}
    assert 1 <= len(d.never_hours) <= 12


def test_an_anomaly_days_late_file_matches_its_marker(tmp_path):
    """The evaluation crashed on 20 Dec 2017: 19 Dec had an anomaly and a late file, and the
    late file was written from the day's original events, so its checksum missed the marker."""
    import hashlib
    import json
    from pathlib import Path

    from generator.defects import LATE_FILE, Defects
    from generator.replay import replay
    mini = Path(__file__).parent.parent / "fixtures" / "olist_mini"
    replay(mini, tmp_path, date(2017, 1, 1), date(2017, 1, 7), purchased_from=date(2017, 1, 1),
           defects=Defects.only({(LATE_FILE, "2017-01-05/10"): 1}),
           anomalies=an.Anomalies(date(2017, 1, 1), date(2017, 1, 31),
                                  scripted={"2017-01-05": "mix_shift"}))
    folder = tmp_path / "date=2017-01-05"
    listed = {f["file"]: f["sha256"] for f in
              json.loads((folder / "_complete.json").read_text())["files"]}
    late = folder / "events_2017-01-05T10.jsonl"
    assert hashlib.sha256(late.read_bytes()).hexdigest() == listed[late.name]
