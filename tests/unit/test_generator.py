"""The replay generator on hand-built Olist-shaped rows (tests/fixtures/olist_mini/README.md).

Expected values are worked out by hand from the fixture rows, not computed by the code under test.
"""
import hashlib
import json
from collections import Counter
from datetime import date
from pathlib import Path

import pytest

from generator.landing import MARKER, write_day
from generator.olist import build_events
from generator.replay import replay

MINI = Path(__file__).parent.parent / "fixtures" / "olist_mini"


@pytest.fixture(scope="module")
def events():
    return build_events(MINI)


def find(events, event_type, key):
    matches = [e for e in events if e["event_type"] == event_type and e["logical_key"] == key]
    assert len(matches) == 1, f"expected one {event_type} for {key}, got {len(matches)}"
    return matches[0]


def test_event_counts_per_type(events):
    assert Counter(e["event_type"] for e in events) == {
        "order_placed": 6,
        "payment_approved": 5,       # o_never_approved has none
        "handed_to_carrier": 4,      # o_delivered, o_undated, o_open, o_cancel_after_delivery
        "delivered": 3,              # two dated, one undated
        "order_cancelled": 2,
        "order_unavailable": 1,
        "review_submitted": 3,
    }


def test_event_ids_are_unique(events):
    assert len({e["event_id"] for e in events}) == len(events)


def test_order_placed_carries_items_payments_and_the_person(events):
    p = find(events, "order_placed", "o_delivered")
    assert p["event_time"] == p["emitted_at"] == "2017-01-05 10:15:00"
    pl = p["payload"]
    assert (pl["customer_unique_id"], pl["customer_zip_code_prefix"], pl["customer_state"]) == \
        ("person_a", "01310", "SP")
    # Values are Olist's text, verbatim: no float rounding, leading zeros kept.
    assert [i["price"] for i in pl["items"]] == ["58.90", "10.00"]
    # Payments are listed 2 then 1 in the CSV; the event lists them in sequence order.
    assert [p["payment_sequential"] for p in pl["payments"]] == ["1", "2"]


def test_never_approved_order_still_carries_its_payment(events):
    pl = find(events, "order_placed", "o_never_approved")["payload"]
    assert pl["items"] == []
    assert pl["payments"] == [{"payment_sequential": "1", "payment_type": "credit_card",
                               "payment_installments": "1", "payment_value": "49.90"}]


def test_each_address_travels_with_its_own_order(events):
    a = find(events, "order_placed", "o_delivered")["payload"]
    b = find(events, "order_placed", "o_open")["payload"]
    assert a["customer_unique_id"] == b["customer_unique_id"] == "person_a"
    assert (a["customer_city"], b["customer_city"]) == ("sao paulo", "curitiba")


def test_undated_delivery_arrives_a_day_after_the_last_milestone(events):
    d = find(events, "delivered", "o_undated")
    assert d["event_time"] is None
    assert d["payload"]["order_delivered_customer_date"] is None
    assert d["emitted_at"] == "2017-01-08 08:00:00"  # carrier handover 2017-01-07 08:00 + 1 day


def test_cancellations_have_no_event_time(events):
    c = find(events, "order_cancelled", "o_never_approved")
    assert c["event_time"] is None
    assert c["emitted_at"] == "2017-01-07 12:00:00"  # only milestone is the purchase
    u = find(events, "order_unavailable", "o_unavailable")
    assert u["emitted_at"] == "2017-01-07 13:20:00"  # approval + 1 day


def test_cancelled_after_delivery_keeps_both_events(events):
    # Olist: canceled, yet a delivery date exists. Both are observed; status derivation decides.
    assert find(events, "delivered", "o_cancel_after_delivery")["event_time"] == "2017-01-10 16:00:00"
    assert find(events, "order_cancelled", "o_cancel_after_delivery")["emitted_at"] == \
        "2017-01-11 16:00:00"


def test_open_order_has_no_final_event(events):
    types = {e["event_type"] for e in events if e["logical_key"] == "o_open"}
    assert types == {"order_placed", "payment_approved", "handed_to_carrier"}


def test_review_uses_answer_time_and_drops_free_text(events):
    r = find(events, "review_submitted", "o_delivered:r_2")
    assert r["event_time"] == "2017-01-14 08:00:00"
    assert set(r["payload"]) == {"order_id", "review_id", "review_score",
                                 "review_creation_date", "review_answer_timestamp"}


def test_replay_writes_hourly_files_and_a_marker_per_date(tmp_path):
    markers = replay(MINI, tmp_path, date(2017, 1, 5), date(2017, 1, 15))
    assert markers[0]["business_date"] == "2017-01-05"
    assert len(markers) == 11  # one per date, including empty ones

    jan5 = markers[0]
    # Purchases at 10:15, approval at 10:30 (hour 10) and a purchase at 23:40 (hour 23).
    assert [(f["file"], f["rows"]) for f in jan5["files"]] == [
        ("events_2017-01-05T10.jsonl", 2), ("events_2017-01-05T23.jsonl", 1)]
    assert markers[-1]["business_date"] == "2017-01-15"
    assert markers[-1]["files"] == []

    total = sum(f["rows"] for m in markers for f in m["files"])
    assert total == 24

    # The marker's counts and checksums describe the files exactly.
    for m in markers:
        folder = tmp_path / f"date={m['business_date']}"
        assert (folder / MARKER).exists()
        for f in m["files"]:
            content = (folder / f["file"]).read_bytes()
            assert hashlib.sha256(content).hexdigest() == f["sha256"]
            assert len(content.decode().splitlines()) == f["rows"]


def test_files_are_ordered_by_emission_time(tmp_path):
    replay(MINI, tmp_path, date(2017, 1, 7), date(2017, 1, 7))
    lines = (tmp_path / "date=2017-01-07" / "events_2017-01-07T18.jsonl").read_text().splitlines()
    assert [json.loads(x)["event_type"] for x in lines] == ["order_placed", "payment_approved"]


def test_rerun_writes_identical_bytes(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    replay(MINI, first, date(2017, 1, 5), date(2017, 1, 15))
    replay(MINI, second, date(2017, 1, 5), date(2017, 1, 15))
    replay(MINI, first, date(2017, 1, 5), date(2017, 1, 15))  # rerun into an existing landing
    files = sorted(p.relative_to(first) for p in first.rglob("*") if p.is_file())
    assert files == sorted(p.relative_to(second) for p in second.rglob("*") if p.is_file())
    assert all((first / f).read_bytes() == (second / f).read_bytes() for f in files)


def test_landing_files_are_never_modified(tmp_path, events):
    day = [e for e in events if e["emitted_at"].startswith("2017-01-05")]
    write_day(day, tmp_path, "2017-01-05")
    with pytest.raises(FileExistsError):
        write_day(day[:1], tmp_path, "2017-01-05")  # same file name, different content
