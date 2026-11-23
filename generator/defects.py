"""Reversible defects: disturb how the true event stream arrives, never what it says in the end
(spec: Event model, "Reversible defects (truth replay)").

Every defect is reversed by the stream itself: a late event still arrives, a duplicate repeats an
event_id, a wrong value is followed by a correction carrying Olist's value, a delayed master row
still reaches the database. So a correct pipeline ends with exactly Olist's business tables.

Each decision is a SHA-256 of the seed, the defect and the thing it applies to, turned into a
number in [0, 1). The same event gets the same defect in every replay and in any slice, and
nothing depends on the order events are processed in. Tests can also script defects exactly.

Everything injected is recorded in the ground-truth manifest. Only tests read it; pipeline code
never does (CLAUDE.md).
"""
import copy
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from generator.events import make_event

TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Defect kinds, in the precedence used when one event could get several: each event gets at most
# one timing defect, so delays can't add up past the drain period.
CORRECTION, LATE, OUT_OF_ORDER, DUPLICATE = ("correction", "late_event", "out_of_order",
                                             "duplicate")
LATE_FILE, EARLY_FACT, HELD_SELLER, LATE_DIM_CHANGE = ("late_file", "early_arriving_customer",
                                                       "held_back_seller", "late_dimension_change")

# The longest delay any single defect adds (late events: 10 days), plus a late file's day.
DRAIN_DAYS = 11


@dataclass(frozen=True)
class Defects:
    """Rates (spec's suggested starting values) or scripted picks.

    scripted maps (kind, identity) to a parameter (usually a delay in days). Identities: an
    event's (event_type, logical_key); an order_id for out_of_order and master-row delays; a
    seller_id; a file's "YYYY-MM-DD/HH". With rates off, only scripted defects happen.
    """
    seed: int = 7
    late: float = 0.05
    out_of_order: float = 0.01
    duplicate: float = 0.02
    correction: float = 0.005
    early_fact: float = 0.002
    held_seller: float = 0.002
    late_dim_change: float = 0.10
    late_file: float = 0.001
    scripted: dict = field(default_factory=dict)

    @classmethod
    def only(cls, scripted: dict) -> "Defects":
        return cls(late=0, out_of_order=0, duplicate=0, correction=0, early_fact=0,
                   held_seller=0, late_dim_change=0, late_file=0, scripted=scripted)

    def draw(self, kind: str, identity) -> float:
        text = f"{self.seed}\x1f{kind}\x1f{identity}"
        return int(hashlib.sha256(text.encode()).hexdigest()[:15], 16) / 16**15

    def pick(self, kind: str, identity, rate: float, low: int, high: int):
        """The defect's parameter (days, from low to high) if it applies, else None."""
        if (kind, identity) in self.scripted:
            return self.scripted[(kind, identity)]
        if rate and self.draw(kind, identity) < rate:
            return low + int(self.draw(kind + ":param", identity) * (high - low + 1))
        return None


def shift(ts: str, **delta) -> str:
    return (datetime.strptime(ts, TS_FORMAT) + timedelta(**delta)).strftime(TS_FORMAT)


def wrong_payload(event: dict) -> dict:
    """A plausible wrong first value, of the kinds the spec names: delivery time or payment."""
    p = copy.deepcopy(event["payload"])
    if event["event_type"] == "delivered":
        p["order_delivered_customer_date"] = shift(p["order_delivered_customer_date"], days=1)
    else:
        first = p["payments"][0]
        value = Decimal(first["payment_value"])
        first["payment_value"] = f"{(value * Decimal('1.1')) if value else Decimal(10):.2f}"
    return p


def correct(event: dict, days: int) -> tuple[list[dict], dict]:
    """Version 1 carries a wrong value; version 2, a few days later, restores Olist's."""
    bad = wrong_payload(event)
    time = event["event_time"]
    v1_time = bad.get("order_delivered_customer_date", time)
    v1_emitted = max(event["emitted_at"], v1_time)
    key = event["logical_key"].split(":")
    v1 = make_event(event["event_type"], key, v1_time, v1_emitted, bad)
    v2 = make_event(event["event_type"], key, time, shift(v1_emitted, days=days),
                    event["payload"], version=2, supersedes_event_id=v1["event_id"])
    note = {"defect": CORRECTION, "event_type": event["event_type"],
            "logical_key": event["logical_key"], "wrong_event_id": v1["event_id"],
            "corrected_event_id": v2["event_id"], "corrected_after_days": days}
    return [v1, v2], note


def inject_events(events: list[dict], d: Defects) -> tuple[list[dict], list[dict]]:
    """Apply the event-level defects. Returns the events as they will be emitted, and notes."""
    out, notes, done = [], [], set()
    by_order = {}
    for e in events:
        by_order.setdefault((e["event_type"], e["logical_key"]), e)

    for e in events:
        ident = (e["event_type"], e["logical_key"])
        timed = e["event_time"] is not None
        can_correct = ((e["event_type"] == "delivered" and timed)
                       or (e["event_type"] == "order_placed" and e["payload"]["payments"]))
        days = d.pick(CORRECTION, ident, d.correction, 1, 3) if can_correct else None
        if days is not None:
            versions, note = correct(e, days)
            out.extend(versions)
            notes.append(note)
            done.add(ident)
            continue
        if e["event_type"] in ("handed_to_carrier", "delivered") and timed:
            days = d.pick(LATE, ident, d.late, 1, 10)
            if days is not None:
                e = {**e, "emitted_at": shift(e["event_time"], days=days)}
                notes.append({"defect": LATE, "event_type": e["event_type"],
                              "logical_key": e["logical_key"], "event_id": e["event_id"],
                              "delay_days": days})
                done.add(ident)
        out.append(e)

    # Out of order: the carrier handover is sent 1 to 48 hours after the delivery event.
    for i, e in enumerate(out):
        if e["event_type"] != "handed_to_carrier" or e["event_time"] is None:
            continue
        order = e["logical_key"]
        delivered = by_order.get(("delivered", order))
        if (delivered is None or delivered["event_time"] is None
                or ("handed_to_carrier", order) in done or ("delivered", order) in done):
            continue
        hours = d.pick(OUT_OF_ORDER, order, d.out_of_order, 1, 48)
        if hours is not None:
            out[i] = {**e, "emitted_at": shift(delivered["emitted_at"], hours=hours)}
            notes.append({"defect": OUT_OF_ORDER, "logical_key": order,
                          "event_id": e["event_id"], "emitted_at": out[i]["emitted_at"]})
            done.add(("handed_to_carrier", order))

    # Exact duplicates: the same event, sent again 1 to 48 hours later (often in another file).
    dupes = []
    for e in out:
        ident = (e["event_type"], e["logical_key"])
        if ident in done:
            continue
        hours = d.pick(DUPLICATE, ident, d.duplicate, 1, 48)
        if hours is not None:
            dupes.append({**e, "emitted_at": shift(e["emitted_at"], hours=hours)})
            notes.append({"defect": DUPLICATE, "event_type": e["event_type"],
                          "logical_key": e["logical_key"], "event_id": e["event_id"],
                          "resent_at": dupes[-1]["emitted_at"]})
    return out + dupes, notes


@dataclass(frozen=True)
class MasterWrite:
    """One write to the source master, applied when the replay clock reaches `at`."""
    at: str
    kind: str  # "customer" (insert or address update) or "seller"
    row: dict


def master_writes(events: list[dict], sellers: dict[str, dict], held: set[str],
                  d: Defects) -> tuple[list[MasterWrite], list[dict]]:
    """Customer writes at each order's purchase (some delayed), and held-back seller inserts."""
    writes, notes, last = [], [], {}
    placed = sorted((e for e in events if e["event_type"] == "order_placed" and e["version"] == 1),
                    key=lambda e: e["event_time"])
    seen_sellers = {}
    for e in placed:
        p = e["payload"]
        person = p["customer_unique_id"]
        address = (p["customer_zip_code_prefix"], p["customer_city"], p["customer_state"])
        at, oid = p["order_purchase_timestamp"], p["order_id"]
        if person not in last:
            days = d.pick(EARLY_FACT, oid, d.early_fact, 1, 3)
            kind = EARLY_FACT
        elif address != last[person]:
            days = d.pick(LATE_DIM_CHANGE, oid, d.late_dim_change, 1, 3)
            kind = LATE_DIM_CHANGE
        else:
            days = None
            kind = None
        if days is not None:
            notes.append({"defect": kind, "order_id": oid, "customer_unique_id": person,
                          "delay_days": days})
            at = shift(at, days=days)
        if person not in last or address != last[person]:
            writes.append(MasterWrite(at, "customer", {**{k: p[k] for k in (
                "customer_unique_id", "customer_zip_code_prefix", "customer_city",
                "customer_state")}, "order_purchase_timestamp": p["order_purchase_timestamp"]}))
        last[person] = address
        for item in p["items"]:
            seen_sellers.setdefault(item["seller_id"], p["order_purchase_timestamp"])

    for seller_id in sorted(held):
        first = seen_sellers.get(seller_id)
        if first is None:
            continue  # never referenced in this replay: loaded at the start like the others
        days = d.scripted.get((HELD_SELLER, seller_id)) or (
            1 + int(d.draw(HELD_SELLER + ":param", seller_id) * 3))
        writes.append(MasterWrite(shift(first, days=days), "seller", sellers[seller_id]))
        notes.append({"defect": HELD_SELLER, "seller_id": seller_id, "first_order_at": first,
                      "delay_days": days})
    return sorted(writes, key=lambda w: (w.at, w.kind, w.row.get("customer_unique_id", ""),
                                         w.row.get("seller_id", ""))), notes


def held_back_sellers(sellers: list[dict], d: Defects) -> set[str]:
    return {s["seller_id"] for s in sellers
            if d.pick(HELD_SELLER, s["seller_id"], d.held_seller, 1, 3) is not None}


def is_late_file(d: Defects, day: str, hour: str) -> bool:
    return d.pick(LATE_FILE, f"{day}/{hour}", d.late_file, 1, 1) is not None
