"""Irreversible anomalies, for the evaluation replay only (spec: Event model, "Irreversible
anomalies"; Testing and evaluation, "DataSentinel evaluation").

They exist to be caught. Each is placed on its own test-period day, never reversed, and recorded
in the ground-truth manifest with the event ids or files it touched, so the evaluation can tell
whether any of it reached a published release. They never run in the truth replay, and only
tests read the manifest (CLAUDE.md).

The six types, and what should catch each:
  missing_files   hours of files listed in the marker, never delivered    completeness
  duplicate_file  a file sent again under a name the marker doesn't list  completeness (block)
  volume_spike    the day's orders copied under new order ids             volume
  mix_shift       payment type or price level pushed off its baseline     drift
  schema_break    a required field renamed, same schema_version           schema contract
  null_burst      a field emptied on a share of one event type's events   contract or defect rate

Placement and parameters come from SHA-256 draws of the seed, like the reversible defects, so
the same seed gives the same evaluation.
"""
import copy
import hashlib
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from generator.events import make_event

TYPES = ("missing_files", "duplicate_file", "volume_spike", "mix_shift", "schema_break",
         "null_burst")

# Fields a null burst empties, by event type. Most are required, so the contract should
# quarantine them; delivered's date is optional, and its gap is a known source defect.
NULL_TARGETS = (("order_placed", "customer_city"), ("order_placed", "customer_state"),
                ("review_submitted", "review_score"),
                ("delivered", "order_delivered_customer_date"))
RENAMES = (("order_placed", "order_purchase_timestamp", "purchase_timestamp"),
           ("order_placed", "customer_unique_id", "customer_uid"),
           ("payment_approved", "order_approved_at", "approved_at"),
           ("review_submitted", "review_score", "score"))


@dataclass(frozen=True)
class Anomalies:
    """About per_type anomalies of each type on distinct days in [start, end]."""
    start: date
    end: date
    per_type: int = 30
    seed: int = 13
    # Tests can place anomalies exactly: {"YYYY-MM-DD": type}. Replaces the drawn schedule.
    scripted: dict = field(default_factory=dict)

    def draw(self, *parts) -> float:
        text = "\x1f".join(str(p) for p in (self.seed, *parts))
        return int(hashlib.sha256(text.encode()).hexdigest()[:13], 16) / 16 ** 13

    def schedule(self) -> dict[str, str]:
        """day -> anomaly type. Days are shuffled by draw; types take turns, so each type spreads
        over the whole period (weekdays, weekends and the sale days alike)."""
        if self.scripted:
            return dict(self.scripted)
        days = [(self.start + timedelta(days=n)).isoformat()
                for n in range((self.end - self.start).days + 1)]
        picked = sorted(days, key=lambda d: self.draw("day", d))[:self.per_type * len(TYPES)]
        return {d: TYPES[i % len(TYPES)] for i, d in enumerate(sorted(picked))}


@dataclass
class DayAnomaly:
    """What the replay must do differently on an anomaly day."""
    events: list[dict]                      # the day's events, possibly changed or added to
    never_hours: frozenset = frozenset()   # listed in the marker, never delivered
    duplicate_hour: str | None = None      # copied under an unlisted name
    note: dict = field(default_factory=dict)


def scale(text: str | None, factor: int) -> str | None:
    return None if text is None else str((Decimal(text) * factor).quantize(Decimal("0.01")))


def missing_files(a: Anomalies, day: str, events: list[dict]) -> DayAnomaly:
    hours = sorted({e["emitted_at"][11:13] for e in events})
    if not hours:
        return DayAnomaly(events, note={"skipped": "no files that day"})
    span = 1 + int(a.draw("span", day) * 12)              # an hour up to half a day
    first = int(a.draw("first", day) * len(hours))
    gone = frozenset(hours[first:first + span])
    lost = [e["event_id"] for e in events if e["emitted_at"][11:13] in gone]
    return DayAnomaly(events, never_hours=gone,
                      note={"hours": sorted(gone), "event_ids": lost})


def duplicate_file(a: Anomalies, day: str, events: list[dict]) -> DayAnomaly:
    hours = sorted({e["emitted_at"][11:13] for e in events})
    if not hours:
        return DayAnomaly(events, note={"skipped": "no files that day"})
    hour = hours[int(a.draw("dup", day) * len(hours))]
    return DayAnomaly(events, duplicate_hour=hour, note={"hour": hour})


def volume_spike(a: Anomalies, day: str, events: list[dict]) -> DayAnomaly:
    placed = [e for e in events if e["event_type"] == "order_placed"]
    copies = 2 + int(a.draw("copies", day) * 3)           # 2 to 4 extra copies of each order
    extra = []
    for n in range(1, copies + 1):
        for e in placed:
            payload = copy.deepcopy(e["payload"])
            payload["order_id"] = f"spike{n}-{payload['order_id']}"
            extra.append(make_event("order_placed", [payload["order_id"]], e["event_time"],
                                    e["emitted_at"], payload))
    return DayAnomaly(events + extra, note={"copies": copies,
                                            "event_ids": [e["event_id"] for e in extra]})


def mix_shift(a: Anomalies, day: str, events: list[dict]) -> DayAnomaly:
    out, touched = [], []
    by_price = a.draw("which", day) < 0.5
    factor = 3 + int(a.draw("factor", day) * 3)           # prices x3 to x5
    for e in events:
        if e["event_type"] != "order_placed":
            out.append(e)
            continue
        e = copy.deepcopy(e)
        p = e["payload"]
        if by_price:
            # Prices, freight and payments scale together, so only the price level moves (a
            # payment mismatch would be a different, already-known defect).
            for i in p["items"]:
                i["price"], i["freight_value"] = scale(i["price"], factor), scale(
                    i["freight_value"], factor)
            for pay in p["payments"]:
                pay["payment_value"] = scale(pay["payment_value"], factor)
        else:
            for pay in p["payments"]:
                if a.draw("voucher", e["event_id"], pay["payment_sequential"]) < 0.8:
                    pay["payment_type"] = "voucher"
        out.append(e)
        touched.append(e["event_id"])
    kind = f"price x{factor}" if by_price else "payment type to voucher (80%)"
    return DayAnomaly(out, note={"shift": kind, "event_ids": touched})


def schema_break(a: Anomalies, day: str, events: list[dict]) -> DayAnomaly:
    event_type, old, new = RENAMES[int(a.draw("rename", day) * len(RENAMES))]
    out, touched = [], []
    for e in events:
        if e["event_type"] == event_type and old in e["payload"]:
            e = copy.deepcopy(e)
            e["payload"][new] = e["payload"].pop(old)
            touched.append(e["event_id"])
        out.append(e)
    return DayAnomaly(out, note={"renamed": f"{event_type}.{old} -> {new}",
                                 "event_ids": touched})


def null_burst(a: Anomalies, day: str, events: list[dict]) -> DayAnomaly:
    event_type, field_name = NULL_TARGETS[int(a.draw("target", day) * len(NULL_TARGETS))]
    share = 0.3 + a.draw("share", day) * 0.3              # 30% to 60% of that type's events
    out, touched = [], []
    for e in events:
        if e["event_type"] == event_type and a.draw("null", e["event_id"]) < share:
            e = copy.deepcopy(e)
            e["payload"][field_name] = None
            touched.append(e["event_id"])
        out.append(e)
    return DayAnomaly(out, note={"field": f"{event_type}.{field_name}",
                                 "share": round(share, 3), "event_ids": touched})


APPLY = {"missing_files": missing_files, "duplicate_file": duplicate_file,
         "volume_spike": volume_spike, "mix_shift": mix_shift, "schema_break": schema_break,
         "null_burst": null_burst}


def apply(a: Anomalies, kind: str, day: str, events: list[dict]) -> DayAnomaly:
    result = APPLY[kind](a, day, events)
    result.note = {"anomaly": kind, "business_date": day, **result.note}
    return result
