"""Replay Olist as a daily event feed, optionally with reversible defects (generator/defects.py).

Usage (the Phase 1 truth slice):
    python -m generator.replay --olist data/raw/olist --landing lake/landing \
        --start 2017-01-01 --end 2017-03-01 --purchased-from 2017-01-01

For every business date from start to end inclusive it writes that date's hourly files and its
marker, including dates with no events (an empty marker says "nothing was sent", which a missing
marker can't). It also plays the source system: sellers and products are loaded at the start, and
each order's customer row is written in PostgreSQL at the order's time. --no-db skips that.
"""
import argparse
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import psycopg

from generator.anomalies import Anomalies, DayAnomaly
from generator.anomalies import apply as apply_anomaly
from generator.defects import (
    LATE_FILE,
    Defects,
    MasterWrite,
    held_back_sellers,
    inject_events,
    is_late_file,
    master_writes,
    schema_changes,
)
from generator.landing import (
    business_date,
    by_business_date,
    write_day,
    write_duplicate,
    write_late_files,
)
from generator.olist import build_events, load
from generator.source_db import (
    apply_schema,
    conninfo,
    load_products,
    load_sellers,
    record_order_address,
    set_clock,
)


def dates(start: date, end: date):
    d = start
    while d <= end:
        yield d.isoformat()
        d += timedelta(days=1)


def load_master(conn: psycopg.Connection, data: dict, start: date, held: set[str]) -> None:
    # Spec: sellers and products are loaded at the start, except the few held back to simulate
    # early-arriving facts. Loading again on a later run changes nothing (ON CONFLICT DO
    # NOTHING), so replaying in several pieces is safe.
    set_clock(conn, f"{start} 00:00:00")
    load_sellers(conn, [s for s in data["sellers"] if s["seller_id"] not in held])
    load_products(conn, data["products"])
    conn.commit()


def apply_writes(conn: psycopg.Connection, writes: list[MasterWrite]) -> None:
    """Apply one day's master writes in time order, each at its own replay-clock time."""
    for w in writes:
        set_clock(conn, w.at)
        if w.kind == "customer":
            record_order_address(conn, w.row)
        else:
            load_sellers(conn, [w.row])
    conn.commit()  # one day at a time: a crash mid-day leaves the previous days intact


@dataclass
class Feed:
    """Everything a replay emits, worked out once: events by day, late files, master writes."""
    days: dict[str, list[dict]]
    late_hours: dict[str, frozenset[str]]
    writes: dict[str, list[MasterWrite]]
    held: set[str]
    notes: list[dict]
    # Evaluation replay only: business date -> what its irreversible anomaly changes.
    anomalies: dict[str, DayAnomaly] = field(default_factory=dict)


def plan(data: dict, purchased_from: date | None, feed_end: date, defects: Defects,
         anomalies: Anomalies | None = None) -> Feed:
    cut = f"{purchased_from} 00:00:00" if purchased_from else None
    # Drain: nothing new is emitted after the last business date; only delayed arrivals land.
    natural = [e for e in build_events(data, cut) if business_date(e) <= feed_end.isoformat()]
    events, notes = inject_events(natural, defects)
    held = held_back_sellers(data["sellers"], defects)
    writes, write_notes = master_writes(natural, {s["seller_id"]: s for s in data["sellers"]},
                                        held, defects)
    # Only a seller that gets a delayed insert is held back; one no order in this replay names
    # is loaded at the start like the rest, or it would never reach the master at all.
    held = {w.row["seller_id"] for w in writes if w.kind == "seller"}
    events, schema_notes = schema_changes(
        events, defects, {p["product_id"]: p["product_category_name"] for p in data["products"]})
    notes += schema_notes
    days = by_business_date(events)
    late_hours = {}
    for day, day_events in days.items():
        hours = frozenset(h for h in {e["emitted_at"][11:13] for e in day_events}
                          if is_late_file(defects, day, h))
        if hours:
            late_hours[day] = hours
            notes.extend({"defect": LATE_FILE, "business_date": day, "hour": h} for h in hours)
    by_day = defaultdict(list)
    for w in writes:
        by_day[w.at[:10]].append(w)
    planned = {}
    if anomalies is not None:
        for day, kind in sorted(anomalies.schedule().items()):
            if day <= feed_end.isoformat():
                planned[day] = apply_anomaly(anomalies, kind, day, days.get(day, []))
                notes.append(planned[day].note)
    return Feed(days, late_hours, dict(by_day), held, notes + write_notes, planned)


def write_manifest(path: Path, feed: Feed, defects: Defects) -> None:
    """The ground-truth manifest: every defect injected. Tests read it; the pipeline never does."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {"seed": defects.seed, "defects": sorted(feed.notes, key=lambda n: json.dumps(
        n, sort_keys=True))}
    path.write_text(json.dumps(body, indent=1, sort_keys=True) + "\n")


def replay(olist_dir: Path, landing_dir: Path, start: date, end: date,
           purchased_from: date | None = None, conn: psycopg.Connection | None = None,
           after_day: Callable[[date], None] | None = None, defects: Defects | None = None,
           feed_end: date | None = None, manifest: Path | None = None,
           anomalies: Anomalies | None = None) -> list[dict]:
    """Write each business date's files and master rows, in date order.

    after_day, if given, is called once each date is fully written. The source database only
    holds its state "as of" the latest date written, like a real source, so a pipeline that
    extracts from it must run each date before the next one is written (the truth test passes
    run_day here). Writing everything first would let day 1 see day 60's addresses.

    defects: None is the Phase 1 feed (no defects). feed_end is the last business date with new
    events; dates after it up to `end` are the drain period. The feed is worked out in full
    every call, so replaying one date at a time gives the same files as one long replay.

    anomalies: the evaluation replay's irreversible anomalies (generator/anomalies.py). Never
    pass them to a truth replay: they are not reversed, so business tables would differ.
    """
    data = load(olist_dir)
    d = defects or Defects.only({})
    feed = plan(data, purchased_from, feed_end or end, d, anomalies)
    if manifest is not None:
        write_manifest(manifest, feed, d)
    if conn is not None:
        load_master(conn, data, start, feed.held)

    markers = []
    for day in dates(start, end):
        previous = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        if previous in feed.late_hours:
            # An anomaly day's late file must carry the anomaly's events too: its marker was
            # written from them, and a different file would fail the checksum.
            sent = feed.anomalies[previous].events if previous in feed.anomalies \
                else feed.days[previous]
            write_late_files(sent, landing_dir, previous, feed.late_hours[previous])
        events, late = feed.days.get(day, []), feed.late_hours.get(day, frozenset())
        odd = feed.anomalies.get(day)
        if odd is not None:
            events, late = odd.events, late | odd.never_hours
        markers.append(write_day(events, landing_dir, day, late))
        if odd is not None and odd.duplicate_hour is not None:
            write_duplicate(events, landing_dir, day, odd.duplicate_hour)
        if conn is not None:
            apply_writes(conn, feed.writes.get(day, []))
        if after_day is not None:
            after_day(date.fromisoformat(day))
    return markers


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--olist", type=Path, required=True)
    p.add_argument("--landing", type=Path, required=True)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--purchased-from", type=date.fromisoformat, default=None)
    p.add_argument("--no-db", action="store_true", help="write files only, not the source master")
    p.add_argument("--defects", action="store_true", help="inject the spec's reversible defects")
    p.add_argument("--feed-end", type=date.fromisoformat, default=None,
                   help="last business date with new events; later dates are the drain period")
    p.add_argument("--manifest", type=Path, default=None, help="where to write the ground truth")
    a = p.parse_args(argv)

    options = {"defects": Defects() if a.defects else None, "feed_end": a.feed_end,
               "manifest": a.manifest}
    if a.no_db:
        markers = replay(a.olist, a.landing, a.start, a.end, a.purchased_from, **options)
    else:
        with psycopg.connect(conninfo()) as conn:
            apply_schema(conn)
            markers = replay(a.olist, a.landing, a.start, a.end, a.purchased_from, conn, **options)
    rows = sum(f["rows"] for m in markers for f in m["files"])
    print(f"{len(markers)} business dates, {sum(len(m['files']) for m in markers)} files, {rows} events")


if __name__ == "__main__":
    main()
