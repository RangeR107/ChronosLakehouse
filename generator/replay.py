"""Replay Olist as a daily event feed (truth replay, Phase 1: no defects yet).

Usage (the Phase 1 truth slice):
    python -m generator.replay --olist data/raw/olist --landing lake/landing \
        --start 2017-01-01 --end 2017-03-01 --purchased-from 2017-01-01

For every business date from start to end inclusive it writes that date's hourly files and its
marker, including dates with no events (an empty marker says "nothing was sent", which a missing
marker can't). It also plays the source system: sellers and products are loaded at the start, and
each order's customer row is written in PostgreSQL at the order's time. --no-db skips that.
"""
import argparse
from datetime import date, timedelta
from pathlib import Path

import psycopg

from generator.landing import by_business_date, sort_key, write_day
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


def load_master(conn: psycopg.Connection, data: dict, start: date) -> None:
    # Spec: sellers and products are loaded at the start. Loading again on a later run changes
    # nothing (ON CONFLICT DO NOTHING), so replaying in several pieces is safe.
    set_clock(conn, f"{start} 00:00:00")
    load_sellers(conn, data["sellers"])
    load_products(conn, data["products"])
    conn.commit()


def record_day(conn: psycopg.Connection, events: list[dict]) -> None:
    """Write each order's customer row at the moment the order was placed."""
    for e in sorted(events, key=sort_key):
        if e["event_type"] == "order_placed":
            set_clock(conn, e["emitted_at"])
            record_order_address(conn, e["payload"])
    conn.commit()  # one day at a time: a crash mid-day leaves the previous days intact


def replay(olist_dir: Path, landing_dir: Path, start: date, end: date,
           purchased_from: date | None = None, conn: psycopg.Connection | None = None) -> list[dict]:
    data = load(olist_dir)
    cut = f"{purchased_from} 00:00:00" if purchased_from else None
    days = by_business_date(build_events(data, cut))
    if conn is not None:
        load_master(conn, data, start)

    markers = []
    for d in dates(start, end):
        day = days.get(d, [])
        markers.append(write_day(day, landing_dir, d))
        if conn is not None:
            record_day(conn, day)
    return markers


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--olist", type=Path, required=True)
    p.add_argument("--landing", type=Path, required=True)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--purchased-from", type=date.fromisoformat, default=None)
    p.add_argument("--no-db", action="store_true", help="write files only, not the source master")
    a = p.parse_args(argv)

    if a.no_db:
        markers = replay(a.olist, a.landing, a.start, a.end, a.purchased_from)
    else:
        with psycopg.connect(conninfo()) as conn:
            apply_schema(conn)
            markers = replay(a.olist, a.landing, a.start, a.end, a.purchased_from, conn)
    rows = sum(f["rows"] for m in markers for f in m["files"])
    print(f"{len(markers)} business dates, {sum(len(m['files']) for m in markers)} files, {rows} events")


if __name__ == "__main__":
    main()
