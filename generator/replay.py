"""Replay Olist as a daily event feed (truth replay, Phase 1: no defects yet).

Usage:
    python -m generator.replay --olist data/raw/olist --landing lake/landing \
        --start 2016-09-04 --end 2016-11-02

Writes every business date from start to end inclusive, one marker per date, including dates
with no events: an empty marker says "nothing was sent", which a missing marker can't.
"""
import argparse
from datetime import date, timedelta
from pathlib import Path

from generator.landing import by_business_date, write_day
from generator.olist import build_events


def dates(start: date, end: date):
    d = start
    while d <= end:
        yield d.isoformat()
        d += timedelta(days=1)


def replay(olist_dir: Path, landing_dir: Path, start: date, end: date) -> list[dict]:
    days = by_business_date(build_events(olist_dir))
    return [write_day(days.get(d, []), landing_dir, d) for d in dates(start, end)]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--olist", type=Path, required=True)
    p.add_argument("--landing", type=Path, required=True)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    a = p.parse_args(argv)
    markers = replay(a.olist, a.landing, a.start, a.end)
    rows = sum(f["rows"] for m in markers for f in m["files"])
    print(f"{len(markers)} business dates, {sum(len(m['files']) for m in markers)} files, {rows} events")


if __name__ == "__main__":
    main()
