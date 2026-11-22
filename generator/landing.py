"""Write a business date's events as hourly JSON Lines files plus a completion marker.

Spec: "Arrival" and "Completion marker". Locally the landing folder stands in for the S3
landing/ prefix:

    landing/date=2017-01-05/events_2017-01-05T13.jsonl   one file per hour that has events
    landing/date=2017-01-05/_complete.json                written last, lists every file

The marker is the producer's delivery contract: what was sent, with row counts and checksums.
It never says what is wrong, so it is not the ground-truth manifest, and the pipeline may read it.
"""
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from generator.events import EVENT_TYPES, SCHEMA_VERSION

MARKER = "_complete.json"
TYPE_RANK = {t: i for i, t in enumerate(EVENT_TYPES)}


def business_date(event: dict) -> str:
    return event["emitted_at"][:10]


def by_business_date(events: list[dict]) -> dict[str, list[dict]]:
    days = defaultdict(list)
    for e in events:
        days[business_date(e)].append(e)
    return days


def sort_key(event: dict):
    # A total order, so the same events always produce byte-identical files.
    return (event["emitted_at"], TYPE_RANK[event["event_type"]], event["logical_key"],
            event["version"])


def to_line(event: dict) -> str:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"


def write_once(path: Path, content: bytes) -> None:
    """Landing files are never modified: rewriting is allowed only with identical bytes."""
    if path.exists():
        if path.read_bytes() != content:
            raise FileExistsError(f"{path} exists with different content; landing files are immutable")
        return
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    tmp.replace(path)  # a reader never sees a half-written file


def write_day(events: list[dict], landing_dir: Path, day: str) -> dict:
    """Write one business date's files, then its marker. Returns the marker."""
    folder = Path(landing_dir) / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)

    hours = defaultdict(list)
    for e in sorted(events, key=sort_key):
        if business_date(e) != day:
            raise ValueError(f"event {e['event_id']} belongs to {business_date(e)}, not {day}")
        hours[e["emitted_at"][11:13]].append(e)

    files = []
    for hour in sorted(hours):
        name = f"events_{day}T{hour}.jsonl"
        content = "".join(to_line(e) for e in hours[hour]).encode("utf-8")
        write_once(folder / name, content)
        files.append({"file": name, "rows": len(hours[hour]),
                      "sha256": hashlib.sha256(content).hexdigest()})

    # Written after every file it lists, so a reader that sees the marker sees complete files.
    marker = {"business_date": day, "schema_version": SCHEMA_VERSION, "files": files}
    write_once(folder / MARKER, (json.dumps(marker, indent=2) + "\n").encode("utf-8"))
    return marker
