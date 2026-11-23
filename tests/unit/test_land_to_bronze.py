"""Landing to bronze: what gets loaded, exactly once, and what the completeness check is told.

Spec: Architecture ("Bronze"), chronos_daily steps 1, 2 and 5, and late-data scenario 7 (retry).
Landing comes from the replay generator on the hand-built fixture (tests/fixtures/olist_mini).
"""
import hashlib
import json
import uuid
from datetime import date
from pathlib import Path

import pytest

from generator.replay import replay
from jobs.context import Context
from jobs.land_to_bronze import (
    ChecksumMismatch,
    LandingNotReady,
    land_to_bronze,
    register_files,
    wait_for_landing,
)

MINI = Path(__file__).parent.parent / "fixtures" / "olist_mini"
JAN5, JAN15 = date(2017, 1, 5), date(2017, 1, 15)


def run_day(ctx, day):
    """The bronze part of jobs.run_day, without the PostgreSQL extract these tests don't need."""
    wait_for_landing(ctx, day)
    registered = register_files(ctx, day)
    landed = land_to_bronze(ctx, day, registered["new"])
    return {"business_date": day.isoformat(), "files": len(registered["new"]), "events": landed,
            "missing": registered["missing"], "unlisted": registered["unlisted"]}


@pytest.fixture
def ctx(spark, tmp_path):
    landing = tmp_path / "landing"
    replay(MINI, landing, date(2017, 1, 1), JAN15, purchased_from=date(2017, 1, 1))
    # Each test gets its own namespaces, so tables never leak between tests.
    return Context(spark, landing, namespace_prefix=f"t{uuid.uuid4().hex[:8]}_")


def events(ctx, day=JAN5):
    return ctx.spark.sql(f"""
        SELECT source_file, line_number, raw, event_id, event_type,
               CAST(ingested_at AS STRING) AS ingested_at
        FROM {ctx.table('bronze', 'events')} WHERE business_date = DATE'{day}'
        ORDER BY source_file, line_number""").collect()


def manifest(ctx):
    return ctx.spark.sql(f"""SELECT file_path, etag, size_bytes, row_count, run_id,
                                    CAST(loaded_at AS STRING) AS loaded_at
                             FROM {ctx.table('bronze', 'file_manifest')} ORDER BY file_path""").collect()


def test_lines_land_exactly_as_received(ctx):
    run_day(ctx, JAN5)
    rows = events(ctx)
    # 5 Jan has three events: two in the 10:00 file, one in the 23:00 file.
    assert [(r.source_file, r.line_number) for r in rows] == [
        ("date=2017-01-05/events_2017-01-05T10.jsonl", 1),
        ("date=2017-01-05/events_2017-01-05T10.jsonl", 2),
        ("date=2017-01-05/events_2017-01-05T23.jsonl", 1)]
    file_lines = (ctx.landing_dir / rows[0].source_file).read_text().splitlines()
    assert [r.raw for r in rows[:2]] == file_lines
    assert rows[0].event_id == json.loads(file_lines[0])["event_id"]
    assert [r.event_type for r in rows] == ["order_placed", "payment_approved", "order_placed"]


def test_ingested_at_is_replay_time_not_wall_clock(ctx):
    run_day(ctx, JAN5)
    assert {r.ingested_at for r in events(ctx)} == {"2017-01-06 00:00:00"}


def test_manifest_records_every_file_with_its_checksum(ctx):
    run_day(ctx, JAN5)
    marker = json.loads((ctx.landing_dir / "date=2017-01-05" / "_complete.json").read_text())
    got = [(m.file_path, m.etag, m.row_count, m.run_id, m.loaded_at) for m in manifest(ctx)]
    assert got == [(f"date=2017-01-05/{f['file']}", f["sha256"], f["rows"], "run_2017-01-05",
                    "2017-01-06 00:00:00") for f in marker["files"]]
    for m in manifest(ctx):
        assert m.size_bytes == (ctx.landing_dir / m.file_path).stat().st_size


def test_retry_loads_nothing_twice(ctx):
    run_day(ctx, JAN5)
    before = (events(ctx), manifest(ctx))
    again = run_day(ctx, JAN5)
    assert again["files"] == 0 and again["events"] == 0
    assert (events(ctx), manifest(ctx)) == before


def test_crash_between_the_two_commits_recovers_without_duplicates(ctx):
    files = register_files(ctx, JAN5)["new"]
    land_to_bronze(ctx, JAN5, files)
    # Simulate dying after the events commit but before the manifest commit.
    ctx.spark.sql(f"DELETE FROM {ctx.table('bronze', 'file_manifest')}")
    assert len(register_files(ctx, JAN5)["new"]) == 2  # the manifest says: not loaded yet
    run_day(ctx, JAN5)
    assert len(events(ctx)) == 3  # lines already in bronze were not appended again
    assert len(manifest(ctx)) == 2


def test_unlisted_files_are_reported_and_never_loaded(ctx):
    stray = ctx.landing_dir / "date=2017-01-05" / "events_2017-01-05T11.jsonl"
    stray.write_text('{"event_id": "not-from-the-producer"}\n')
    result = run_day(ctx, JAN5)
    assert result["unlisted"] == ["date=2017-01-05/events_2017-01-05T11.jsonl"]
    assert all(r.source_file != "date=2017-01-05/events_2017-01-05T11.jsonl" for r in events(ctx))


def test_missing_files_are_reported_and_the_rest_load(ctx):
    (ctx.landing_dir / "date=2017-01-05" / "events_2017-01-05T23.jsonl").unlink()
    result = run_day(ctx, JAN5)
    assert result["missing"] == ["date=2017-01-05/events_2017-01-05T23.jsonl"]
    assert len(events(ctx)) == 2


def test_a_late_file_is_loaded_by_the_next_run(ctx):
    # The producer lists the 23:00 file in 5 Jan's marker but delivers it after that day's run.
    late = ctx.landing_dir / "date=2017-01-05" / "events_2017-01-05T23.jsonl"
    content = late.read_bytes()
    late.unlink()
    assert run_day(ctx, JAN5)["missing"] == ["date=2017-01-05/events_2017-01-05T23.jsonl"]
    late.write_bytes(content)
    result = run_day(ctx, date(2017, 1, 6))
    assert "date=2017-01-05/events_2017-01-05T23.jsonl" not in result["missing"]
    rows = [r for r in events(ctx) if r.source_file.endswith("T23.jsonl")
            and r.source_file.startswith("date=2017-01-05")]
    # events() reads 5 Jan's partition: the file keeps its own business date, and its arrival
    # time is the replay time of the run that received it.
    assert rows and {r.ingested_at for r in rows} == {"2017-01-07 00:00:00"}


def test_a_file_changed_after_the_marker_is_refused(ctx):
    path = ctx.landing_dir / "date=2017-01-05" / "events_2017-01-05T23.jsonl"
    path.write_text(path.read_text().replace("person_b", "someone_else"))
    assert hashlib.sha256(path.read_bytes()).hexdigest()  # file still exists, content differs
    with pytest.raises(ChecksumMismatch):
        run_day(ctx, JAN5)
    assert events(ctx) == []  # nothing from that day was appended


def test_no_marker_means_not_ready(ctx):
    with pytest.raises(LandingNotReady):
        run_day(ctx, date(2017, 1, 16))  # replayed only up to 15 Jan


def test_a_day_with_no_events_loads_nothing(ctx):
    # 3 Jan: no events that day, nor in the ten days its run looks back over.
    assert run_day(ctx, date(2017, 1, 3)) == {"business_date": "2017-01-03", "files": 0,
                                              "events": 0, "missing": [], "unlisted": []}


def test_every_event_lands_once_over_the_whole_fixture(ctx):
    d = date(2017, 1, 1)
    while d <= JAN15:
        run_day(ctx, d)
        run_day(ctx, d)  # every day retried once
        d = date.fromordinal(d.toordinal() + 1)
    total = ctx.spark.sql(f"""SELECT count(*), count(DISTINCT event_id)
                              FROM {ctx.table('bronze', 'events')}""").collect()[0]
    assert tuple(total) == (24, 24)  # all events of the 2017 orders in the fixture
