"""Which bronze rows a run processes (jobs/batches.py), with the holds given directly."""
from datetime import date, datetime

import pytest

from jobs import batches
from jobs.context import Context

DAY = date(2017, 1, 10)  # runs at 11 Jan 00:00


def t(day: int) -> datetime:
    return datetime(2017, 1, day)


@pytest.fixture
def rows(spark):
    return spark.createDataFrame([(t(d),) for d in range(5, 13)], "ingested_at timestamp_ntz")


def taken(spark, rows, monkeypatch, holds, after):
    monkeypatch.setattr(batches, "holds", lambda ctx: holds)
    ctx = Context(spark, None)
    return sorted(r[0].day for r in rows.where(batches.in_batch(ctx, DAY, after)).collect())


def test_newer_than_the_published_mark_up_to_run_time(spark, rows, monkeypatch):
    assert taken(spark, rows, monkeypatch, {}, t(8)) == [9, 10, 11]


def test_held_and_rejected_are_left_out_released_is_taken_in(spark, rows, monkeypatch):
    holds = {t(9): batches.HELD, t(10): batches.REJECTED, t(6): batches.RELEASED,
             t(5): batches.PUBLISHED}
    assert taken(spark, rows, monkeypatch, holds, t(8)) == [6, 11]
