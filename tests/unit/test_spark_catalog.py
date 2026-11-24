"""The jobs' Iceberg catalog never answers from a stale cached table (jobs/spark.py).

On the evaluation's 12 Jun 2018, publish_release read a table's refs without the run branch
that open_run had created (docs/decisions.md). The likely cause is the Spark catalog's table
cache: a session that loaded a table earlier keeps that copy of its metadata, so a commit made
elsewhere (another session, another process) stays invisible to it until the entry expires.
"""
import logging
import uuid

from jobs.context import Context
from sentinel.release import refs, run_refs


def test_a_branch_made_by_another_session_is_visible_to_publish(spark, tmp_path, caplog):
    table = f"local.cache_{uuid.uuid4().hex[:8]}.probe"
    spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg "
              "TBLPROPERTIES ('format-version' = '2')")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    ctx = Context(spark, tmp_path)
    assert set(refs(ctx, table)) == {"main"}   # this session has now loaded the table

    other = spark.newSession()                 # its own catalog, as another job would have
    other.sql(f"ALTER TABLE {table} CREATE BRANCH `run_2018-06-12`")

    # The first read must already see it: no cached copy, so no refresh is needed.
    assert "run_2018-06-12" in refs(ctx, table)
    with caplog.at_level(logging.WARNING, logger="sentinel.release"):
        assert "run_2018-06-12" in run_refs(ctx, table, "run_2018-06-12")
    assert "refreshing" not in caplog.text
