"""Shared pytest fixtures.

The Spark session comes from jobs/spark.py, the same builder the jobs use, so tests and jobs can't
drift apart in configuration.

PostgreSQL tests get a throwaway database per test, created and dropped here, so they never touch
the chronos database a developer is using.
"""
import os
import uuid

import psycopg
import pytest

from generator.source_db import apply_schema, conninfo
from jobs.spark import build_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # A fresh warehouse per test session, so a test never sees tables left by an earlier run.
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_session(str(warehouse), app_name="chronos-tests", shuffle_partitions=2,
                            master="local[2]")
    yield session
    session.stop()


@pytest.fixture
def source_db():
    """A fresh database with the source schema applied; yields an open connection to it."""
    try:
        admin = psycopg.connect(conninfo("postgres"), autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as e:
        # A skip must never pass for a result: where the database is required, fail loudly.
        if os.environ.get("CHRONOS_REQUIRE_POSTGRES") == "1":
            pytest.fail(f"PostgreSQL required but unreachable: {e}")
        pytest.skip("PostgreSQL not reachable (start it with: docker compose up -d chronos-db)")

    name = f"chronos_test_{uuid.uuid4().hex[:12]}"
    admin.execute(f'CREATE DATABASE "{name}"')
    conn = psycopg.connect(conninfo(name))
    try:
        apply_schema(conn)
        conn.commit()
        yield conn
    finally:
        conn.close()
        admin.execute(f'DROP DATABASE "{name}"')
        admin.close()
