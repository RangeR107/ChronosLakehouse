"""Shared pytest fixtures.

The Spark session mirrors the local setup in the spec ("Getting started: week 1"): PySpark 3.5.6
with the Iceberg 1.10.0 runtime jar, the same versions Glue 5.1 bundles, so what passes here
behaves the same on Glue later.

PostgreSQL tests get a throwaway database per test, created and dropped here, so they never touch
the chronos database a developer is using.
"""
import os
import uuid

import psycopg
import pytest
from pyspark.sql import SparkSession

from generator.source_db import apply_schema, conninfo

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # A fresh warehouse per test session, so a test never sees tables left by an earlier run.
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.appName("chronos-tests")
        .master("local[2]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(warehouse))
        # Tests use tiny data; the default 200 shuffle partitions only add start-up time.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
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
