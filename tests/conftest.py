"""Shared pytest fixtures.

The Spark session mirrors the local setup in the spec ("Getting started: week 1"): PySpark 3.5.6
with the Iceberg 1.10.0 runtime jar, the same versions Glue 5.1 bundles, so what passes here
behaves the same on Glue later.
"""
import pytest
from pyspark.sql import SparkSession

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
