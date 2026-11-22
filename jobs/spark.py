"""The one place a Spark session is configured, for the jobs and the tests alike.

PySpark 3.5.6 with the Iceberg 1.10.0 runtime jar: the versions AWS Glue 5.1 bundles, so code
that passes locally behaves the same on Glue. Locally the catalog is a Hadoop catalog over a
folder; on Glue it becomes the Glue Data Catalog (Phase 4).
"""
import os
import sys

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
CATALOG = "local"


def build_session(warehouse: str, app_name: str = "chronos", shuffle_partitions: int = 8,
                  master: str = "local[*]") -> SparkSession:
    # Python workers must run the same interpreter as the driver. Otherwise Spark starts
    # whatever `python3` is first on the PATH (Homebrew's 3.12 here, not the venv's 3.11) and
    # refuses to run Python code across minor versions. When Spark is started from Python, as
    # here, PySpark reads this environment variable, not the spark.pyspark.python setting.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        # The data is small (about 10,000 events a day at most); 200 default shuffle partitions
        # would mostly be empty tasks.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Olist timestamps carry no time zone and must never be converted (spec, Shared
        # definitions). Pinning the session zone to UTC makes Spark store and print them exactly
        # as written, whatever the laptop's or the Glue worker's zone is.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
