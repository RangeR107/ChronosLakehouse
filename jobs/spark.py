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
    builder = SparkSession.builder.appName(app_name).master(master)
    # The Airflow image ships the jar (CHRONOS_SPARK_JARS); elsewhere Spark fetches the package
    # once into the local Ivy cache.
    jars = os.environ.get("CHRONOS_SPARK_JARS")
    builder = (builder.config("spark.jars", jars) if jars
               else builder.config("spark.jars.packages", ICEBERG_PACKAGE))
    return (
        builder
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        # No table cache: a cached table keeps its metadata, so a commit made by another session
        # or process (a branch, a fast-forward, maintenance) stays invisible to this one. That is
        # the likely cause of the publish that missed its run branch (docs/decisions.md). Every
        # read loads the table's current metadata instead; at our sizes that costs little.
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
        # The data is small (about 10,000 events a day at most); 200 default shuffle partitions
        # would mostly be empty tasks.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Olist timestamps carry no time zone and must never be converted (spec, Shared
        # definitions). Pinning the session zone to UTC makes Spark store and print them exactly
        # as written, whatever the laptop's or the Glue worker's zone is.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # PySpark draws a progress bar by default. In Airflow logs and in the demo's output it
        # buries the lines that matter (which date ran, what was published).
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
