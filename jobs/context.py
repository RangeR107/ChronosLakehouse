"""What every step function receives besides the business date.

Steps take (ctx, business_date) and keep no state of their own, so the Phase 2 Airflow DAG can
call the same functions with nothing added (docs/decisions.md, Phase 1 runner).
"""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from jobs.spark import CATALOG


@dataclass(frozen=True)
class Context:
    spark: SparkSession
    landing_dir: Path
    # Prefix for namespaces, so tests can give each test its own tables in one warehouse.
    namespace_prefix: str = ""
    # Where extract_dimensions reads the source master (jobs/db.py builds the default).
    source_conninfo: str = ""

    def table(self, layer: str, name: str) -> str:
        return f"{CATALOG}.{self.namespace_prefix}{layer}.{name}"

    def namespace(self, layer: str) -> str:
        return f"{CATALOG}.{self.namespace_prefix}{layer}"


def run_id(business_date: date) -> str:
    """Phase 1 names a run after its business date; a retry is the same run."""
    return f"run_{business_date.isoformat()}"


def run_time(business_date: date) -> datetime:
    """When the run for a business date happens on the replay clock: just after the day closes.

    Ingestion is stamped with this replay time, not the laptop's clock, so a retry produces
    identical rows and "as known at time T" means the same thing for every run.
    """
    return datetime.combine(business_date + timedelta(days=1), time())
