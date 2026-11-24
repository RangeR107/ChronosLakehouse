"""What every step function receives besides the business date.

Steps take (ctx, business_date) and keep no state of their own, so the Phase 2 Airflow DAG can
call the same functions with nothing added (docs/decisions.md, Phase 1 runner).
"""
import functools
from contextlib import contextmanager
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
    # The chronos database: extract_dimensions reads its `source` schema, load_serving writes
    # `serving` (jobs/db.py builds the default).
    source_conninfo: str = ""
    # Static reference files delivered with the source (Olist's category translation).
    reference_dir: Path = Path("data/raw/olist")

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


def release_id(business_date: date) -> str:
    return f"release_{business_date.isoformat()}"


def run_branch(business_date: date) -> str:
    """The Iceberg branch a run writes silver and gold to (spec: Publishing, run lifecycle)."""
    return run_id(business_date)


@contextmanager
def on_branch(ctx: Context, branch: str | None):
    """Read and write silver and gold on `branch` inside the block, then restore the setting.

    Only tables with write.wap.enabled follow spark.wap.branch; bronze never has it, so bronze is
    always read and written on main (open_run enables it on silver and gold only).
    """
    key = "spark.wap.branch"
    before = ctx.spark.conf.get(key, None)
    if branch is None:
        ctx.spark.conf.unset(key)
    else:
        ctx.spark.conf.set(key, branch)
    try:
        yield
    finally:
        if before is None:
            ctx.spark.conf.unset(key)
        else:
            ctx.spark.conf.set(key, before)


def on_run_branch(step):
    """For steps between open_run and the audit: they work on the run's branch, never on main."""
    @functools.wraps(step)
    def wrapped(ctx: Context, business_date: date, *args, **kwargs):
        with on_branch(ctx, run_branch(business_date)):
            return step(ctx, business_date, *args, **kwargs)
    return wrapped
