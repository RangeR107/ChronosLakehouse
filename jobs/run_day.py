"""Run business dates through the pipeline, one at a time, in the DAG's order (Phase 1 runner).

    python -m jobs.run_day --start 2017-01-01 --end 2017-03-01

run_day only calls the step functions in order and passes along what one returns to the next;
it has no logic of its own. The Phase 2 chronos_daily DAG wraps the same functions the same way
(docs/decisions.md). Dates run serially, like max_active_runs = 1, because each run builds on
the previous one.
"""
import argparse
import os
from datetime import date, timedelta
from pathlib import Path

from jobs.bronze_to_silver import bronze_to_silver
from jobs.build_dimensions import build_dimensions
from jobs.build_facts import build_facts
from jobs.context import Context
from jobs.db import conninfo
from jobs.extract_dimensions import extract_dimensions
from jobs.land_to_bronze import land_to_bronze, register_files, wait_for_landing
from jobs.load_serving import load_serving
from jobs.recompute_aggregates import recompute_aggregates
from jobs.spark import build_session


def default_context(landing: Path | None = None, warehouse: Path | None = None) -> Context:
    """The context the runner and the Airflow tasks use; paths can come from the environment."""
    landing = landing or Path(os.environ.get("CHRONOS_LANDING", "lake/landing"))
    warehouse = warehouse or Path(os.environ.get("CHRONOS_WAREHOUSE", "lake/warehouse"))
    reference = Path(os.environ.get("CHRONOS_OLIST", "data/raw/olist"))
    return Context(build_session(str(warehouse.resolve())), landing,
                   source_conninfo=conninfo(), reference_dir=reference)


def run_day(ctx: Context, business_date: date) -> dict:
    wait_for_landing(ctx, business_date)
    registered = register_files(ctx, business_date)
    extracted = extract_dimensions(ctx, business_date)
    landed = land_to_bronze(ctx, business_date, registered["new"])
    silver = bronze_to_silver(ctx, business_date)
    dims = build_dimensions(ctx, business_date)
    facts = build_facts(ctx, business_date)
    aggregates = recompute_aggregates(ctx, business_date)
    served = load_serving(ctx, business_date)
    return {"business_date": business_date.isoformat(), "files": len(registered["new"]),
            "events": landed, "missing": registered["missing"],
            "unlisted": registered["unlisted"], "extracted": extracted, "silver": silver,
            "dimensions": dims, "facts": facts, "aggregates": aggregates, "served": served}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--landing", type=Path, default=None)
    p.add_argument("--warehouse", type=Path, default=None)
    a = p.parse_args(argv)

    ctx = default_context(a.landing, a.warehouse)
    d = a.start
    while d <= a.end:
        print(run_day(ctx, d))
        d += timedelta(days=1)


if __name__ == "__main__":
    main()
