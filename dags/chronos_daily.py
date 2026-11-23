"""chronos_daily: one business date end to end (spec: Orchestration, chronos_daily).

A thin wrapper: every task calls the step function of the same name in jobs/ with the run's
logical date, and nothing else (docs/decisions.md, Phase 1 runner). The only value passed between
tasks is register_files' list of new files. Steps 4 and 10 to 13 (open_run, sentinel_audit,
publish_release, notify) arrive with releases in Phase 3; load_serving runs last until then.

Settings from the spec: max_active_runs = 1, because each date builds on the previous one
(scenario 9); infrastructure tasks retry twice with backoff (scenario 7 makes that safe).
The schedule is None: business dates are replayed history, run by backfill or by chronos_replay,
never by the wall clock.
"""
from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

# depends_on_past + wait_for_downstream: a date's tasks wait until the previous date's run has
# finished, however runs are created. max_active_runs alone isn't enough: an Airflow 3 backfill
# applies its own limit (--max-active-runs), not the DAG's.
DEFAULTS = {"retries": 2, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
            "depends_on_past": True, "wait_for_downstream": True}


def business_date(logical_date: datetime) -> date:
    return logical_date.date()


def context():
    # Imported inside the task, so parsing the DAG file never starts Java.
    from jobs.run_day import default_context
    return default_context()


@dag(dag_id="chronos_daily", schedule=None, start_date=datetime(2016, 9, 4), catchup=False,
     max_active_runs=1, default_args=DEFAULTS, tags=["chronos"])
def chronos_daily():

    @task(retries=12, retry_delay=timedelta(minutes=5), retry_exponential_backoff=False)
    def wait_for_landing(logical_date=None):
        # Retries play the sensor: the task fails while the day's completion marker is missing.
        from jobs.land_to_bronze import wait_for_landing as step
        step(context(), business_date(logical_date))

    @task
    def register_files(logical_date=None) -> dict:
        from jobs.land_to_bronze import register_files as step
        return step(context(), business_date(logical_date))

    @task
    def extract_dimensions(logical_date=None):
        from jobs.extract_dimensions import extract_dimensions as step
        return step(context(), business_date(logical_date))

    @task
    def land_to_bronze(registered: dict, logical_date=None):
        from jobs.land_to_bronze import land_to_bronze as step
        return step(context(), business_date(logical_date), registered["new"])

    def step_task(module: str, name: str):
        @task(task_id=name)
        def run(logical_date=None):
            import importlib
            step = getattr(importlib.import_module(f"jobs.{module}"), name)
            return step(context(), business_date(logical_date))
        return run

    registered = register_files()
    landed = land_to_bronze(registered)
    wait_for_landing() >> registered >> extract_dimensions() >> landed
    (landed
     >> step_task("bronze_to_silver", "bronze_to_silver")()
     >> step_task("build_dimensions", "build_dimensions")()
     >> step_task("build_facts", "build_facts")()
     >> step_task("recompute_aggregates", "recompute_aggregates")()
     >> step_task("load_serving", "load_serving")())


chronos_daily()
