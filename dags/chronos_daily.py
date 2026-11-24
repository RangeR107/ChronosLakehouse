"""chronos_daily: one business date end to end (spec: Orchestration, chronos_daily).

A thin wrapper: every task calls the step function of the same name in jobs/ with the run's
logical date, and nothing else (docs/decisions.md, Phase 1 runner). The only value passed between
tasks is register_files' list of new files.

Alerts (sentinel/alerts.py): every task's last failed try calls task_failed, which names the
run, the task and the error (a blocked audit's error lists the blocking checks); notify, the last
task, sends the success summary. Both are recorded in metrics.notifications and posted to Slack
when CHRONOS_SLACK_WEBHOOK is set.

Settings from the spec: max_active_runs = 1, because each date builds on the previous one
(scenario 9); infrastructure tasks retry twice with backoff (scenario 7 makes that safe). A
backfill applies its own limit, so it must be run with --max-active-runs 1 (docs/runbook.md).

Why there is no depends_on_past: a date that fails or is blocked must not stop the next one. The
next date's run starts from the last published release and picks the failed date's data up
(spec: Publishing). The one failure that must stop later dates, a publish that died between
fast-forwards, is guarded by open_run itself: it refuses to start while any release is still
'publishing' (docs/decisions.md).

The schedule is None: business dates are replayed history, run by backfill or by chronos_replay,
never by the wall clock.
"""
from datetime import date, datetime, timedelta

from airflow.sdk import dag, task


def on_failure(context):
    # Imported inside the callback, so parsing the DAG file stays light.
    from sentinel.alerts import task_failed
    task_failed(context)


DEFAULTS = {"retries": 2, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
            "on_failure_callback": on_failure}


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

    # Shares the one-slot chronos_lake pool with maintenance_weekly (see maintenance_weekly.py).
    @task.short_circuit(pool="chronos_lake")
    def open_run(logical_date=None) -> bool:
        # False skips the rest: a date already published republishes nothing, and the run ends
        # successfully (docs/runbook.md).
        from sentinel.release import open_run as step
        return not step(context(), business_date(logical_date))["already_published"]

    def step_task(module: str, name: str, **options):
        @task(task_id=name, **options)
        def run(logical_date=None):
            import importlib
            step = getattr(importlib.import_module(module), name)
            return step(context(), business_date(logical_date))
        return run

    registered = register_files()
    landed = land_to_bronze(registered)
    wait_for_landing() >> registered >> extract_dimensions() >> open_run() >> landed
    (landed
     >> step_task("jobs.bronze_to_silver", "bronze_to_silver")()
     >> step_task("jobs.build_dimensions", "build_dimensions")()
     >> step_task("jobs.build_facts", "build_facts")()
     >> step_task("jobs.recompute_aggregates", "recompute_aggregates")()
     # A failed audit needs a person to look at it: never retried automatically.
     >> step_task("sentinel.gate", "sentinel_audit", retries=0)()
     # Can retry: it skips tables already advanced.
     >> step_task("sentinel.release", "publish_release")()
     >> step_task("jobs.load_serving", "load_serving")()
     >> step_task("sentinel.release", "clean_up")()
     >> step_task("sentinel.alerts", "notify")())


chronos_daily()
