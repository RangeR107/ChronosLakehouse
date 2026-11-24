"""maintenance_weekly: compact small files, expire unneeded snapshots, remove orphan files
(spec: Orchestration, maintenance_weekly; jobs/maintenance.py).

Never commits to main while a run's branches are open, in two ways:
  - maintain() itself refuses while any table has a run branch or a release is 'publishing'.
  - Its task and chronos_daily's open_run share the one-slot chronos_lake pool, so no run can
    open its branches while maintenance is working.
A pool alone isn't enough: it holds one task at a time, not a whole run, so maintenance could
otherwise slip in between two tasks of a running date.

Business dates are replayed history, so "weekly" means every replayed Sunday: chronos_replay
triggers this DAG after each Sunday's chronos_daily run. Schedule None: never the wall clock.
"""
from datetime import datetime

from airflow.sdk import dag, task


def on_failure(context):
    from sentinel.alerts import task_failed
    task_failed(context)


@dag(dag_id="maintenance_weekly", schedule=None, start_date=datetime(2016, 9, 4), catchup=False,
     max_active_runs=1, default_args={"on_failure_callback": on_failure}, tags=["chronos"])
def maintenance_weekly():

    @task(pool="chronos_lake", retries=0)
    def maintain(logical_date=None) -> dict:
        from jobs.maintenance import maintain as step
        from jobs.run_day import default_context
        report = step(default_context(), logical_date.date())
        # The per-table report goes to the log; the return value (XCom) stays small.
        print(report["per_table"])
        return {"tables": report["tables"], **report["total"]}

    maintain()


maintenance_weekly()
