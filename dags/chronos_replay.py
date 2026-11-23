"""chronos_replay: plays the source system, one business date at a time (simulation only).

For each date it writes the day's hourly files, completion marker and customer master rows
(generator/replay.py), then triggers chronos_daily for the same date and waits for it. The
source database only knows the past, like a real one, so the pipeline must process a date before
the next one is written (docs/decisions.md, "Two bugs the fixture truth test found").

This DAG is the producer, not the pipeline: chronos_daily never imports the generator, and
nothing here reads the ground-truth manifest. Run a slice with a backfill, for example:

    airflow backfill create --dag-id chronos_replay --from-date 2017-01-01 --to-date 2017-03-01 \
        --max-active-runs 1 --reprocess-behavior failed

Dates must run strictly in order. --max-active-runs 1 matters because a backfill applies its own
limit, not the DAG's; depends_on_past and wait_for_downstream enforce the order regardless.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

PURCHASED_FROM = "2017-01-01"  # the Phase 1 slice starts at the reliable window


# A daily schedule bounded to Olist's dates, so backfill can create runs, but the scheduler never
# makes a run for today's wall-clock date (an unbounded @daily did exactly that).
@dag(dag_id="chronos_replay", schedule="@daily", start_date=datetime(2016, 9, 4),
     end_date=datetime(2018, 9, 30), catchup=False,
     max_active_runs=1, default_args={"depends_on_past": True, "wait_for_downstream": True},
     tags=["chronos", "simulation"])
def chronos_replay():

    @task
    def write_feed(logical_date=None):
        import psycopg

        from generator.replay import replay
        from generator.source_db import apply_schema, conninfo
        day = logical_date.date()
        with psycopg.connect(conninfo()) as conn:
            apply_schema(conn)
            replay(Path(os.environ["CHRONOS_OLIST"]), Path(os.environ["CHRONOS_LANDING"]),
                   day, day, purchased_from=datetime.fromisoformat(PURCHASED_FROM).date(),
                   conn=conn)

    run_pipeline = TriggerDagRunOperator(
        task_id="run_chronos_daily", trigger_dag_id="chronos_daily",
        logical_date="{{ logical_date }}", wait_for_completion=True, poke_interval=15,
        reset_dag_run=True)

    write_feed() >> run_pipeline


chronos_replay()
