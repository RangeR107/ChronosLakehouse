"""A local demo: replay the first weeks of 2017 through the whole pipeline, without Airflow.

    python -m jobs.demo --fresh            # 30 business dates, about 10 minutes
    python -m jobs.demo --days 60 --fresh  # the truth test's slice, with the schema change

It plays both sides, the way the chronos_replay DAG does: the producer (generator/) writes a
business date's files and source rows, then the pipeline runs that date, then the next date is
written. The source database only knows the past, like a real one.

Everything it writes is its own: the chronos_demo database (source, serving, metrics) and
lake/demo/. It never touches the Airflow lake or the evaluation. The feed carries the spec's
reversible defects (late, duplicate, out-of-order and corrected events, late files). Each
date ends as chronos_daily ends, with its notice recorded for the console (and posted to Slack
only if CHRONOS_SLACK_WEBHOOK is set).

On one date the producer also resends a file under a name its marker doesn't list, which is
pure arrival behaviour, no data is changed. DataSentinel blocks that date, nothing is published,
and its lines are held. The demo then does what the runbook says a person does after checking
the file is a stray resend: it releases the held lines, and the next date's release includes
them. Any other block (possible after the 28-day warm-up, with --days above 30) is released
too, and the demo says it went unchecked. Pass --ask to make each decision yourself: on a
block the demo pauses, you look at it in the console, and answer release, reject or keep held.
"""
import argparse
import shutil
from datetime import date, timedelta
from pathlib import Path

import psycopg

from generator.anomalies import Anomalies
from generator.defects import Defects
from generator.replay import replay
from generator.source_db import apply_schema
from jobs.batches import REJECTED, RELEASED, decide
from jobs.context import Context, run_id
from jobs.db import conninfo
from jobs.maintenance import maintain
from jobs.run_day import run_day
from jobs.spark import build_session
from sentinel.alerts import notify, record
from sentinel.gate import SentinelBlocked

DB = "chronos_demo"
LAKE = Path("lake/demo")
OLIST = Path("data/raw/olist")
START = date(2017, 1, 1)  # the start of the reliable window (docs/metrics.yml)
STRAY_FILE_DAY = 10       # the date whose file is resent under an unlisted name
ANSWERS = {"r": RELEASED, "j": REJECTED, "k": None}


def reset() -> None:
    """Drop what an earlier demo wrote: its database and its lake folder, nothing else."""
    with psycopg.connect(conninfo("postgres"), autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{DB}" WITH (FORCE)')
    shutil.rmtree(LAKE, ignore_errors=True)


def ensure_database() -> None:
    with psycopg.connect(conninfo("postgres"), autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB,)).fetchone():
            admin.execute(f'CREATE DATABASE "{DB}"')


def ask(d: date) -> str | None:
    """The runbook's decision (docs/runbook.md), made by whoever runs the demo."""
    print(f"            Look at {d} in the console (make console: Open problems, Alerts).")
    print("            Release if the data is fine, reject only if it is wrong.")
    while True:
        try:
            answer = input("            Release, reject, or keep it held? [r/j/k] ")
        except EOFError:  # no one to ask (input piped and used up): leave the decision open
            return None
        if answer.strip().lower() in ANSWERS:
            return ANSWERS[answer.strip().lower()]


def summary(conn: psycopg.Connection) -> str:
    rows = conn.execute("SELECT status, count(*) FROM metrics.releases GROUP BY 1 "
                        "ORDER BY 1").fetchall()
    served = conn.execute("SELECT release_id FROM serving.load_state").fetchone()
    held = conn.execute("SELECT state, count(*) FROM metrics.batch_holds GROUP BY 1").fetchall()
    return (f"releases: {dict(rows)}; batches a block held: {dict(held) or 'none'}; "
            f"dashboards now serve {served[0] if served else 'nothing yet'}")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--days", type=int, default=30, help="business dates to replay (default 30)")
    p.add_argument("--fresh", action="store_true", help="drop an earlier demo first")
    p.add_argument("--ask", action="store_true",
                   help="on a block, pause and ask whether to release, reject or keep it held")
    a = p.parse_args(argv)
    if not (OLIST / "olist_orders_dataset.csv").exists():
        raise SystemExit(f"The Olist CSVs are missing from {OLIST}/ (see README, 'Get the data').")
    if a.fresh:
        reset()
    ensure_database()
    end = START + timedelta(days=a.days - 1)
    stray = START + timedelta(days=STRAY_FILE_DAY - 1)

    spark = build_session(str((LAKE / "warehouse").resolve()), app_name="chronos-demo")
    with psycopg.connect(conninfo(DB)) as conn:
        apply_schema(conn)
        conn.commit()
        ctx = Context(spark, LAKE / "landing", source_conninfo=conninfo(DB),
                      reference_dir=OLIST)

        def after_day(d: date) -> None:
            try:
                out = run_day(ctx, d)
            except SentinelBlocked as e:
                # What Airflow's failure callback records for a blocked audit (task_failed).
                record(ctx.source_conninfo, ctx.namespace_prefix, "blocked", d, run_id(d),
                       f"run_day blocked for {d}: {e}", {"error": str(e)})
                print(f"{d}  BLOCKED  {e}")
                print("            Nothing was published; the date's lines are held.")
                if a.ask:
                    state, why = ask(d), "your decision"
                else:
                    # Only the scripted stray file is known to be harmless. A longer demo can
                    # block for other reasons after the warm-up; say plainly that nobody looked.
                    state = RELEASED
                    why = ("a stray resend, checked" if d == stray
                           else "unchecked: the demo releases every block")
                if state is None:
                    print("            Kept held: later dates publish without them.")
                    return
                decide(ctx, d, state)
                print(f"            Released ({why}): the next date takes them in."
                      if state == RELEASED else
                      "            Rejected: they will never be published.")
                return
            if out.get("already_published"):
                print(f"{d}  already published, nothing republished")
            else:
                r = out["release"]
                print(f"{d}  published {r['release_id']}  events {out['events']:>5}  "
                      f"warnings {len(r['warnings'])}"
                      f"{'' if r['complete'] else '  (marked incomplete)'}")
                notify(ctx, d)  # chronos_daily's last step: the run summary
            if d.weekday() == 6:  # maintenance_weekly runs after every replayed Sunday
                maintain(ctx, d)

        print(f"Replaying {START} to {end} into {DB} and {LAKE}/ ...")
        replay(OLIST, LAKE / "landing", START, end, purchased_from=START, conn=conn,
               after_day=after_day, defects=Defects(), feed_end=end,
               anomalies=Anomalies(START, end, scripted={stray.isoformat(): "duplicate_file"}))
        print("\n" + summary(conn))
    print(f"""
Look at the results:
  CHRONOS_DB_NAME={DB} .venv/bin/streamlit run dashboard/app.py
  CHRONOS_DB_NAME={DB} .venv/bin/streamlit run sentinel/console/app.py --server.port 8502
""")


if __name__ == "__main__":
    main()
