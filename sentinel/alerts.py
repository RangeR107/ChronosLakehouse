"""Alerts and run notices (spec: DataSentinel, "Alerts"; chronos_daily step 13, notify).

Every notice is written to metrics.notifications, which the console's "Alerts and blocks" tab
reads. When CHRONOS_SLACK_WEBHOOK is set, it is also posted there. The webhook is a secret: it
comes from the environment (the owner's .env, which compose passes to Airflow), never from code.

Two ways in:
  - notify(ctx, date): the last task of a run that published. A success summary: the release,
    its warnings, whether it is complete, and any alert-severity check (a listed file still
    missing after 2 days).
  - task_failed(context): Airflow's on_failure_callback, after a task's last retry. A blocked
    audit arrives this way too, with the blocking checks in the exception.

Sending never fails the pipeline: a Slack outage is recorded on the notice, not raised. The data
is already safe either way; the notice only tells a person about it.
"""
import json
import logging
import os
import urllib.request
from datetime import date

import psycopg
from psycopg.types.json import Jsonb

from jobs.context import Context, release_id, run_id
from jobs.db import ensure_metrics_schema

log = logging.getLogger(__name__)

POST_TIMEOUT_SECONDS = 10


def console_url() -> str:
    return os.environ.get("CHRONOS_CONSOLE_URL", "http://localhost:8502")


def post_slack(text: str) -> str:
    """Post to the webhook if one is configured; return what happened, for the record."""
    url = os.environ.get("CHRONOS_SLACK_WEBHOOK")
    if not url:
        return "not_configured"
    request = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=POST_TIMEOUT_SECONDS) as response:
            return f"slack {response.status}"
    except (OSError, ValueError) as e:  # network, HTTP or a bad URL: recorded, never raised
        log.warning("Slack post failed: %s", e)
        return f"failed: {type(e).__name__}"


def record(conninfo: str, namespace: str, kind: str, business_date: date | None,
           run: str | None, summary: str, detail: dict) -> str:
    """Store the notice, post it, and store how the post went."""
    text = f"[Chronos {kind}] {summary}\nConsole: {console_url()}"
    delivered = post_slack(text)
    with psycopg.connect(conninfo) as conn:
        ensure_metrics_schema(conn)
        conn.execute("""INSERT INTO metrics.notifications (namespace, kind, business_date,
                            run_id, summary, detail, delivered)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                     (namespace, kind, business_date, run, summary, Jsonb(detail), delivered))
        conn.commit()
    return delivered


def run_summary(conn: psycopg.Connection, ctx: Context, business_date: date) -> dict:
    release = conn.execute("""SELECT release_id, complete, warnings FROM metrics.releases
                              WHERE namespace = %s AND business_date = %s""",
                           (ctx.namespace_prefix, business_date)).fetchone()
    alerts = [f"{t} {c}: {o}" for t, c, o in conn.execute(
        """SELECT table_name, check_name, observed FROM metrics.check_results
           WHERE namespace = %s AND run_id = %s AND severity = 'alert' AND status = 'fail'""",
        (ctx.namespace_prefix, run_id(business_date))).fetchall()]
    rid, complete, warnings = release or (release_id(business_date), None, [])
    return {"release_id": rid, "complete": complete, "warnings": warnings or [],
            "alerts": alerts}


def notify(ctx: Context, business_date: date) -> dict:
    """chronos_daily's last step: what was published, and anything a person should know."""
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_metrics_schema(conn)
        s = run_summary(conn, ctx, business_date)
    kind = "alert" if s["alerts"] else "published"
    summary = (f"{business_date}: published {s['release_id']}"
               f"{'' if s['complete'] else ' (incomplete)'}, {len(s['warnings'])} warnings"
               + (f"; ALERT: {'; '.join(s['alerts'])}" if s["alerts"] else ""))
    s["delivered"] = record(ctx.source_conninfo, ctx.namespace_prefix, kind, business_date,
                            run_id(business_date), summary, s)
    return s


def task_failed(context: dict) -> None:
    """Airflow on_failure_callback: name the run, the task and why it failed."""
    from jobs.run_day import default_context  # only Airflow calls this
    ti = context["task_instance"]
    error = context.get("exception")
    logical = context.get("logical_date")
    business_date = logical.date() if logical else None
    blocked = type(error).__name__ == "SentinelBlocked"
    summary = (f"{ti.dag_id}.{ti.task_id} {'blocked' if blocked else 'failed'} for "
               f"{business_date} (try {ti.try_number}): {error}")
    try:
        ctx = default_context()
        record(ctx.source_conninfo, ctx.namespace_prefix, "blocked" if blocked else "task_failed",
               business_date, run_id(business_date) if business_date else None, summary,
               {"dag_id": ti.dag_id, "task_id": ti.task_id, "run_id": context.get("run_id"),
                "error": str(error)})
    except Exception:  # a callback that raises would hide the original failure
        log.exception("could not record the failure notice: %s", summary)
