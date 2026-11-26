"""The DataSentinel console: what the audit decided, and what needs a person (spec: DataSentinel,
"Console in Streamlit").

    streamlit run sentinel/console/app.py

Like the dashboard, it reads PostgreSQL only (the metrics store and serving), never the lake, and
imports no pipeline code. Pick the database and namespace in the sidebar (CHRONOS_DB_NAME sets
the default): chronos is the Airflow lake, chronos_demo the local demo, and chronos_eval with
eval_ the evaluation replay.

Tabs: open problems (held batches, blocked releases, unfinished publishes), releases, check
history per table, the quarantine browser, alerts and blocks, and the restatement report.
"""
import os

import pandas as pd
import psycopg
import streamlit as st
from psycopg.conninfo import make_conninfo

st.set_page_config(page_title="DataSentinel", layout="wide")


def conninfo(dbname: str) -> str:
    # The same environment variables as jobs/db.py; the console doesn't import pipeline code.
    return make_conninfo(
        host=os.environ.get("CHRONOS_DB_HOST", "localhost"),
        port=os.environ.get("CHRONOS_DB_PORT", "5433"),
        user=os.environ.get("CHRONOS_DB_USER", "chronos"),
        password=os.environ.get("CHRONOS_DB_PASSWORD", "chronos"),
        dbname=dbname)


@st.cache_data(ttl=30)
def query(dbname: str, sql: str, params: tuple = (), empty: tuple = ()) -> pd.DataFrame:
    """Rows as a DataFrame. A table the pipeline hasn't created yet reads as empty (with the
    columns in `empty`): the console only reads, it never creates anything."""
    with psycopg.connect(conninfo(dbname)) as conn:
        try:
            cur = conn.execute(sql, params)
        except psycopg.errors.UndefinedTable:
            return pd.DataFrame(columns=list(empty))
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


st.sidebar.title("DataSentinel")
db = st.sidebar.text_input("Database", os.environ.get("CHRONOS_DB_NAME", "chronos"))
ns = st.sidebar.text_input("Namespace prefix", "eval_" if db == "chronos_eval" else "")
st.sidebar.caption("chronos is the Airflow lake, chronos_demo the demo (python -m jobs.demo), "
                   "chronos_eval with eval_ the evaluation.")

try:
    releases = query(db, """SELECT business_date, release_id, status, complete, warnings,
                                   published_at FROM metrics.releases WHERE namespace = %s
                            ORDER BY business_date DESC""", (ns,))
except psycopg.Error as e:
    st.error(f"No metrics store in {db}: {e}")
    st.stop()

served = query(db, "SELECT release_id, business_date FROM serving.load_state") \
    if not ns else pd.DataFrame()
held = query(db, """SELECT business_date, run_id, ingested_at, state, reasons
                    FROM metrics.batch_holds WHERE namespace = %s ORDER BY ingested_at""", (ns,),
             empty=("business_date", "run_id", "ingested_at", "state", "reasons"))

c1, c2, c3, c4 = st.columns(4)
c1.metric("Releases published", int((releases.status == "published").sum()))
c2.metric("Blocked runs", int((releases.status == "blocked").sum()))
c3.metric("Batches waiting for a decision", int((held.state == "held").sum()) if len(held) else 0)
c4.metric("Serving", served.release_id.iloc[0] if len(served) else "n/a")

tabs = st.tabs(["Open problems", "Releases", "Check history", "Quarantine", "Alerts and blocks",
                "Restatements"])

with tabs[0]:
    st.subheader("Held batches")
    st.caption("A blocked run's batches wait here until someone decides: "
               "`python -m jobs.batches release|reject <date>` (docs/runbook.md).")
    st.dataframe(held[held.state == "held"] if len(held) else held, width="stretch")
    stuck = releases[releases.status == "publishing"]
    if len(stuck):
        st.error("A publish died midway; later runs are stopped until it is finished: "
                 f"{', '.join(stuck.release_id)}. Rerun publish_release for that date.")
    incomplete = releases[(releases.status == "published") & ~releases.complete.astype(bool)]
    st.subheader("Published but incomplete")
    st.caption("A listed file was missing, or the data was stale: the dashboards should say so.")
    st.dataframe(incomplete, width="stretch")

with tabs[1]:
    st.dataframe(releases, width="stretch")

with tabs[2]:
    tables = query(db, "SELECT DISTINCT table_name FROM metrics.check_results "
                       "WHERE namespace = %s ORDER BY 1", (ns,))
    if len(tables):
        table = st.selectbox("Table", tables.table_name)
        checks = query(db, """SELECT business_date, check_name, method, severity, status,
                                     observed, expected FROM metrics.check_results
                              WHERE namespace = %s AND table_name = %s
                              ORDER BY business_date DESC, check_name""", (ns, table))
        check = st.selectbox("Check", sorted(checks.check_name.unique()))
        one = checks[checks.check_name == check].sort_values("business_date")
        st.line_chart(one.set_index("business_date")["observed"])
        st.dataframe(one.sort_values("business_date", ascending=False),
                     width="stretch")

with tabs[3]:
    q = query(db, """SELECT business_date, run_id, source_file, line_number, reasons, raw
                     FROM metrics.quarantine WHERE namespace = %s
                     ORDER BY business_date DESC, source_file, line_number LIMIT 2000""", (ns,),
              empty=("business_date", "run_id", "source_file", "line_number", "reasons", "raw"))
    if len(q):
        q["reason"] = q.reasons.map(lambda r: r[0].split(":")[0] if r else "")
        pick = st.multiselect("Reason", sorted(q.reason.unique()))
        st.dataframe(q[q.reason.isin(pick)] if pick else q, width="stretch")
    else:
        st.info("No quarantined lines recorded.")

with tabs[4]:
    st.subheader("Notices sent")
    st.caption("Every alert and run summary (sentinel/alerts.py). `delivered` says whether it "
               "reached Slack; without CHRONOS_SLACK_WEBHOOK they are recorded here only.")
    st.dataframe(query(db, """SELECT raised_at, kind, business_date, summary, delivered
                              FROM metrics.notifications WHERE namespace = %s
                              ORDER BY raised_at DESC LIMIT 500""", (ns,),
                       empty=("raised_at", "kind", "business_date", "summary", "delivered")),
                 width="stretch")
    st.subheader("Failed blocking and alert checks")
    st.dataframe(query(db, """SELECT business_date, severity, table_name, check_name, observed,
                                     expected FROM metrics.check_results
                              WHERE namespace = %s AND status = 'fail'
                                AND severity IN ('block', 'alert')
                              ORDER BY business_date DESC""", (ns,)), width="stretch")

with tabs[5]:
    if not ns:
        st.dataframe(query(db, "SELECT * FROM serving.restatements WHERE releases > 1 "
                               "ORDER BY abs(restatement) DESC NULLS LAST"),
                     width="stretch")
    else:
        st.info("The restatement report is served for an unprefixed namespace only.")
