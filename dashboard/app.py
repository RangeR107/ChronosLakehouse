"""The dashboard: questions 1 and 2, and the restatement report (question 8), read from
PostgreSQL `serving` only.

    streamlit run dashboard/app.py

It never reads the lake: what it shows is exactly what load_serving last committed, in one
transaction (docs/decisions.md, "The Phase 1 dashboard reads PostgreSQL serving"). Every number
follows docs/metrics.yml: orders purchased in the reliable window, delivered with a date, late
when the delivery day is after the estimated day.
"""
import os

import pandas as pd
import psycopg
import streamlit as st
from psycopg.conninfo import make_conninfo


def conninfo() -> str:
    # The same environment variables as jobs/db.py; the dashboard doesn't import pipeline code.
    return make_conninfo(
        host=os.environ.get("CHRONOS_DB_HOST", "localhost"),
        port=os.environ.get("CHRONOS_DB_PORT", "5433"),
        user=os.environ.get("CHRONOS_DB_USER", "chronos"),
        password=os.environ.get("CHRONOS_DB_PASSWORD", "chronos"),
        dbname=os.environ.get("CHRONOS_DB_NAME", "chronos"))


@st.cache_data(ttl=60)
def load():
    with psycopg.connect(conninfo()) as conn:
        state = conn.execute("SELECT run_id, business_date FROM serving.load_state").fetchone()
        cur = conn.execute("SELECT * FROM serving.delivery_by_month_region "
                           "ORDER BY delivery_month, region")
        cols = [c.name for c in cur.description]
        delivery = pd.DataFrame(cur.fetchall(), columns=cols)
        cur = conn.execute("SELECT * FROM serving.restatements "
                           "WHERE releases > 1 ORDER BY abs(restatement) DESC NULLS LAST")
        restated = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
        return state, delivery, restated


st.set_page_config(page_title="Chronos: deliveries", layout="wide")
st.title("Where are deliveries late?")

try:
    state, df, restated = load()
except psycopg.Error as e:
    st.error(f"Can't read the serving database: {e}. Run the pipeline first "
             "(see docs/phase-1-notes.md, How to run it).")
    st.stop()

st.caption(f"Data as of business date {state[1]} ({state[0]}). Orders purchased from "
           "1 Jan 2017 to 31 Aug 2018, delivered with a delivery date; late means delivered "
           "on a later calendar day than estimated.")

overall = df[df.region == "all"]
totals = overall[["delivered_orders", "late_orders"]].sum()
a, b, c = st.columns(3)
a.metric("Delivered orders", f"{int(totals.delivered_orders):,}")
b.metric("Late", f"{int(totals.late_orders):,}")
c.metric("Late rate", f"{totals.late_orders / max(totals.delivered_orders, 1):.2%}")

regions = df[df.region != "all"]
st.subheader("Late rate by delivery month and region")
st.line_chart(regions.pivot(index="delivery_month", columns="region", values="late_rate"))

st.subheader("How cautious are the promises? Days delivered before the estimate")
st.caption("Median per month, all regions; negative means late.")
st.bar_chart(overall.set_index("delivery_month")[["median_days_early"]])

st.subheader("Table")
st.dataframe(df, hide_index=True, use_container_width=True)

st.header("How much did published numbers change?")
st.caption("A pipeline metric, not a business one: it shows how values first published moved as "
           "late, duplicate and corrected events arrived (simulated arrival). Latest published "
           "value minus first, per metric, month and slice, with numerators and denominators.")
if restated.empty:
    st.info("Nothing has been republished yet.")
else:
    moved = restated[restated.restatement.fillna(0).abs() > 1e-12]
    a, b = st.columns(2)
    a.metric("Values republished", f"{len(restated):,}")
    b.metric("Values that changed", f"{len(moved):,}")
    st.dataframe(restated, hide_index=True, use_container_width=True)
