"""load_serving: copy gold's dashboard tables into PostgreSQL `serving` in one transaction
(spec: Publishing, "Serve"; chronos_daily step 12).

The rows go into a staging table, then one transaction MERGEs them into the serving table,
deletes rows gold no longer has, and records what was loaded. The dashboard therefore sees the
previous load or the new one, never half of each. Phase 3 reads gold at a release's snapshot ids
instead of its latest state; the transaction stays the same.
"""
from datetime import date

import psycopg

from jobs.context import Context, run_id, run_time

SERVING = {
    "delivery_by_month_region": {
        "gold": "agg_delivery_by_month_region",
        "keys": ["delivery_month", "region"],
        "columns": {
            "delivery_month": "text", "region": "text", "delivered_orders": "bigint",
            "late_orders": "bigint", "late_rate": "double precision",
            "median_days_early": "double precision", "p10_days_early": "double precision",
            "p90_days_early": "double precision", "share_7_days_early": "double precision"},
    },
    # Question 8. A pipeline metric: it describes simulated arrival, not Olist's business.
    "restatements": {
        "gold": "agg_restatements",
        "keys": ["metric_id", "period_type", "period_start", "slice_type", "slice_value"],
        "columns": {
            "metric_id": "text", "period_type": "text", "period_start": "date",
            "slice_type": "text", "slice_value": "text", "releases": "bigint",
            "first_release": "text", "first_value": "double precision",
            "first_numerator": "double precision", "first_denominator": "double precision",
            "latest_release": "text", "latest_value": "double precision",
            "latest_numerator": "double precision", "latest_denominator": "double precision",
            "restatement": "double precision"},
    },
}


def ensure_serving_schema(conn: psycopg.Connection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS serving")
    for name, t in SERVING.items():
        cols = ", ".join(f"{c} {typ}" for c, typ in t["columns"].items())
        conn.execute(f"CREATE TABLE IF NOT EXISTS serving.{name} ({cols}, "
                     f"PRIMARY KEY ({', '.join(t['keys'])}))")
    # One row: what the dashboard is showing. Phase 3 turns this into the active release id.
    conn.execute("""CREATE TABLE IF NOT EXISTS serving.load_state (
                        id boolean PRIMARY KEY DEFAULT true CHECK (id),
                        run_id text NOT NULL, business_date date NOT NULL,
                        loaded_at timestamp NOT NULL)""")


def load_table(conn: psycopg.Connection, name: str, rows: list[tuple]) -> None:
    t = SERVING[name]
    cols = list(t["columns"])
    stage = f"stage_{name}"
    conn.execute(f"CREATE TEMP TABLE {stage} (LIKE serving.{name}) ON COMMIT DROP")
    with conn.cursor().copy(f"COPY {stage} ({', '.join(cols)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    on = " AND ".join(f"t.{k} = s.{k}" for k in t["keys"])
    values = [c for c in cols if c not in t["keys"]]
    conn.execute(f"""
        MERGE INTO serving.{name} t USING {stage} s ON {on}
        WHEN MATCHED THEN UPDATE SET {', '.join(f'{c} = s.{c}' for c in values)}
        WHEN NOT MATCHED THEN INSERT ({', '.join(cols)})
                              VALUES ({', '.join(f's.{c}' for c in cols)})""")
    conn.execute(f"DELETE FROM serving.{name} t WHERE NOT EXISTS "
                 f"(SELECT 1 FROM {stage} s WHERE {on})")


def load_serving(ctx: Context, business_date: date) -> dict[str, int]:
    # Collected to the driver: the dashboard tables are small aggregates by design.
    data = {name: [tuple(r) for r in ctx.spark.table(ctx.table("gold", t["gold"]))
                   .select(*t["columns"]).collect()]
            for name, t in SERVING.items()}
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_serving_schema(conn)
        conn.commit()
        with conn.transaction():
            for name, rows in data.items():
                load_table(conn, name, rows)
            conn.execute("""INSERT INTO serving.load_state (run_id, business_date, loaded_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET run_id = EXCLUDED.run_id,
                                business_date = EXCLUDED.business_date,
                                loaded_at = EXCLUDED.loaded_at""",
                         (run_id(business_date), business_date, run_time(business_date)))
    return {name: len(rows) for name, rows in data.items()}
