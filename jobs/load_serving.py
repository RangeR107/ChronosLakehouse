"""load_serving: copy gold's dashboard tables into PostgreSQL `serving` in one transaction
(spec: Publishing, "Serve"; chronos_daily step 12).

Gold is read at the published release's snapshot ids (metrics.releases). The rows go into
staging tables, then one transaction MERGEs them into the serving tables, deletes rows the release
no longer has, and records the active release. The dashboards therefore show one release or the
next, never half of each.
"""
from datetime import date

import psycopg

from jobs.context import Context, on_branch, release_id, run_id, run_time

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
    # One row: what the dashboard is showing, including the active release.
    conn.execute("""CREATE TABLE IF NOT EXISTS serving.load_state (
                        id boolean PRIMARY KEY DEFAULT true CHECK (id),
                        run_id text NOT NULL, business_date date NOT NULL,
                        loaded_at timestamp NOT NULL, release_id text)""")
    # Databases made before Phase 3 lack release_id. ALTER TABLE takes an exclusive lock even
    # with IF NOT EXISTS, so it would queue behind any open dashboard read: run it only when the
    # column is really missing, and give up after a few seconds rather than hang the run.
    has_release = conn.execute("""SELECT 1 FROM information_schema.columns
                                  WHERE table_schema = 'serving' AND table_name = 'load_state'
                                    AND column_name = 'release_id'""").fetchone()
    if not has_release:
        conn.execute("SET LOCAL lock_timeout = '5s'")
        conn.execute("ALTER TABLE serving.load_state ADD COLUMN release_id text")


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


def release_snapshots(conn: psycopg.Connection, namespace: str,
                      business_date: date) -> dict[str, int]:
    row = conn.execute("SELECT snapshots FROM metrics.releases WHERE namespace = %s AND "
                       "business_date = %s AND status = 'published'",
                       (namespace, business_date)).fetchone()
    if row is None:
        raise LookupError(f"no published release for {business_date}: nothing to serve")
    return row[0]


def load_serving(ctx: Context, business_date: date) -> dict:
    """Serve the date's published release, read at its snapshot ids, never at main's latest.

    The dashboards never move backwards: if they already show a later release, a rerun of an
    older date leaves them alone.
    """
    rid = release_id(business_date)
    with psycopg.connect(ctx.source_conninfo) as conn:
        ensure_serving_schema(conn)
        conn.commit()
        active = conn.execute("SELECT business_date, release_id FROM serving.load_state"
                              ).fetchone()
        if active and active[0] > business_date:
            return {"skipped": f"serving already shows {active[1]}"}
        snapshots = release_snapshots(conn, ctx.namespace_prefix, business_date)
        data = {}
        with on_branch(ctx, None):
            for name, t in SERVING.items():
                table = ctx.table("gold", t["gold"])
                # Collected to the driver: the dashboard tables are small aggregates by design.
                data[name] = [tuple(r) for r in ctx.spark.sql(
                    f"SELECT {', '.join(t['columns'])} FROM {table} "
                    f"VERSION AS OF {snapshots[table]}").collect()]
        with conn.transaction():
            for name, rows in data.items():
                load_table(conn, name, rows)
            conn.execute("""INSERT INTO serving.load_state
                                (run_id, business_date, loaded_at, release_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET run_id = EXCLUDED.run_id,
                                business_date = EXCLUDED.business_date,
                                loaded_at = EXCLUDED.loaded_at,
                                release_id = EXCLUDED.release_id""",
                         (run_id(business_date), business_date, run_time(business_date), rid))
    return {"release_id": rid, **{name: len(rows) for name, rows in data.items()}}
