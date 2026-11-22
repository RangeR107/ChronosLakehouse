"""extract_dimensions: copy changed master rows from PostgreSQL into bronze (spec: chronos_daily
step 3, and the Decision log row on high-water-mark extraction).

For each master table it reads rows whose updated_at, the time the database wrote them, is:
  - later than the high-water mark minus a one-day overlap, and
  - earlier than the run's replay time.

The high-water mark is the latest updated_at already in bronze, so it is derived from the data,
never kept as separate state. The overlap re-reads the last day on purpose: a transaction that
commits late can carry an updated_at older than the mark, a classic way high-water marks lose
rows. Re-read rows that bronze already holds are skipped, so a retry appends nothing.

The upper bound is the replay clock standing in for "now": the generator can run ahead of the
pipeline, and without the bound day 1's extract would see rows written on day 60.
"""
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from jobs.context import Context, run_id, run_time

OVERLAP = timedelta(days=1)
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class Master:
    key: str
    columns: tuple[tuple[str, str], ...]  # (name, Spark type) besides the key and timestamps


MASTERS = {
    "customers": Master("customer_unique_id", (
        ("customer_zip_code_prefix", "string"), ("customer_city", "string"),
        ("customer_state", "string"), ("address_effective_from", "timestamp_ntz"))),
    "sellers": Master("seller_id", (
        ("seller_zip_code_prefix", "string"), ("seller_city", "string"),
        ("seller_state", "string"))),
    "products": Master("product_id", (
        ("product_category_name", "string"), ("product_weight_g", "int"),
        ("product_length_cm", "int"), ("product_height_cm", "int"),
        ("product_width_cm", "int"))),
}


def bronze_table(ctx: Context, name: str) -> str:
    return ctx.table("bronze", f"{name}_extract")


def ensure_table(ctx: Context, name: str, m: Master) -> None:
    cols = ",\n".join(f"{c} {t.upper()}" for c, t in m.columns)
    ctx.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ctx.namespace('bronze')}")
    ctx.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table(ctx, name)} (
            {m.key} STRING,
            {cols},
            created_at   TIMESTAMP_NTZ,
            updated_at   TIMESTAMP_NTZ COMMENT 'when PostgreSQL wrote the row (replay clock)',
            extracted_at TIMESTAMP_NTZ COMMENT 'replay time of the run that read it',
            run_id       STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)


def high_water_mark(ctx: Context, name: str):
    return ctx.spark.sql(f"SELECT max(updated_at) FROM {bronze_table(ctx, name)}").collect()[0][0]


def read_changed_rows(ctx: Context, name: str, m: Master, lower, upper) -> list[tuple]:
    # Everything comes back as text and is typed by Spark, the same way for every column, so no
    # driver-side conversion (time zones included) can alter a value on the way.
    cols = [m.key, *(c for c, _ in m.columns), "created_at", "updated_at"]
    select = ", ".join(f"{c}::text" for c in cols)
    where, params = ["updated_at < %(upper)s"], {"upper": upper.strftime(TS_FORMAT)}
    if lower is not None:
        where.append("updated_at > %(lower)s")
        params["lower"] = lower.strftime(TS_FORMAT)
    sql = (f"SELECT {select} FROM source.{name} WHERE {' AND '.join(where)}"
           f" ORDER BY {m.key}, updated_at")
    with psycopg.connect(ctx.source_conninfo) as conn:
        return conn.execute(sql, params).fetchall()


def extract_table(ctx: Context, business_date: date, name: str) -> int:
    m = MASTERS[name]
    ensure_table(ctx, name, m)
    mark = high_water_mark(ctx, name)
    lower = mark - OVERLAP if mark is not None else None
    rows = read_changed_rows(ctx, name, m, lower, run_time(business_date))
    if not rows:
        return 0

    names = [m.key, *(c for c, _ in m.columns), "created_at", "updated_at"]
    df = ctx.spark.createDataFrame(rows, StructType([StructField(c, StringType()) for c in names]))
    typed = df.select(
        F.col(m.key),
        *(F.col(c).cast(t).alias(c) for c, t in m.columns),
        F.col("created_at").cast("timestamp_ntz").alias("created_at"),
        F.col("updated_at").cast("timestamp_ntz").alias("updated_at"))
    # Rows the overlap re-read are already in bronze with the same key and updated_at.
    seen = ctx.spark.table(bronze_table(ctx, name)).select(m.key, "updated_at")
    new = typed.join(seen, [m.key, "updated_at"], "left_anti")
    stamp = F.lit(run_time(business_date).isoformat(sep=" ")).cast("timestamp_ntz")
    new = (new.withColumn("extracted_at", stamp).withColumn("run_id", F.lit(run_id(business_date)))
           .select(*names, "extracted_at", "run_id"))
    # Materialise once: the plan reads the bronze table it is about to append to, and must not
    # be evaluated again after the append (it would then filter out its own rows).
    new = new.localCheckpoint()
    count = new.count()
    if count:
        new.writeTo(bronze_table(ctx, name)).append()
    return count


def extract_dimensions(ctx: Context, business_date: date) -> dict[str, int]:
    """Rows appended to bronze per master table."""
    return {name: extract_table(ctx, business_date, name) for name in MASTERS}
