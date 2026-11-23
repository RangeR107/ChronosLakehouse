"""MERGE only the rows that changed (shared by build_dimensions, build_facts, recompute_aggregates).

Gold tables are written by recomputing rows and merging them on the table's grain. Comparing
values first means an unchanged rerun commits nothing, so retries are idempotent and a table's
snapshot history only shows real changes.
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.context import Context


def changed_rows(ctx: Context, table: str, rows: DataFrame) -> DataFrame:
    """The rows that are new or differ from the table's current row, compared on every column."""
    s, t = rows.alias("s"), ctx.spark.table(table).alias("t")
    # Null-safe: with a plain equality null never equals null, so every row holding a null (a
    # current SCD version's effective_to, an undelivered order's date) would look changed.
    same = reduce(lambda a, b: a & b, [F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
                                       for c in rows.columns])
    return s.join(t, same, "left_anti").select(*rows.columns)


def merge_changed(ctx: Context, table: str, rows: DataFrame, keys: list[str],
                  delete_missing: bool = False, delete_within: str | None = None) -> int:
    """Insert new keys, update rows whose values changed, leave the rest. Returns rows written.

    delete_missing is for tables recomputed whole, such as aggregates: a target row whose key
    the recompute no longer produces is deleted (an order delivered yesterday and cancelled
    today takes its region's row with it). delete_within limits that to target rows matching a
    SQL condition, for a recompute of only some periods.
    """
    t = ctx.spark.table(table).alias("t")
    changed = changed_rows(ctx, table, rows).localCheckpoint()
    count = changed.count()
    view = f"_changed_{table.replace('.', '_')}"
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    if count:
        changed.createOrReplaceTempView(view)
        ctx.spark.sql(f"""MERGE INTO {table} t USING {view} s ON {on}
                          WHEN MATCHED THEN UPDATE SET *
                          WHEN NOT MATCHED THEN INSERT *""")
    if delete_missing:
        scope = t.where(delete_within) if delete_within else t
        gone = scope.select(*keys).join(rows.select(*keys), keys, "left_anti").localCheckpoint()
        removed = gone.count()
        if removed:
            gone.createOrReplaceTempView(f"{view}_gone")
            ctx.spark.sql(f"MERGE INTO {table} t USING {view}_gone s ON {on} "
                          f"WHEN MATCHED THEN DELETE")
        count += removed
    return count


def create_tables(ctx: Context, layer: str, tables: dict[str, str]) -> None:
    ctx.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ctx.namespace(layer)}")
    for name, columns in tables.items():
        ctx.spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctx.table(layer, name)} ({columns})
                          USING iceberg TBLPROPERTIES ('format-version' = '2')""")
