"""build_dimensions: surrogate keys, SCD Type 2 customers, geography, sellers, products, dates.

Spec: Data model (SCD Type 2 rules, geography, Type 1 sellers and products) and late-data
scenario 6. Extract rows are written straight into bronze, as extract_dimensions would.
"""
import uuid
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from jobs.bronze_to_silver import ensure_silver_tables
from jobs.build_dimensions import build_dimensions
from jobs.context import Context
from jobs.extract_dimensions import MASTERS, bronze_table, ensure_table
from jobs.keys import surrogate_key, surrogate_key_py

MINI = Path(__file__).parent.parent / "fixtures" / "olist_mini"
DAY = date(2017, 3, 1)


@pytest.fixture
def ctx(spark, tmp_path):
    c = Context(spark, tmp_path, namespace_prefix=f"t{uuid.uuid4().hex[:8]}_", reference_dir=MINI)
    for name, m in MASTERS.items():
        ensure_table(c, name, m)
    ensure_silver_tables(c)
    return c


def add_customers(ctx, *rows, extracted_at="2017-01-10 00:00:00"):
    """rows: (person, zip, city, state, address_effective_from, updated_at)"""
    df = ctx.spark.createDataFrame(rows, "customer_unique_id string, customer_zip_code_prefix string,"
                                         " customer_city string, customer_state string,"
                                         " address_effective_from string, updated_at string")
    (df.select("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
               F.col("address_effective_from").cast("timestamp_ntz").alias("address_effective_from"),
               F.col("updated_at").cast("timestamp_ntz").alias("created_at"),
               F.col("updated_at").cast("timestamp_ntz").alias("updated_at"),
               F.lit(extracted_at).cast("timestamp_ntz").alias("extracted_at"),
               F.lit("run_test").alias("run_id"))
     .writeTo(bronze_table(ctx, "customers")).append())


def versions(ctx):
    return [tuple(r) for r in ctx.spark.sql(f"""
        SELECT customer_unique_id, customer_city, CAST(effective_from AS STRING),
               CAST(effective_to AS STRING), is_current, effective_from_basis
        FROM {ctx.table('gold', 'dim_customer')} ORDER BY 1, 3""").collect()]


def test_surrogate_key_matches_sha256_and_can_be_negative(spark):
    parts = [("person_a", "2017-01-05 10:15:00"), ("71065", "guara", "DF"), ("x",), ("y",)]
    got = [spark.range(1).select(surrogate_key(*(F.lit(p) for p in ps))).collect()[0][0]
           for ps in parts]
    assert got == [surrogate_key_py(*ps) for ps in parts]
    assert any(k < 0 for k in got), "the fixture should include a key with the top bit set"
    missing = spark.range(1).select(surrogate_key(F.lit("71065"), F.lit(None).cast("string"),
                                                  F.lit("DF"))).collect()[0][0]
    assert missing is None  # an unknown part gives no key, never a key of the other parts


def test_address_changes_become_versions(ctx):
    # The real zip 71065 case: guara -> brasilia -> guara, each a new version.
    add_customers(ctx,
                  ("p1", "71065", "guara", "DF", "2017-06-07 19:54:10", "2017-06-07 19:54:10"),
                  ("p1", "71065", "brasilia", "DF", "2017-08-14 12:18:15", "2017-08-14 12:18:15"),
                  ("p1", "71065", "guara", "DF", "2017-08-22 11:50:48", "2017-08-22 11:50:48"),
                  ("p2", "01310", "sao paulo", "SP", "2017-01-05 10:15:00", "2017-01-05 10:15:00"),
                  extracted_at="2017-09-01 00:00:00")
    build_dimensions(ctx, date(2017, 9, 1))
    assert versions(ctx) == [
        ("p1", "guara", "2017-06-07 19:54:10", "2017-08-14 12:18:15", False, "first_order_seen"),
        ("p1", "brasilia", "2017-08-14 12:18:15", "2017-08-22 11:50:48", False, "first_order_seen"),
        ("p1", "guara", "2017-08-22 11:50:48", None, True, "first_order_seen"),
        ("p2", "sao paulo", "2017-01-05 10:15:00", None, True, "first_order_seen")]
    keys = ctx.spark.sql(f"SELECT customer_key FROM {ctx.table('gold', 'dim_customer')}"
                         " WHERE customer_unique_id = 'p2'").collect()[0][0]
    assert keys == surrogate_key_py("p2", "2017-01-05 10:15:00")


def test_a_late_version_slots_into_the_chain(ctx):
    add_customers(ctx, ("p1", "01310", "sao paulo", "SP", "2017-01-05 10:15:00", "2017-01-05 10:15:00"),
                  ("p1", "88010", "florianopolis", "SC", "2017-03-01 09:00:00", "2017-03-01 09:00:00"))
    build_dimensions(ctx, DAY)
    # Scenario 6: an address used from 1 Feb reaches the master only on 4 Mar, after a later one.
    add_customers(ctx, ("p1", "80010", "curitiba", "PR", "2017-02-01 08:00:00", "2017-03-04 12:00:00"),
                  extracted_at="2017-03-05 00:00:00")
    build_dimensions(ctx, date(2017, 3, 4))
    assert [(v[1], v[2], v[3], v[4]) for v in versions(ctx)] == [
        ("sao paulo", "2017-01-05 10:15:00", "2017-02-01 08:00:00", False),
        ("curitiba", "2017-02-01 08:00:00", "2017-03-01 09:00:00", False),
        ("florianopolis", "2017-03-01 09:00:00", None, True)]


def test_the_run_sees_only_rows_extracted_by_its_replay_time(ctx):
    add_customers(ctx, ("p1", "01310", "sao paulo", "SP", "2017-01-05 10:15:00", "2017-01-05 10:15:00"),
                  extracted_at="2017-01-06 00:00:00")
    add_customers(ctx, ("p2", "01310", "sao paulo", "SP", "2017-02-05 10:15:00", "2017-02-05 10:15:00"),
                  extracted_at="2017-02-06 00:00:00")
    build_dimensions(ctx, date(2017, 1, 5))
    assert [v[0] for v in versions(ctx)] == ["p1"]


def test_rerun_commits_nothing(ctx):
    add_customers(ctx, ("p1", "01310", "sao paulo", "SP", "2017-01-05 10:15:00", "2017-01-05 10:15:00"))
    first = build_dimensions(ctx, DAY)
    assert first["dim_customer"] == 1 and first["dim_date"] > 1000
    snapshots = ctx.spark.sql(f"SELECT count(*) FROM {ctx.table('gold', 'dim_customer')}.snapshots").collect()[0][0]
    assert build_dimensions(ctx, DAY) == {k: 0 for k in first}
    assert ctx.spark.sql(f"SELECT count(*) FROM {ctx.table('gold', 'dim_customer')}.snapshots").collect()[0][0] == snapshots


def test_geography_normalises_city_and_maps_region(ctx):
    orders = ctx.spark.createDataFrame(
        # The third spelling is decomposed Unicode: 'a' plus a combining tilde (one real seller).
        [("e1", "o1", "01310", "São Paulo "), ("e2", "o2", "01310", "sao paulo"),
         ("e4", "o4", "01310", "sa\u0303o paulo"), ("e3", "o3", "69005", "manaus")],
        "event_id string, order_id string, customer_zip_code_prefix string, customer_city string")
    (orders.select("event_id", "order_id", F.lit("c").alias("customer_id"),
                   F.lit("p").alias("customer_unique_id"), "customer_zip_code_prefix",
                   "customer_city",
                   F.when(F.col("order_id") == "o3", "AM").otherwise("SP").alias("customer_state"),
                   F.lit(None).cast("timestamp_ntz").alias("order_purchase_timestamp"),
                   F.lit(None).cast("timestamp_ntz").alias("order_estimated_delivery_date"),
                   F.lit(1).alias("version"))
     .writeTo(ctx.table("silver", "orders")).append())
    build_dimensions(ctx, DAY)
    rows = ctx.spark.sql(f"""SELECT zip_code_prefix, city, state, region, geography_key
                             FROM {ctx.table('gold', 'dim_geography')} ORDER BY 1""").collect()
    assert [tuple(r)[:4] for r in rows] == [("01310", "sao paulo", "SP", "Southeast"),
                                            ("69005", "manaus", "AM", "North")]
    assert rows[0].geography_key == surrogate_key_py("01310", "sao paulo", "SP")


def test_products_get_english_categories(ctx):
    rows = [("p_1", "cama_mesa_banho"), ("p_2", None), ("p_3", "pc_gamer")]
    (ctx.spark.createDataFrame(rows, "product_id string, product_category_name string")
     .select("product_id", "product_category_name",
             *(F.lit(None).cast("int").alias(c) for c in ("product_weight_g", "product_length_cm",
                                                         "product_height_cm", "product_width_cm")),
             *(F.lit("2017-01-01 00:00:00").cast("timestamp_ntz").alias(c)
               for c in ("created_at", "updated_at", "extracted_at")),
             F.lit("run_test").alias("run_id"))
     .writeTo(bronze_table(ctx, "products")).append())
    build_dimensions(ctx, DAY)
    got = ctx.spark.sql(f"SELECT product_id, category FROM {ctx.table('gold', 'dim_product')} ORDER BY 1")
    assert [tuple(r) for r in got.collect()] == [
        ("p_1", "bed_bath_table"), ("p_2", "unknown"), ("p_3", "pc_gamer")]


def test_calendar_marks_black_friday(ctx):
    build_dimensions(ctx, DAY)
    row = ctx.spark.sql(f"""SELECT date_key, weekday, quarter, sale_day FROM {ctx.table('gold', 'dim_date')}
                            WHERE date = DATE'2017-11-24'""").collect()[0]
    assert tuple(row) == (20171124, "Friday", 4, "black_friday")


def add_order(ctx, person, purchased_at):
    ctx.spark.sql(f"""INSERT INTO {ctx.table('silver', 'orders')} VALUES
        ('e_{person}', 'o_{person}', 'c_{person}', '{person}', '80010', 'curitiba', 'PR',
         TIMESTAMP_NTZ '{purchased_at}', TIMESTAMP_NTZ '2017-02-01 00:00:00', 1)""")


def test_a_late_customer_row_that_starts_after_the_first_order_closes_the_placeholder(ctx):
    # p1 orders on 5 Jan before the master has them: an inferred placeholder, current.
    add_order(ctx, "p1", "2017-01-05 10:15:00")
    build_dimensions(ctx, date(2017, 1, 5))
    assert [(v[1], v[2], v[3], v[4]) for v in versions(ctx)] == [
        (None, "2017-01-05 10:15:00", None, True)]
    # Their row arrives with an address first used on 7 Jan (it changed again meanwhile). The
    # placeholder covers 5 to 7 Jan and is closed; only the real row is current.
    add_customers(ctx, ("p1", "80010", "curitiba", "PR", "2017-01-07 09:00:00",
                        "2017-01-07 09:00:00"), extracted_at="2017-01-08 00:00:00")
    build_dimensions(ctx, date(2017, 1, 7))
    assert [(v[1], v[2], v[3], v[4]) for v in versions(ctx)] == [
        (None, "2017-01-05 10:15:00", "2017-01-07 09:00:00", False),
        ("curitiba", "2017-01-07 09:00:00", None, True)]
