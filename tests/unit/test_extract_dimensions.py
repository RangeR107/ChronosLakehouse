"""extract_dimensions: the high-water mark, its one-day overlap and the replay-time upper bound.

Spec: chronos_daily step 3, late-data scenario 6 (late dimension change) and the Decision log row
on polling extraction. Source rows are written the way the generator writes them, with the replay
clock set first, so the database's trigger stamps updated_at.
"""
import uuid
from datetime import date

import pytest

from generator.source_db import load_products, load_sellers, record_order_address, set_clock
from jobs.context import Context
from jobs.db import conninfo
from jobs.extract_dimensions import extract_dimensions

pytestmark = pytest.mark.postgres

JAN5, JAN6 = date(2017, 1, 5), date(2017, 1, 6)


@pytest.fixture
def ctx(spark, source_db, tmp_path):
    return Context(spark, tmp_path, namespace_prefix=f"t{uuid.uuid4().hex[:8]}_",
                   source_conninfo=conninfo(source_db.info.dbname))


def order(person, purchase, city="sao paulo"):
    return {"customer_unique_id": person, "customer_zip_code_prefix": "01310",
            "customer_city": city, "customer_state": "SP", "order_purchase_timestamp": purchase}


def write(conn, clock, *orders):
    set_clock(conn, clock)
    for o in orders:
        record_order_address(conn, o)
    conn.commit()


def customers(ctx):
    return [tuple(r) for r in ctx.spark.sql(f"""
        SELECT customer_unique_id, customer_city, CAST(address_effective_from AS STRING),
               CAST(updated_at AS STRING), CAST(extracted_at AS STRING), run_id
        FROM {ctx.table('bronze', 'customers_extract')}
        ORDER BY extracted_at, customer_unique_id, updated_at""").collect()]


def test_reads_only_rows_written_before_the_run(ctx, source_db):
    write(source_db, "2017-01-05 10:15:00", order("person_a", "2017-01-05 10:15:00"))
    # Written on 6 Jan: the generator ran ahead, but the 5 Jan run must not see it yet.
    write(source_db, "2017-01-06 09:00:00", order("person_b", "2017-01-06 09:00:00"))
    assert extract_dimensions(ctx, JAN5)["customers"] == 1
    assert customers(ctx) == [("person_a", "sao paulo", "2017-01-05 10:15:00",
                               "2017-01-05 10:15:00", "2017-01-06 00:00:00", "run_2017-01-05")]
    assert extract_dimensions(ctx, JAN6)["customers"] == 1  # person_b; person_a re-read, skipped


def test_retry_appends_nothing(ctx, source_db):
    write(source_db, "2017-01-05 10:15:00", order("person_a", "2017-01-05 10:15:00"))
    first = extract_dimensions(ctx, JAN5)
    before = customers(ctx)
    assert extract_dimensions(ctx, JAN5) == {"customers": 0, "sellers": 0, "products": 0}
    assert first["customers"] == 1 and customers(ctx) == before


def test_late_address_update_keeps_business_time_and_is_still_found(ctx, source_db):
    write(source_db, "2017-01-05 10:15:00", order("person_a", "2017-01-05 10:15:00"))
    extract_dimensions(ctx, JAN5)
    # Scenario 6: the order on 7 Jan used a new address, but the master row is only updated on
    # 10 Jan. Its updated_at is new, so the 10 Jan extract finds it; its business time is old.
    write(source_db, "2017-01-10 12:00:00", order("person_a", "2017-01-07 09:00:00", "curitiba"))
    for day in (date(2017, 1, 7), date(2017, 1, 8), date(2017, 1, 9)):
        assert extract_dimensions(ctx, day)["customers"] == 0
    assert extract_dimensions(ctx, date(2017, 1, 10))["customers"] == 1
    assert customers(ctx)[-1] == ("person_a", "curitiba", "2017-01-07 09:00:00",
                                  "2017-01-10 12:00:00", "2017-01-11 00:00:00", "run_2017-01-10")


def test_overlap_catches_a_row_committed_late_with_an_older_updated_at(ctx, source_db):
    write(source_db, "2017-01-05 20:00:00", order("person_a", "2017-01-05 20:00:00"))
    extract_dimensions(ctx, JAN5)  # high-water mark is now 5 Jan 20:00
    # A transaction stamped 18:00 that committed after the extract ran: older than the mark.
    write(source_db, "2017-01-05 18:00:00", order("person_c", "2017-01-05 18:00:00"))
    assert extract_dimensions(ctx, JAN6)["customers"] == 1
    assert [c[0] for c in customers(ctx)] == ["person_a", "person_c"]


def test_sellers_and_products_are_typed(ctx, source_db):
    set_clock(source_db, "2017-01-01 00:00:00")
    load_sellers(source_db, [{"seller_id": "s_1", "seller_zip_code_prefix": "01310",
                              "seller_city": "sao paulo", "seller_state": "SP"}])
    load_products(source_db, [
        {"product_id": "p_1", "product_category_name": "perfumaria", "product_weight_g": "225",
         "product_length_cm": "16", "product_height_cm": "10", "product_width_cm": "14"},
        {"product_id": "p_2", "product_category_name": None, "product_weight_g": None,
         "product_length_cm": None, "product_height_cm": None, "product_width_cm": None}])
    source_db.commit()
    assert extract_dimensions(ctx, date(2017, 1, 1)) == {"customers": 0, "sellers": 1,
                                                        "products": 2}
    rows = ctx.spark.sql(f"""SELECT product_id, product_category_name, product_weight_g
                             FROM {ctx.table('bronze', 'products_extract')} ORDER BY 1""").collect()
    assert [tuple(r) for r in rows] == [("p_1", "perfumaria", 225), ("p_2", None, None)]
    zip_code = ctx.spark.sql(f"SELECT seller_zip_code_prefix FROM "
                             f"{ctx.table('bronze', 'sellers_extract')}").collect()[0][0]
    assert zip_code == "01310"  # a code, not a number: the leading zero stays
