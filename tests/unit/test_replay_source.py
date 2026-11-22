"""The replay writes the source master as the operational system would (fixture: olist_mini)."""
from datetime import date
from pathlib import Path

import pytest

from generator.replay import replay

pytestmark = pytest.mark.postgres

MINI = Path(__file__).parent.parent / "fixtures" / "olist_mini"


def run(source_db, landing, end=date(2017, 1, 15)):
    return replay(MINI, landing, date(2017, 1, 1), end, purchased_from=date(2017, 1, 1),
                  conn=source_db)


def rows(conn, sql):
    return [tuple(r) for r in conn.execute(sql).fetchall()]


def test_customers_are_written_at_order_time(source_db, tmp_path):
    run(source_db, tmp_path)
    assert rows(source_db, """
        SELECT customer_unique_id, customer_city, address_effective_from::text,
               created_at::text, updated_at::text
        FROM source.customers ORDER BY 1""") == [
        # person_a moved: first seen in sao paulo, then ordered from curitiba on 7 Jan.
        ("person_a", "curitiba", "2017-01-07 09:00:00", "2017-01-05 10:15:00", "2017-01-07 09:00:00"),
        ("person_b", "rio de janeiro", "2017-01-05 23:40:00", "2017-01-05 23:40:00", "2017-01-05 23:40:00"),
        ("person_c", "belo horizonte", "2017-01-06 12:00:00", "2017-01-06 12:00:00", "2017-01-06 12:00:00"),
        ("person_d", "salvador", "2017-01-06 13:00:00", "2017-01-06 13:00:00", "2017-01-06 13:00:00"),
        ("person_e", "manaus", "2017-01-07 18:00:00", "2017-01-07 18:00:00", "2017-01-07 18:00:00"),
        # person_f only ordered in 2016, before the slice, so the slice's source never saw them.
    ]


def test_sellers_and_products_are_loaded_at_the_start(source_db, tmp_path):
    run(source_db, tmp_path)
    assert rows(source_db, "SELECT seller_id, updated_at::text FROM source.sellers ORDER BY 1") == [
        ("s_1", "2017-01-01 00:00:00"), ("s_2", "2017-01-01 00:00:00")]
    assert rows(source_db, """SELECT product_id, product_category_name, product_weight_g
                              FROM source.products ORDER BY 1""") == [
        ("p_1", "cama_mesa_banho", 225), ("p_2", None, None), ("p_3", "pc_gamer", 1000)]


def test_replaying_again_changes_no_row(source_db, tmp_path):
    run(source_db, tmp_path)
    before = rows(source_db, "SELECT * FROM source.customers ORDER BY 1")
    run(source_db, tmp_path)  # same days again: same files, and no row looks changed
    assert rows(source_db, "SELECT * FROM source.customers ORDER BY 1") == before


def test_replaying_in_pieces_equals_one_run(source_db, tmp_path):
    replay(MINI, tmp_path / "a", date(2017, 1, 1), date(2017, 1, 6), date(2017, 1, 1), source_db)
    replay(MINI, tmp_path / "a", date(2017, 1, 7), date(2017, 1, 15), date(2017, 1, 1), source_db)
    pieces = rows(source_db, "SELECT * FROM source.customers ORDER BY 1")
    source_db.execute("TRUNCATE source.customers, source.sellers, source.products")
    run(source_db, tmp_path / "b")
    assert rows(source_db, "SELECT * FROM source.customers ORDER BY 1") == pieces
