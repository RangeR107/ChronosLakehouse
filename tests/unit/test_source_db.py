"""The source master in PostgreSQL: the replay-clock trigger and the generator's writes.

Spec: "Source database". The two rules tested hardest are the ones the spec calls easy to break:
updated_at comes from the database's replay clock, and address_effective_from carries business
time. A late update must get a new updated_at while keeping an old address_effective_from.
"""
import psycopg
import pytest

from generator.source_db import apply_schema, load_sellers, record_order_address, set_clock

pytestmark = pytest.mark.postgres

SELLER = {"seller_id": "s_1", "seller_zip_code_prefix": "13023", "seller_city": "campinas",
          "seller_state": "SP"}


def order(purchase, zip_code="01310", city="sao paulo", state="SP"):
    return {"customer_unique_id": "person_a", "customer_zip_code_prefix": zip_code,
            "customer_city": city, "customer_state": state, "order_purchase_timestamp": purchase}


def customer(conn):
    return conn.execute(
        "SELECT customer_zip_code_prefix, customer_city, address_effective_from::text,"
        " created_at::text, updated_at::text FROM source.customers"
        " WHERE customer_unique_id = 'person_a'").fetchone()


def test_trigger_stamps_replay_time_on_insert_and_update(source_db):
    set_clock(source_db, "2017-01-05 10:00:00")
    load_sellers(source_db, [SELLER])
    set_clock(source_db, "2017-01-09 08:00:00")
    source_db.execute("UPDATE source.sellers SET seller_city = 'valinhos'")
    created, updated = source_db.execute(
        "SELECT created_at::text, updated_at::text FROM source.sellers").fetchone()
    assert (created, updated) == ("2017-01-05 10:00:00", "2017-01-09 08:00:00")


def test_writer_cannot_set_updated_at_itself(source_db):
    set_clock(source_db, "2017-01-05 10:00:00")
    load_sellers(source_db, [SELLER])
    source_db.execute("UPDATE source.sellers SET updated_at = '2030-01-01'")
    assert source_db.execute("SELECT updated_at::text FROM source.sellers").fetchone()[0] == \
        "2017-01-05 10:00:00"


def test_writing_before_the_clock_is_set_fails(source_db):
    with pytest.raises(psycopg.errors.RaiseException, match="replay_clock is empty"):
        load_sellers(source_db, [SELLER])


def test_replay_clock_holds_one_row(source_db):
    set_clock(source_db, "2017-01-05 10:00:00")
    set_clock(source_db, "2017-01-06 10:00:00")
    assert source_db.execute("SELECT count(*), max(now_ts)::text FROM source.replay_clock") \
        .fetchone() == (1, "2017-01-06 10:00:00")
    with pytest.raises(psycopg.errors.CheckViolation):
        source_db.execute("INSERT INTO source.replay_clock VALUES (false, '2017-01-01')")


def test_reloading_sellers_changes_nothing(source_db):
    set_clock(source_db, "2017-01-05 10:00:00")
    load_sellers(source_db, [SELLER])
    set_clock(source_db, "2017-02-01 00:00:00")
    load_sellers(source_db, [SELLER])  # a rerun must not look like a change to the extract
    assert source_db.execute("SELECT updated_at::text FROM source.sellers").fetchone()[0] == \
        "2017-01-05 10:00:00"


def test_first_order_inserts_the_person(source_db):
    set_clock(source_db, "2017-01-05 10:15:00")
    record_order_address(source_db, order("2017-01-05 10:15:00"))
    assert customer(source_db) == ("01310", "sao paulo", "2017-01-05 10:15:00",
                                   "2017-01-05 10:15:00", "2017-01-05 10:15:00")


def test_same_address_again_changes_nothing(source_db):
    set_clock(source_db, "2017-01-05 10:15:00")
    record_order_address(source_db, order("2017-01-05 10:15:00"))
    set_clock(source_db, "2017-03-01 09:00:00")
    record_order_address(source_db, order("2017-03-01 09:00:00"))
    # effective_from stays at the FIRST order seen with this address.
    assert customer(source_db)[2:] == ("2017-01-05 10:15:00", "2017-01-05 10:15:00",
                                       "2017-01-05 10:15:00")


def test_new_address_updates_with_business_time_and_replay_time(source_db):
    set_clock(source_db, "2017-01-05 10:15:00")
    record_order_address(source_db, order("2017-01-05 10:15:00"))
    # A late update: the order was placed on 1 Mar, but the row only reaches the database on
    # 4 Mar. Business time and arrival time must stay separate.
    set_clock(source_db, "2017-03-04 12:00:00")
    record_order_address(source_db, order("2017-03-01 09:00:00", "80010", "curitiba", "PR"))
    assert customer(source_db) == ("80010", "curitiba", "2017-03-01 09:00:00",
                                   "2017-01-05 10:15:00", "2017-03-04 12:00:00")


def test_replaying_an_older_order_never_moves_the_person_back(source_db):
    set_clock(source_db, "2017-01-05 10:15:00")
    record_order_address(source_db, order("2017-01-05 10:15:00"))
    set_clock(source_db, "2017-03-01 09:00:00")
    record_order_address(source_db, order("2017-03-01 09:00:00", "80010", "curitiba", "PR"))
    set_clock(source_db, "2017-03-02 00:00:00")
    record_order_address(source_db, order("2017-01-05 10:15:00"))  # rerun of an old day
    assert customer(source_db)[:3] == ("80010", "curitiba", "2017-03-01 09:00:00")


def test_moving_back_to_an_old_address_is_a_new_version(source_db):
    # The real zip 71065 case: guara -> brasilia -> guara.
    set_clock(source_db, "2017-06-07 19:54:10")
    record_order_address(source_db, order("2017-06-07 19:54:10", "71065", "guara", "DF"))
    set_clock(source_db, "2017-08-14 12:18:15")
    record_order_address(source_db, order("2017-08-14 12:18:15", "71065", "brasilia", "DF"))
    set_clock(source_db, "2017-08-22 11:50:48")
    record_order_address(source_db, order("2017-08-22 11:50:48", "71065", "guara", "DF"))
    assert customer(source_db)[:3] == ("71065", "guara", "2017-08-22 11:50:48")


def test_schema_can_be_applied_again(source_db):
    apply_schema(source_db)  # every statement is IF NOT EXISTS / OR REPLACE
    source_db.commit()
