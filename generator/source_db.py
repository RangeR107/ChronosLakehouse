"""The generator's writes to the source master in PostgreSQL (spec: "Source database").

The generator plays the operational system: it loads sellers and products, inserts a customer at
their first order and updates the row when an order arrives with a new address. It sets the
replay clock before each write, so the database's trigger stamps updated_at with replay time.
"""
import os
from pathlib import Path

import psycopg

SCHEMA_SQL = Path(__file__).parent.parent / "sql" / "source_schema.sql"


def conninfo(dbname: str | None = None) -> str:
    """Connection string from environment variables, defaulting to the local Docker database."""
    return psycopg.conninfo.make_conninfo(
        host=os.environ.get("CHRONOS_DB_HOST", "localhost"),
        port=os.environ.get("CHRONOS_DB_PORT", "5433"),
        user=os.environ.get("CHRONOS_DB_USER", "chronos"),
        password=os.environ.get("CHRONOS_DB_PASSWORD", "chronos"),
        dbname=dbname or os.environ.get("CHRONOS_DB_NAME", "chronos"),
    )


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_SQL.read_text())


def set_clock(conn: psycopg.Connection, now_ts: str) -> None:
    conn.execute(
        "INSERT INTO source.replay_clock (one, now_ts) VALUES (true, %s) "
        "ON CONFLICT (one) DO UPDATE SET now_ts = EXCLUDED.now_ts", (now_ts,))


def load_sellers(conn: psycopg.Connection, sellers: list[dict]) -> None:
    # DO NOTHING on a rerun: rewriting an unchanged row would give it a new updated_at, and the
    # next extract would pick it up as a change that never happened.
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO source.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)"
            " VALUES (%(seller_id)s, %(seller_zip_code_prefix)s, %(seller_city)s, %(seller_state)s)"
            " ON CONFLICT (seller_id) DO NOTHING", sellers)


def load_products(conn: psycopg.Connection, products: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO source.products (product_id, product_category_name, product_weight_g,"
            " product_length_cm, product_height_cm, product_width_cm)"
            " VALUES (%(product_id)s, %(product_category_name)s, %(product_weight_g)s,"
            " %(product_length_cm)s, %(product_height_cm)s, %(product_width_cm)s)"
            " ON CONFLICT (product_id) DO NOTHING", products)


def record_order_address(conn: psycopg.Connection, order: dict) -> None:
    """Insert the person at their first order; update when a later order has a new address.

    `order` is an order_placed payload. The update fires only when the address differs AND the
    order is newer than the address on file, so replaying an old day again is a no-op instead of
    moving the person back to an old address.
    """
    conn.execute(
        """
        INSERT INTO source.customers AS c (customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state, address_effective_from)
        VALUES (%(customer_unique_id)s, %(customer_zip_code_prefix)s, %(customer_city)s,
            %(customer_state)s, %(order_purchase_timestamp)s)
        ON CONFLICT (customer_unique_id) DO UPDATE SET
            customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
            customer_city = EXCLUDED.customer_city,
            customer_state = EXCLUDED.customer_state,
            address_effective_from = EXCLUDED.address_effective_from
        WHERE (c.customer_zip_code_prefix, c.customer_city, c.customer_state)
                  IS DISTINCT FROM (EXCLUDED.customer_zip_code_prefix, EXCLUDED.customer_city,
                                    EXCLUDED.customer_state)
          AND EXCLUDED.address_effective_from > c.address_effective_from
        """, order)
