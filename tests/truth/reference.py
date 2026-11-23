"""The truth test's reference build: the business tables computed from the Olist CSVs in DuckDB SQL.

It shares no code with the pipeline on purpose (spec: Testing and evaluation). The pipeline gets
its tables through events, files, bronze, silver and MERGEs; this gets them straight from the
source files. If both agree row by row, whatever happened on the way didn't change the answer.

"As known at the end of the slice": an event counts if it was emitted on or before the cut-off
date. Events with a time are emitted at that time; events without one (cancellations and the 8
undated deliveries) one day after the order's last observed timestamp, the spec's written rule.
"""
from datetime import date
from pathlib import Path

import duckdb

# Spec: "Region mapping", IBGE's five regions.
REGIONS = {
    "North": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Northeast": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Central-West": ["DF", "GO", "MT", "MS"],
    "Southeast": ["ES", "MG", "RJ", "SP"],
    "South": ["PR", "RS", "SC"],
}

SQL = r"""
-- Surrogate key: first 8 bytes of SHA-256 as a signed BIGINT; parts joined with 0x1F.
CREATE MACRO skey(s) AS (
    (('0x' || substr(sha256(s), 1, 16))::UBIGINT::HUGEINT)
    - CASE WHEN substr(sha256(s), 1, 1) >= '8' THEN 18446744073709551616::HUGEINT
           ELSE 0::HUGEINT END)::BIGINT;
CREATE MACRO sep() AS chr(31);
CREATE MACRO ts_text(t) AS strftime(t, '%Y-%m-%d %H:%M:%S');
CREATE MACRO dkey(t) AS CAST(strftime(t, '%Y%m%d') AS INTEGER);
CREATE MACRO city(c) AS replace(replace(
    strip_accents(lower(trim(regexp_replace(c, '\s+', ' ', 'g')))), '´', ''''), '`', '''');
CREATE MACRO geo_key(z, c, s) AS skey(z || sep() || city(c) || sep() || s);
CREATE MACRO days(a, b) AS (epoch(b) - epoch(a)) / 86400.0;

CREATE TABLE raw_orders AS SELECT * FROM read_csv('{dir}/olist_orders_dataset.csv', all_varchar = true);
CREATE TABLE raw_customers AS SELECT * FROM read_csv('{dir}/olist_customers_dataset.csv', all_varchar = true);
CREATE TABLE raw_items AS SELECT * FROM read_csv('{dir}/olist_order_items_dataset.csv', all_varchar = true);
CREATE TABLE raw_payments AS SELECT * FROM read_csv('{dir}/olist_order_payments_dataset.csv', all_varchar = true);
CREATE TABLE raw_reviews AS SELECT * FROM read_csv('{dir}/olist_order_reviews_dataset.csv', all_varchar = true);
CREATE TABLE raw_sellers AS SELECT * FROM read_csv('{dir}/olist_sellers_dataset.csv', all_varchar = true);
CREATE TABLE raw_products AS SELECT * FROM read_csv('{dir}/olist_products_dataset.csv', all_varchar = true);
CREATE TABLE raw_names AS SELECT * FROM read_csv('{dir}/product_category_name_translation.csv', all_varchar = true);

-- Orders in the slice: purchased from the start, and the purchase (order_placed) known by the cut-off.
CREATE TABLE o AS
SELECT order_id, customer_id, order_status AS status,
       CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase,
       CAST(NULLIF(order_approved_at, '') AS TIMESTAMP) AS approved_all,
       CAST(NULLIF(order_delivered_carrier_date, '') AS TIMESTAMP) AS carrier_all,
       CAST(NULLIF(order_delivered_customer_date, '') AS TIMESTAMP) AS delivered_all,
       CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated
FROM raw_orders
WHERE CAST(order_purchase_timestamp AS TIMESTAMP) >= TIMESTAMP '{start}'
  AND CAST(CAST(order_purchase_timestamp AS TIMESTAMP) AS DATE) <= DATE '{cutoff}';

CREATE TABLE known AS
SELECT o.*, c.customer_unique_id AS person, c.customer_zip_code_prefix AS zip,
       c.customer_city AS city_raw, c.customer_state AS state,
       CASE WHEN CAST(approved_all AS DATE) <= DATE '{cutoff}' THEN approved_all END AS approved,
       CASE WHEN CAST(carrier_all AS DATE) <= DATE '{cutoff}' THEN carrier_all END AS carrier,
       CASE WHEN CAST(delivered_all AS DATE) <= DATE '{cutoff}' THEN delivered_all END AS delivered,
       -- the rule for events without a time: one day after the last observed timestamp
       CAST(greatest(purchase, approved_all, carrier_all, delivered_all) + INTERVAL 1 DAY AS DATE)
           <= DATE '{cutoff}' AS untimed_known
FROM o JOIN raw_customers c USING (customer_id);

CREATE TABLE status AS
SELECT order_id,
       CASE WHEN status IN ('canceled', 'unavailable') AND untimed_known THEN status
            WHEN delivered IS NOT NULL THEN 'delivered'
            WHEN status = 'delivered' AND delivered_all IS NULL AND untimed_known THEN 'delivered'
       END AS final_status
FROM known;

CREATE TABLE items AS
SELECT i.order_id, CAST(order_item_id AS INTEGER) AS order_item_id, product_id, seller_id,
       CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
       CAST(price AS DECIMAL(12, 2)) AS price, CAST(freight_value AS DECIMAL(12, 2)) AS freight_value
FROM raw_items i JOIN o USING (order_id);

CREATE TABLE payments AS
SELECT p.order_id, CAST(payment_sequential AS INTEGER) AS payment_sequential, payment_type,
       CAST(payment_installments AS INTEGER) AS payment_installments,
       CAST(payment_value AS DECIMAL(12, 2)) AS payment_value
FROM raw_payments p JOIN o USING (order_id);

-- dim_customer: a new version whenever a person's order arrives with a different address.
CREATE TABLE ref_dim_customer AS
WITH a AS (
    SELECT person, zip, city_raw AS city, state, purchase,
           lag(zip) OVER w AS pz, lag(city_raw) OVER w AS pc, lag(state) OVER w AS ps
    FROM known WINDOW w AS (PARTITION BY person ORDER BY purchase)),
v AS (
    SELECT person, zip, city, state, purchase AS effective_from FROM a
    WHERE pz IS NULL OR (zip, city, state) IS DISTINCT FROM (pz, pc, ps))
SELECT skey(person || sep() || ts_text(effective_from)) AS customer_key,
       person AS customer_unique_id, zip AS customer_zip_code_prefix, city AS customer_city,
       state AS customer_state, effective_from, 'first_order_seen' AS effective_from_basis,
       lead(effective_from) OVER w AS effective_to, lead(effective_from) OVER w IS NULL AS is_current,
       false AS is_inferred
FROM v WINDOW w AS (PARTITION BY person ORDER BY effective_from);

CREATE TABLE region AS SELECT * FROM (VALUES {regions}) t(state, region);

CREATE TABLE ref_dim_geography AS
WITH places AS (
    SELECT zip, city(city_raw) AS city, state FROM known
    UNION
    SELECT seller_zip_code_prefix, city(seller_city), seller_state FROM raw_sellers)
SELECT skey(zip || sep() || city || sep() || p.state) AS geography_key, zip AS zip_code_prefix,
       city, p.state, region
FROM places p LEFT JOIN region r ON p.state = r.state;

CREATE TABLE ref_dim_seller AS
SELECT skey(seller_id) AS seller_key, seller_id, seller_zip_code_prefix, seller_city, seller_state,
       false AS is_inferred
FROM raw_sellers;

CREATE TABLE ref_dim_product AS
SELECT skey(product_id) AS product_key, product_id,
       NULLIF(p.product_category_name, '') AS product_category_name,
       coalesce(n.product_category_name_english, NULLIF(p.product_category_name, ''), 'unknown')
           AS category,
       CAST(NULLIF(product_weight_g, '') AS INTEGER) AS product_weight_g,
       CAST(NULLIF(product_length_cm, '') AS INTEGER) AS product_length_cm,
       CAST(NULLIF(product_height_cm, '') AS INTEGER) AS product_height_cm,
       CAST(NULLIF(product_width_cm, '') AS INTEGER) AS product_width_cm
FROM raw_products p LEFT JOIN raw_names n USING (product_category_name);

CREATE TABLE ref_fact_order_fulfillment AS
WITH t AS (
    SELECT k.*, s.final_status,
           (SELECT count(*) FROM items i WHERE i.order_id = k.order_id) AS item_count,
           (SELECT sum(price + freight_value) FROM items i WHERE i.order_id = k.order_id) AS items_total,
           (SELECT sum(payment_value) FROM payments p WHERE p.order_id = k.order_id) AS paid,
           (SELECT customer_key FROM ref_dim_customer d
             WHERE d.customer_unique_id = k.person AND d.effective_from <= k.purchase
               AND (d.effective_to IS NULL OR k.purchase < d.effective_to)) AS customer_key
    FROM known k JOIN status s USING (order_id))
SELECT order_id, customer_key, geo_key(zip, city_raw, state) AS delivery_geography_key,
       dkey(purchase) AS purchase_date_key, dkey(approved) AS approved_date_key,
       dkey(carrier) AS carrier_date_key, dkey(delivered) AS delivered_date_key,
       dkey(estimated) AS estimated_delivery_date_key,
       purchase AS order_purchase_timestamp, approved AS order_approved_at,
       carrier AS order_delivered_carrier_date, delivered AS order_delivered_customer_date,
       estimated AS order_estimated_delivery_date,
       days(purchase, approved) AS purchase_to_approval_days,
       CASE WHEN NOT (carrier < approved) AND NOT (carrier < purchase)
            THEN days(approved, carrier) END AS approval_to_carrier_days,
       CASE WHEN NOT (delivered < carrier) AND NOT (carrier < purchase)
            THEN days(carrier, delivered) END AS carrier_to_delivery_days,
       days(purchase, delivered) AS purchase_to_delivery_days,
       date_diff('day', CAST(delivered AS DATE), CAST(estimated AS DATE)) AS days_early,
       CASE WHEN final_status = 'delivered' AND delivered IS NOT NULL
            THEN CAST(delivered AS DATE) > CAST(estimated AS DATE) END AS is_late,
       final_status, final_status IS NULL AS is_open, CAST(item_count AS INTEGER) AS item_count,
       list_filter([
           CASE WHEN carrier < approved THEN 'carrier_before_approval' END,
           CASE WHEN carrier < purchase THEN 'carrier_before_purchase' END,
           CASE WHEN delivered < carrier THEN 'delivered_before_carrier' END,
           CASE WHEN final_status = 'delivered' AND delivered IS NULL THEN 'delivered_without_date' END,
           CASE WHEN final_status = 'delivered' AND approved IS NULL THEN 'delivered_never_approved' END,
           CASE WHEN final_status = 'canceled' AND delivered IS NOT NULL
                THEN 'canceled_with_delivery_date' END,
           CASE WHEN item_count = 0 AND coalesce(final_status NOT IN ('canceled', 'unavailable'), true)
                THEN 'no_items' END,
           CASE WHEN paid IS NULL THEN 'no_payments' END,
           CASE WHEN abs(paid - items_total) > 1 THEN 'payment_mismatch' END], lambda x: x IS NOT NULL)
           AS dq_flags
FROM t;

CREATE TABLE ref_fact_order_items AS
SELECT i.order_id, order_item_id, skey(product_id) AS product_key, skey(i.seller_id) AS seller_key,
       geo_key(s.seller_zip_code_prefix, s.seller_city, s.seller_state) AS seller_geography_key,
       f.delivery_geography_key, f.customer_key, f.purchase_date_key, shipping_limit_date, price,
       freight_value
FROM items i JOIN ref_fact_order_fulfillment f USING (order_id)
LEFT JOIN raw_sellers s ON s.seller_id = i.seller_id;

CREATE TABLE ref_fact_payments AS
SELECT p.order_id, payment_sequential, payment_type, payment_installments, payment_value,
       f.purchase_date_key,
       list_filter([CASE WHEN payment_value = 0 THEN 'zero_value' END,
                    CASE WHEN payment_installments = 0 THEN 'zero_installments' END,
                    CASE WHEN payment_type = 'not_defined' THEN 'type_not_defined' END],
                   lambda x: x IS NOT NULL) AS dq_flags
FROM payments p JOIN ref_fact_order_fulfillment f USING (order_id);

-- A review is known once it is submitted (its answer time); the order's score is the latest by
-- creation date, then answer time, then the highest review_id (spec: "Order review score").
CREATE TABLE ref_fact_reviews AS
WITH r AS (
    SELECT review_id, r.order_id, CAST(review_score AS INTEGER) AS review_score,
           CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
           CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
    FROM raw_reviews r JOIN o USING (order_id)
    WHERE CAST(CAST(review_answer_timestamp AS TIMESTAMP) AS DATE) <= DATE '{cutoff}')
SELECT review_id, order_id, review_score, review_creation_date, review_answer_timestamp,
       dkey(review_creation_date) AS creation_date_key,
       row_number() OVER (PARTITION BY order_id ORDER BY review_creation_date DESC,
                          review_answer_timestamp DESC, review_id DESC) = 1 AS is_order_review
FROM r;

-- Questions 1 and 2 (spec: metric definitions), at measure_spec.py's grain.
CREATE TABLE ref_agg_delivery_by_month_region AS
WITH d AS (
    SELECT strftime(order_delivered_customer_date, '%Y-%m') AS delivery_month, g.region,
           is_late, CAST(days_early AS DOUBLE) AS early
    FROM ref_fact_order_fulfillment f
    LEFT JOIN ref_dim_geography g ON g.geography_key = f.delivery_geography_key
    WHERE final_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
      AND order_purchase_timestamp >= TIMESTAMP '2017-01-01'
      AND order_purchase_timestamp < TIMESTAMP '2018-09-01'),
s AS (SELECT * FROM d UNION ALL SELECT delivery_month, 'all', is_late, early FROM d)
SELECT delivery_month, region, count(*) AS delivered_orders,
       sum(CAST(is_late AS BIGINT)) AS late_orders, avg(CAST(is_late AS DOUBLE)) AS late_rate,
       quantile_cont(early, 0.5) AS median_days_early, quantile_cont(early, 0.1) AS p10_days_early,
       quantile_cont(early, 0.9) AS p90_days_early,
       avg(CAST(early >= 7 AS DOUBLE)) AS share_7_days_early
FROM s GROUP BY delivery_month, region;
"""


def build(olist_dir: Path, start: date, cutoff: date) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB holding ref_<table> for every business table the slice checks."""
    regions = ", ".join(f"('{s}', '{r}')" for r, states in REGIONS.items() for s in states)
    con = duckdb.connect()
    sql = (SQL.replace("{dir}", str(Path(olist_dir).resolve()))
              .replace("{start}", f"{start} 00:00:00").replace("{cutoff}", cutoff.isoformat())
              .replace("{regions}", regions))
    con.execute(sql)
    return con
