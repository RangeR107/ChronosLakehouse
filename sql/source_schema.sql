-- The operational source system: customer, seller and product master (spec: "Source database").
-- Lives in the `source` schema of the chronos database; `metrics` and `serving` come later.
-- Safe to run repeatedly: every statement creates only what is missing or replaces a function.

CREATE SCHEMA IF NOT EXISTS source;

-- One row, advanced by the generator: "now" on the replay's simulated clock.
-- The boolean key with CHECK makes a second row impossible, not just unlikely.
CREATE TABLE IF NOT EXISTS source.replay_clock (
    one    boolean PRIMARY KEY DEFAULT true CHECK (one),
    now_ts timestamp NOT NULL
);

-- One row per person. address_effective_from is business time (the purchase time of the first
-- order seen with this address); updated_at is when the row reached the database. Never mix them.
CREATE TABLE IF NOT EXISTS source.customers (
    customer_unique_id       text PRIMARY KEY,
    customer_zip_code_prefix text NOT NULL,   -- text: Olist's prefixes are codes, not numbers
    customer_city            text NOT NULL,
    customer_state           text NOT NULL,
    address_effective_from   timestamp NOT NULL,
    created_at               timestamp NOT NULL,
    updated_at               timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS source.sellers (
    seller_id              text PRIMARY KEY,
    seller_zip_code_prefix text NOT NULL,
    seller_city            text NOT NULL,
    seller_state           text NOT NULL,
    created_at             timestamp NOT NULL,
    updated_at             timestamp NOT NULL
);

-- Olist's product name and description lengths and photo count are left out: nothing uses them.
CREATE TABLE IF NOT EXISTS source.products (
    product_id            text PRIMARY KEY,
    product_category_name text,               -- 610 products have none
    product_weight_g      integer,
    product_length_cm     integer,
    product_height_cm     integer,
    product_width_cm      integer,
    created_at            timestamp NOT NULL,
    updated_at            timestamp NOT NULL
);

-- updated_at must come from the database, not from the writer: extraction filters on it, and a
-- late update must look new even though its business time is old. A column default would not
-- do, because PostgreSQL applies defaults only on INSERT; a BEFORE trigger fires on both.
CREATE OR REPLACE FUNCTION source.set_replay_timestamps() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := (SELECT now_ts FROM source.replay_clock);
    IF NEW.updated_at IS NULL THEN
        RAISE EXCEPTION 'source.replay_clock is empty: set the replay clock before writing';
    END IF;
    IF TG_OP = 'INSERT' THEN
        NEW.created_at := NEW.updated_at;
    ELSE
        NEW.created_at := OLD.created_at;  -- a row is created once
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS customers_replay_timestamps ON source.customers;
CREATE TRIGGER customers_replay_timestamps BEFORE INSERT OR UPDATE ON source.customers
    FOR EACH ROW EXECUTE FUNCTION source.set_replay_timestamps();

DROP TRIGGER IF EXISTS sellers_replay_timestamps ON source.sellers;
CREATE TRIGGER sellers_replay_timestamps BEFORE INSERT OR UPDATE ON source.sellers
    FOR EACH ROW EXECUTE FUNCTION source.set_replay_timestamps();

DROP TRIGGER IF EXISTS products_replay_timestamps ON source.products;
CREATE TRIGGER products_replay_timestamps BEFORE INSERT OR UPDATE ON source.products
    FOR EACH ROW EXECUTE FUNCTION source.set_replay_timestamps();

-- extract_dimensions reads "updated_at later than the high-water mark minus one day".
CREATE INDEX IF NOT EXISTS customers_updated_at ON source.customers (updated_at);
CREATE INDEX IF NOT EXISTS sellers_updated_at ON source.sellers (updated_at);
CREATE INDEX IF NOT EXISTS products_updated_at ON source.products (updated_at);
