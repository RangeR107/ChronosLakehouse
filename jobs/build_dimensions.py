"""build_dimensions: dim_customer (SCD Type 2), dim_geography, dim_seller, dim_product and dim_date
(spec: Data model; chronos_daily step 7).

Each dimension is recomputed from everything silver and bronze hold up to the run's replay time,
then MERGEd into gold on its key: new rows are inserted, changed rows updated, unchanged rows left
alone. At about 100,000 people a full recompute is cheap, and it is what makes a late dimension
change (an address version older than ones already loaded) land in the right place: the version
chain is simply derived again.
"""
from datetime import date, timedelta
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from jobs.context import Context, run_time
from jobs.keys import surrogate_key

TS = "timestamp_ntz"

IBGE_REGION = {  # Brazil's five major regions (spec: Data sources, Region mapping)
    **dict.fromkeys(["AC", "AP", "AM", "PA", "RO", "RR", "TO"], "North"),
    **dict.fromkeys(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"], "Northeast"),
    **dict.fromkeys(["DF", "GO", "MT", "MS"], "Central-West"),
    **dict.fromkeys(["ES", "MG", "RJ", "SP"], "Southeast"),
    **dict.fromkeys(["PR", "RS", "SC"], "South"),
}

# Known sale days, so volume checks can widen their bands instead of raising false alarms
# (spec: DataSentinel, "Known sale days"). Black Friday in Brazil.
SALE_DAYS = {date(2016, 11, 25): "black_friday", date(2017, 11, 24): "black_friday",
             date(2018, 11, 23): "black_friday"}
CALENDAR = (date(2016, 1, 1), date(2019, 12, 31))  # covers every Olist date, estimates included

ACCENTS, PLAIN = "áàâãäéèêëíìîïóòôõöúùûüç´`", "aaaaaeeeeiiiiooooouuuuc''"

TABLES = {
    "dim_customer": """
        customer_key BIGINT, customer_unique_id STRING, customer_zip_code_prefix STRING,
        customer_city STRING, customer_state STRING,
        effective_from TIMESTAMP_NTZ, effective_from_basis STRING,
        effective_to TIMESTAMP_NTZ COMMENT 'exclusive; null for the current version',
        is_current BOOLEAN, is_inferred BOOLEAN""",
    "dim_geography": """
        geography_key BIGINT, zip_code_prefix STRING, city STRING, state STRING, region STRING""",
    "dim_seller": """
        seller_key BIGINT, seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING,
        seller_state STRING, is_inferred BOOLEAN""",
    "dim_product": """
        product_key BIGINT, product_id STRING, product_category_name STRING,
        category STRING COMMENT 'English; unknown when Olist has none; Portuguese when untranslated',
        product_weight_g INT, product_length_cm INT, product_height_cm INT, product_width_cm INT""",
    "dim_date": """
        date_key INT, date DATE, year INT, quarter INT, month INT, day INT, weekday STRING,
        is_weekend BOOLEAN, sale_day STRING""",
}
KEYS = {"dim_customer": "customer_key", "dim_geography": "geography_key",
        "dim_seller": "seller_key", "dim_product": "product_key", "dim_date": "date_key"}


def ensure_gold_tables(ctx: Context) -> None:
    ctx.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ctx.namespace('gold')}")
    for name, columns in TABLES.items():
        ctx.spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctx.table('gold', name)} ({columns})
                          USING iceberg TBLPROPERTIES ('format-version' = '2')""")


def normalise_city(c: Column) -> Column:
    """Lower case, no accents, single spaces: 'São Paulo ' and 'sao paulo' are one place."""
    return F.translate(F.lower(F.trim(F.regexp_replace(c, r"\s+", " "))), ACCENTS, PLAIN)


def known_by(ctx: Context, layer_table: str, business_date: date) -> DataFrame:
    """A bronze extract as the run for business_date could see it."""
    upper = F.lit(run_time(business_date).isoformat(sep=" ")).cast(TS)
    return ctx.spark.table(ctx.table("bronze", layer_table)).where(F.col("extracted_at") <= upper)


def latest_per_key(df: DataFrame, key: str) -> DataFrame:
    """Type 1: the most recent state of each row wins."""
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc())
    return df.withColumn("_n", F.row_number().over(w)).where("_n = 1").drop("_n")


def customer_versions(ctx: Context, business_date: date) -> DataFrame:
    """SCD Type 2 from the master's address history.

    Each state of a master row carries address_effective_from, the purchase time of the first
    order seen with that address: business time, never updated_at. A new version starts when zip,
    city or state changes (252 people get one on the full data). The dates are inferred from
    orders, which effective_from_basis records.
    """
    rows = known_by(ctx, "customers_extract", business_date).select(
        "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
        F.col("address_effective_from").alias("effective_from")).distinct()
    chain = Window.partitionBy("customer_unique_id").orderBy("effective_from")
    return rows.select(
        surrogate_key(F.col("customer_unique_id"),
                      F.col("effective_from").cast("string")).alias("customer_key"),
        "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
        "effective_from", F.lit("first_order_seen").alias("effective_from_basis"),
        F.lead("effective_from").over(chain).alias("effective_to"),
        F.lead("effective_from").over(chain).isNull().alias("is_current"),
        F.lit(False).alias("is_inferred"))


def geographies(ctx: Context, business_date: date) -> DataFrame:
    """Every location seen: each order's delivery address and each seller's address."""
    delivery = ctx.spark.table(ctx.table("silver", "orders")).select(
        F.col("customer_zip_code_prefix").alias("zip"), F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"))
    sellers = known_by(ctx, "sellers_extract", business_date).select(
        F.col("seller_zip_code_prefix").alias("zip"), F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"))
    regions = F.create_map(*(F.lit(x) for kv in IBGE_REGION.items() for x in kv))
    places = (delivery.unionByName(sellers)
              .select("zip", normalise_city(F.col("city")).alias("city"), "state").distinct())
    return places.select(
        geography_key("zip", "city", "state").alias("geography_key"),
        F.col("zip").alias("zip_code_prefix"), "city", "state",
        regions[F.col("state")].alias("region"))


def geography_key(zip_col: str, city_col: str, state_col: str):
    """The key a fact uses to point at a location; the city is normalised first."""
    return surrogate_key(F.col(zip_col), normalise_city(F.col(city_col)), F.col(state_col))


def sellers(ctx: Context, business_date: date) -> DataFrame:
    s = latest_per_key(known_by(ctx, "sellers_extract", business_date), "seller_id")
    return s.select(surrogate_key(F.col("seller_id")).alias("seller_key"), "seller_id",
                    "seller_zip_code_prefix", "seller_city", "seller_state",
                    F.lit(False).alias("is_inferred"))


def products(ctx: Context, business_date: date) -> DataFrame:
    p = latest_per_key(known_by(ctx, "products_extract", business_date), "product_id")
    # Olist's translation table is static reference data delivered with the source, read as is.
    names = ctx.spark.read.option("header", True).csv(
        str(ctx.reference_dir / "product_category_name_translation.csv"))
    return (p.join(names, "product_category_name", "left").select(
        surrogate_key(F.col("product_id")).alias("product_key"), "product_id",
        "product_category_name",
        # 610 products have no category; 2 categories have no English name and keep Portuguese.
        F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown"))
         .alias("category"),
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"))


def calendar(ctx: Context) -> DataFrame:
    first, last = CALENDAR
    days = [first + timedelta(n) for n in range((last - first).days + 1)]
    rows = [(int(d.strftime("%Y%m%d")), d, d.year, (d.month - 1) // 3 + 1, d.month, d.day,
             d.strftime("%A"), d.weekday() >= 5, SALE_DAYS.get(d)) for d in days]
    return ctx.spark.createDataFrame(rows, "date_key int, date date, year int, quarter int,"
                                           " month int, day int, weekday string,"
                                           " is_weekend boolean, sale_day string")


def merge_dimension(ctx: Context, name: str, rows: DataFrame) -> int:
    """Insert new keys, update rows whose values changed, leave the rest untouched.

    Comparing values first means an unchanged rerun commits nothing, so reruns stay idempotent
    and the table's history only shows real changes. Returns the rows inserted or updated.
    """
    table, key = ctx.table("gold", name), KEYS[name]
    s, t = rows.alias("s"), ctx.spark.table(table).alias("t")
    # Null-safe: a current version's effective_to is null, and with a plain equality null never
    # equals null, so every current version would look changed on every rerun.
    same = reduce(lambda a, b: a & b, [F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
                                       for c in rows.columns])
    changed = s.join(t, same, "left_anti").localCheckpoint()
    count = changed.count()
    if count:
        view = f"_changed_{name}"
        changed.createOrReplaceTempView(view)
        ctx.spark.sql(f"""MERGE INTO {table} t USING {view} s ON t.{key} = s.{key}
                          WHEN MATCHED THEN UPDATE SET *
                          WHEN NOT MATCHED THEN INSERT *""")
    return count


def build_dimensions(ctx: Context, business_date: date) -> dict[str, int]:
    """Rows inserted or updated per dimension."""
    ensure_gold_tables(ctx)
    return {
        "dim_date": merge_dimension(ctx, "dim_date", calendar(ctx)),
        "dim_geography": merge_dimension(ctx, "dim_geography", geographies(ctx, business_date)),
        "dim_seller": merge_dimension(ctx, "dim_seller", sellers(ctx, business_date)),
        "dim_product": merge_dimension(ctx, "dim_product", products(ctx, business_date)),
        "dim_customer": merge_dimension(ctx, "dim_customer",
                                        customer_versions(ctx, business_date)),
    }
