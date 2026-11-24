"""Profiles of a run's batch, and the anomaly checks that compare them with history (spec:
DataSentinel, "Checks" and "Anomaly detection").

Each run writes its batch's profile to metrics.profiles. The checks compare today's profile with
the profiles of earlier days that were *published*: a blocked day never becomes part of the
baseline it was judged against. Every decision records its method and baseline.
"""
from collections import defaultdict
from datetime import date, timedelta
from itertools import pairwise

import psycopg
from pyspark.sql import functions as F

from jobs.context import Context, run_id, run_time
from sentinel import config
from sentinel.anomaly.scores import psi, rate_tail, volume_decision, warm_up
from sentinel.checks.batch import TS, batch_rows, ingest_day_lines, silver_events
from sentinel.checks.result import Result

MIN_ROWS_FOR_MIX = 50  # a mix of a handful of rows is noise, not drift
DEFECT_FLAGS = ("carrier_before_approval", "carrier_before_purchase", "delivered_before_carrier",
                "delivered_without_date", "delivered_never_approved",
                "canceled_with_delivery_date", "no_items", "no_payments", "payment_mismatch")
NULL_TABLES = {"orders": ["customer_city", "customer_state", "order_estimated_delivery_date"],
               "order_items": ["product_id", "seller_id", "shipping_limit_date", "freight_value"],
               "order_payments": ["payment_type", "payment_installments"],
               "reviews": ["review_score", "review_creation_date", "review_answer_timestamp"]}


def profile(ctx: Context, business_date: date) -> list[tuple[str, str, str, float]]:
    """(table, column, metric, value) for this run's batch."""
    out = [("bronze.events", "", "lines", float(ingest_day_lines(ctx, business_date).count()))]
    events = silver_events(ctx, business_date)
    # Volume's measure: new orders this run landed itself (not a released batch it took in).
    # Lines are mostly follow-up events for earlier days' orders, so on clean days they swung
    # from 0.49x to 2.24x the usual; orders are demand itself (docs/decisions.md).
    landed = F.lit(run_time(business_date).isoformat(sep=" ")).cast(TS)
    out.append(("silver.events", "", "orders_landed", float(
        events.where((F.col("event_type") == "order_placed") & (F.col("ingested_at") == landed))
        .select("order_id").distinct().count())))
    latest = events.agg(F.max("event_time")).collect()[0][0]
    if latest is not None:
        lag = (run_time(business_date) - latest).total_seconds() / 3600
        out.append(("silver.events", "", "freshness_hours", lag))

    placed = batch_rows(ctx, business_date, "orders").select("order_id").distinct()
    snapshot = ctx.spark.table(ctx.table("gold", "fact_order_fulfillment")).join(placed,
                                                                               "order_id")
    rows = snapshot.count()
    out.append(("gold.fact_order_fulfillment", "", "orders_placed", float(rows)))
    flags = snapshot.select(F.explode("dq_flags").alias("f")).groupBy("f").count().collect()
    counts = {r.f: r["count"] for r in flags}
    out += [("gold.fact_order_fulfillment", f, "defect_count", float(counts.get(f, 0)))
            for f in DEFECT_FLAGS]

    pays = batch_rows(ctx, business_date, "order_payments")
    out += [("silver.order_payments", f"payment_type={r.payment_type}", "mix", float(r["count"]))
            for r in pays.groupBy("payment_type").count().collect()]
    items = batch_rows(ctx, business_date, "order_items")
    edges = config.PRICE_BUCKETS
    bucket = F.lit(None).cast("string")
    for lo, hi in pairwise(edges):
        bucket = F.when((F.col("price") >= lo) & (F.col("price") < hi), F.lit(f"{lo}-{hi}")) \
                  .otherwise(bucket)
    out += [("silver.order_items", f"price={r.b}", "mix", float(r["count"]))
            for r in items.select(bucket.alias("b")).groupBy("b").count().collect()]
    products = ctx.spark.table(ctx.table("gold", "dim_product")).select("product_id", "category")
    out += [("silver.order_items", f"category={r.category}", "mix", float(r["count"]))
            for r in items.join(products, "product_id", "left").groupBy("category")
            .count().collect()]
    reviews = batch_rows(ctx, business_date, "reviews")
    out += [("silver.reviews", f"score={r.review_score}", "mix", float(r["count"]))
            for r in reviews.groupBy("review_score").count().collect()]

    for table, columns in NULL_TABLES.items():
        df = batch_rows(ctx, business_date, table)
        agg = df.agg(F.count("*").alias("_rows"),
                     *(F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns)).collect()[0]
        out.append((f"silver.{table}", "", "rows", float(agg["_rows"])))
        out += [(f"silver.{table}", c, "nulls", float(agg[c] or 0)) for c in columns]
    return out


def save_profile(conn, namespace: str, business_date: date, rows) -> None:
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO metrics.profiles (namespace, run_id, business_date, table_name,
                                          partition_key, column_name, metric, value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (namespace, run_id, table_name, partition_key, column_name, metric)
            DO UPDATE SET value = EXCLUDED.value""",
            [(namespace, run_id(business_date), business_date, t, str(business_date), c, m, v)
             for t, c, m, v in rows])
    conn.commit()


def history_age(conn, namespace: str, business_date: date) -> int:
    """Days since the first published release: how long the checks have had history.

    The spec's warm-up is "until a check has 4 weeks of history". Counting the published days
    inside the trailing window instead meant that, with many days blocked and held, a check
    never left warm-up: in the evaluation's test period, drift scored 2 to 3.7 on price shifts
    and only warned (docs/decisions.md).
    """
    row = conn.execute("""SELECT min(business_date) FROM metrics.releases
                          WHERE namespace = %s AND status = 'published' AND business_date < %s""",
                       (namespace, business_date)).fetchone()
    return (business_date - row[0]).days if row and row[0] else 0


def history(conn, namespace: str, business_date: date, days: int) -> dict:
    """{(table, column, metric): {day: value}} for published days in the trailing window."""
    rows = conn.execute("""
        SELECT p.business_date, p.table_name, p.column_name, p.metric, p.value
        FROM metrics.profiles p
        JOIN metrics.releases r ON r.namespace = p.namespace AND r.run_id = p.run_id
                               AND r.status = 'published'
        WHERE p.namespace = %s AND p.business_date < %s AND p.business_date >= %s""",
        (namespace, business_date, business_date - timedelta(days=days))).fetchall()
    out = defaultdict(dict)
    for d, t, c, m, v in rows:
        out[(t, c, m)][d] = v
    return out


def same_weekday(series: dict, business_date: date, weeks: int, lookback: int) -> list[float]:
    """The latest `weeks` values on the same weekday within `lookback` weeks. Blocked days have
    no published profile, so looking a little further back keeps one block from leaving the
    weekday warn-only for 8 weeks."""
    days = [business_date - timedelta(weeks=n) for n in range(1, lookback + 1)]
    return [series[d] for d in days if d in series][:weeks]


def is_sale_day(ctx: Context, business_date: date) -> bool:
    row = ctx.spark.table(ctx.table("gold", "dim_date")).where(
        F.col("date") == F.lit(business_date)).select("sale_day").collect()
    return bool(row and row[0][0])


def volume_and_freshness(today: dict, past: dict, business_date: date, sale_day: bool):
    out = []
    for key, check, limit, floor, two_sided in (
            (("silver.events", "", "orders_landed"), "volume", config.VOLUME_Z_LIMIT,
             config.VOLUME_MAD_FLOOR, True),
            (("silver.events", "", "freshness_hours"), "freshness", config.FRESHNESS_Z_LIMIT,
             0.0, False)):
        if key not in today:
            continue
        hist = same_weekday(past.get(key, {}), business_date, config.VOLUME_WEEKS,
                            config.VOLUME_LOOKBACK_WEEKS)
        d = volume_decision(today[key], hist, limit, floor)
        if not two_sided and d.method == "modified_z" and d.observed < 0:
            d.status = "pass"  # earlier than usual is never a freshness problem
        # Known sale days widen the bands: a volume alert becomes a warning, not a block.
        # Stale data is published but not as complete, and the dashboards show it (spec: Checks
        # table, Freshness), so freshness never blocks: the release records it instead.
        severity = ("warn" if d.warn_only or check == "freshness"
                    or (sale_day and check == "volume") else "block")
        out.append(Result(key[0], check, d.method, severity, d.status, d.observed, d.expected,
                          str(business_date), {**d.detail, "sale_day": sale_day}))
    return out


def defects(today: dict, past: dict, history_days: int) -> list[Result]:
    """Known-defect counts against the pooled baseline rate: a binomial tail test.

    A day's defects are a count out of that day's orders, a few out of tens or hundreds. A
    modified z on daily rates ignores how many orders each rate rests on, so its median and
    spread jump with single orders (5 of 95 scored 7.3 on the real slice). The binomial tail asks
    the right question: how likely is this count, at the usual rate, out of this many orders?
    """
    rows_key = ("gold.fact_order_fulfillment", "", "orders_placed")
    n = int(today.get(rows_key, 0))
    base_rows_by_day = past.get(rows_key, {})
    days = [d for d in base_rows_by_day if base_rows_by_day[d] > 0]
    base_rows = int(sum(base_rows_by_day[d] for d in days))
    out = []
    for flag in DEFECT_FLAGS:
        key = ("gold.fact_order_fulfillment", flag, "defect_count")
        c = int(today.get(key, 0))
        series = past.get(key, {})
        base_count = int(sum(series.get(d, 0) for d in days))
        if n and base_rows:
            tail, p = rate_tail(c, n, base_count, base_rows)
            status, method, observed = ("fail" if tail < config.RATE_TAIL_LIMIT else "pass",
                                        "rate_limit", tail)
            expected = f"p = {p:.5f} over {base_rows} rows; tail >= {config.RATE_TAIL_LIMIT}"
        else:
            status, method, observed, expected = "pass", "warm_up", float(c), "no baseline yet"
        severity = "warn" if warm_up(history_days) else "block"
        out.append(Result("gold.fact_order_fulfillment", f"known_defect:{flag}", method,
                          severity, status, observed, expected,
                          detail={"count": c, "orders": n, "baseline_defects": base_count}))
    return out


def drift(today: dict, past: dict, history_days: int) -> list[Result]:
    out = []
    for table, prefix in (("silver.order_payments", "payment_type"), ("silver.order_items", "price"),
                          ("silver.order_items", "category"), ("silver.reviews", "score")):
        now = {c: v for (t, c, m), v in today.items()
               if t == table and m == "mix" and c.startswith(prefix + "=")}
        base = defaultdict(float)
        days = set()
        for (t, c, m), series in past.items():
            if t == table and m == "mix" and c.startswith(prefix + "="):
                for d, v in series.items():
                    base[c] += v
                    days.add(d)
        if sum(now.values()) < MIN_ROWS_FOR_MIX or not base:
            out.append(Result(table, f"drift:{prefix}", "too_few_rows", "warn", "pass",
                              sum(now.values()), f">= {MIN_ROWS_FOR_MIX} rows and a baseline"))
            continue
        value = psi(base, now, config.PSI_SMOOTHING)
        warn, block = config.DRIFT_WARN[prefix], config.DRIFT_BLOCK.get(prefix)
        blocks = block is not None and value >= block and not warm_up(history_days)
        status = "fail" if value >= warn else "pass"
        limits = f"PSI < {warn}" + (f", blocks from {block}" if block else ", warn only")
        out.append(Result(table, f"drift:{prefix}", "psi", "block" if blocks else "warn", status,
                          round(value, 4), f"{limits}, against {len(days)} days"))
    return out


def null_rates(today: dict, past: dict) -> list[Result]:
    out = []
    for table, columns in NULL_TABLES.items():
        t = f"silver.{table}"
        rows = today.get((t, "", "rows"), 0)
        base_rows = sum(past.get((t, "", "rows"), {}).values())
        for c in columns:
            if rows < MIN_ROWS_FOR_MIX or not base_rows:
                continue
            share = today.get((t, c, "nulls"), 0) / rows
            base = sum(past.get((t, c, "nulls"), {}).values()) / base_rows
            out.append(Result(t, f"null_rate:{c}", "share_change", "warn",
                              "fail" if abs(share - base) > config.NULL_RATE_WARN else "pass",
                              round(share, 4), f"baseline {base:.4f} +/- {config.NULL_RATE_WARN}"))
    return out


def anomaly_checks(ctx: Context, conn: psycopg.Connection, business_date: date) -> list[Result]:
    rows = profile(ctx, business_date)
    save_profile(conn, ctx.namespace_prefix, business_date, rows)
    today = {(t, c, m): v for t, c, m, v in rows}
    days = max(config.BASELINE_DAYS, config.VOLUME_LOOKBACK_WEEKS * 7)
    past = history(conn, ctx.namespace_prefix, business_date, days)
    age = history_age(conn, ctx.namespace_prefix, business_date)
    recent = {k: {d: v for d, v in s.items()
                  if d >= business_date - timedelta(days=config.BASELINE_DAYS)}
              for k, s in past.items()}
    return (volume_and_freshness(today, past, business_date, is_sale_day(ctx, business_date))
            + defects(today, recent, age) + drift(today, recent, age)
            + null_rates(today, recent))
