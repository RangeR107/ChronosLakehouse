"""Reference implementation of the Chronos metrics (questions 1 to 7), straight from the Olist files.

Usage:  python measure_spec.py <folder with the Olist CSV files> [output folder, default ./measure_out]

It prints headline numbers and writes every breakdown the spec defines to CSV files, one per question,
at the metric's own reporting grain. The metric regression test compares gold with these files.
Question 8 (restatements) needs the replay's publication history, so only the pipeline can produce it.

Numbers in the spec were measured on a GitHub mirror whose reviews file has 100,000 rows; Kaggle's
current file has 99,224, so review-based numbers may differ slightly on your download.

Definitions (see "Business questions and metric definitions" in the spec):
- Reliable window: purchases from 2017-01-01 up to, not including, 2018-09-01.
- Final status: delivered, canceled or unavailable. Every other status is open.
- Qualifying purchase (question 5): any order whose status is not canceled or unavailable.
- Order review: the latest review by creation date, then answer time, then review_id.
- CUME_DIST: rank(method="max", pct=True) on revenue, highest first, over every category with revenue.
- PERCENT_RANK: (rank(method="min") - 1) / (n - 1), on late rate, lowest first.
"""
import os
import sys

import numpy as np
import pandas as pd

START, END = pd.Timestamp("2017-01-01"), pd.Timestamp("2018-09-01")  # END is exclusive
FINAL = {"delivered", "canceled", "unavailable"}
NOT_A_SALE = {"canceled", "unavailable"}
REGION = {  # IBGE major regions
    **dict.fromkeys(["AC", "AP", "AM", "PA", "RO", "RR", "TO"], "North"),
    **dict.fromkeys(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"], "Northeast"),
    **dict.fromkeys(["DF", "GO", "MT", "MS"], "Central-West"),
    **dict.fromkeys(["ES", "MG", "RJ", "SP"], "Southeast"),
    **dict.fromkeys(["PR", "RS", "SC"], "South"),
}


def load(d):
    r = lambda n: pd.read_csv(f"{d}/{n}.csv")
    o = r("olist_orders_dataset")
    for c in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
              "order_delivered_customer_date", "order_estimated_delivery_date"]:
        o[c] = pd.to_datetime(o[c])
    it = r("olist_order_items_dataset")
    it["shipping_limit_date"] = pd.to_datetime(it["shipping_limit_date"])
    rv = r("olist_order_reviews_dataset")
    for c in ["review_creation_date", "review_answer_timestamp"]:
        rv[c] = pd.to_datetime(rv[c])
    return (o, it, r("olist_order_payments_dataset"), rv, r("olist_customers_dataset"),
            r("olist_products_dataset"), r("olist_sellers_dataset"))


def pct(a, b):
    return f"{a:,} of {b:,} ({a / b * 100:.2f}%)"


def in_window(ts):
    return (ts >= START) & (ts < END)


def order_reviews(rv):
    """One review per order: latest creation date, then latest answer time, then highest review_id."""
    s = rv.sort_values(["order_id", "review_creation_date", "review_answer_timestamp", "review_id"])
    return s.groupby("order_id").tail(1)[["order_id", "review_id", "review_score"]]


def wilson_lower(p, n, z=1.96):
    return (p + z**2 / (2 * n) - z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n)) / (1 + z**2 / n)


def save(df, out, name):
    df.to_csv(os.path.join(out, name), index=False)


def delivery_stats(g):
    early = (g.order_estimated_delivery_date.dt.normalize() - g.order_delivered_customer_date.dt.normalize()).dt.days
    return pd.Series({"delivered_orders": len(g), "late_orders": int(g.late.sum()), "late_rate": g.late.mean(),
                      "median_days_early": early.median(), "p10_days_early": early.quantile(.1),
                      "p90_days_early": early.quantile(.9), "share_7_days_early": (early >= 7).mean()})


def main(d, out):
    os.makedirs(out, exist_ok=True)
    o, it, pay, rv, cu, pr, se = load(d)
    o = o.merge(cu[["customer_id", "customer_unique_id", "customer_state"]], on="customer_id", how="left")
    o["region"] = o.customer_state.map(REGION)
    ow = o[in_window(o.order_purchase_timestamp)]
    orev = order_reviews(rv)
    itp = it.merge(pr[["product_id", "product_category_name"]], on="product_id", how="left")
    itp["cat"] = itp.product_category_name.fillna("unknown")
    ncat = itp.groupby("order_id").cat.nunique()

    # Q1 and Q2: delivery month x delivery region, plus an "all" slice
    od = ow[(ow.order_status == "delivered") & ow.order_delivered_customer_date.notna()].copy()
    od["late"] = od.order_delivered_customer_date.dt.normalize() > od.order_estimated_delivery_date.dt.normalize()
    od["delivery_month"] = od.order_delivered_customer_date.dt.to_period("M").astype(str)
    by_region = od.groupby(["delivery_month", "region"]).apply(delivery_stats).reset_index()
    by_all = od.groupby("delivery_month").apply(delivery_stats).reset_index().assign(region="all")
    save(pd.concat([by_region, by_all]).sort_values(["delivery_month", "region"]), out, "q1_q2_delivery_by_month_region.csv")
    print("== Q1, Q2 (orders purchased in the window)")
    print("late rate:", pct(int(od.late.sum()), len(od)))
    s = delivery_stats(od)
    print(f"days early: median {s.median_days_early:.0f}, p10 {s.p10_days_early:.0f}, p90 {s.p90_days_early:.0f},"
          f" share at least 7 days early {s.share_7_days_early * 100:.1f}%")

    # Q4: delivery month x late/on-time group
    m = od.merge(orev, on="order_id")
    m["group"] = np.where(m.late, "late", "on_time")
    q4 = m.groupby(["delivery_month", "group"]).agg(orders=("review_score", "size"), mean_score=("review_score", "mean"),
                                                    one_star_share=("review_score", lambda x: (x == 1).mean())).reset_index()
    save(q4, out, "q4_satisfaction_by_month.csv")
    print("== Q4")
    for grp, g in m.groupby("group"):
        print(f"  {grp}: n={len(g):,}, mean score {g.review_score.mean():.2f}, 1-star {(g.review_score == 1).mean() * 100:.1f}%")

    # Q3: seller x shipping-limit month, ranks and consistency (needs 6 ranked months)
    ns = it.groupby("order_id").seller_id.nunique()
    ss = it[it.order_id.isin(ns[ns == 1].index)].merge(ow, on="order_id")
    ss = ss[ss.order_delivered_carrier_date.notna() & (ss.order_delivered_carrier_date >= ss.order_purchase_timestamp)].copy()
    ss["late"] = ss.order_delivered_carrier_date > ss.shipping_limit_date
    ss["month"] = ss.shipping_limit_date.dt.to_period("M")
    sm = ss.groupby(["month", "seller_id"]).agg(items=("late", "size"), late_items=("late", "sum")).reset_index()
    sm = sm[sm["items"] >= 20].copy()
    sm["late_rate"] = sm.late_items / sm["items"]
    sm = sm[sm.groupby("month").seller_id.transform("size") >= 20].copy()
    r = sm.groupby("month").late_rate.rank(method="min")
    n = sm.groupby("month").late_rate.transform("size")
    sm["percent_rank"] = (r - 1) / (n - 1)
    sm["worst_decile"] = sm.percent_rank >= 0.9
    sm = sm.sort_values(["seller_id", "month"])
    grp = sm.groupby("seller_id").worst_decile
    sm["ranked_months_so_far"] = sm.groupby("seller_id").cumcount() + 1
    sm["worst_in_last_6"] = grp.transform(lambda x: x.rolling(6, min_periods=1).sum()).astype(int)
    sm["classification"] = np.select(
        [sm.ranked_months_so_far < 6, sm.worst_in_last_6 >= 3], ["not_enough_history", "consistently_late"], "ok")
    sm["month"] = sm.month.astype(str)
    save(sm, out, "q3_seller_months.csv")
    counts = sm.groupby("month").size()
    last = sm.month.max()
    provisional = sm[(sm.classification == "not_enough_history") & (sm.worst_in_last_6 >= 3)].seller_id.nunique()
    print("== Q3")
    print(f"items handed over late: {pct(int(ss.late.sum()), len(ss))};"
          f" by calendar day {(ss.order_delivered_carrier_date.dt.normalize() > ss.shipping_limit_date.dt.normalize()).mean() * 100:.2f}%")
    print(f"ranked months: {len(counts)} ({counts.index.min()} to {counts.index.max()}), sellers per ranked month:"
          f" median {counts.median():.0f}, min {counts.min()}")
    print("sellers ever classified consistently late (6 ranked months required):",
          sm[sm.classification == "consistently_late"].seller_id.nunique(),
          f"| in {last}:", sm[(sm.month == last) & (sm.classification == "consistently_late")].seller_id.nunique(),
          "| would qualify with fewer than 6 ranked months (not classified):", provisional)

    # Q5: cohort month, 90- and 180-day repeat; full window must end by END (inclusive boundary)
    oc = o[~o.order_status.isin(NOT_A_SALE)]
    first = oc.groupby("customer_unique_id").order_purchase_timestamp.min().rename("first")
    existing = int((first < START).sum())
    cohort = first[in_window(first)].to_frame()
    cohort["cohort_month"] = cohort["first"].dt.to_period("M").astype(str)
    oc = oc.merge(first, left_on="customer_unique_id", right_index=True)
    rows = []
    for days in (90, 180):
        rep = oc[(oc.order_purchase_timestamp > oc["first"]) &
                 (oc.order_purchase_timestamp <= oc["first"] + pd.Timedelta(days=days))].customer_unique_id.unique()
        cohort[f"eligible_{days}"] = cohort["first"] + pd.Timedelta(days=days) <= END
        cohort[f"repeat_{days}"] = cohort.index.isin(rep) & cohort[f"eligible_{days}"]
    q5 = cohort.groupby("cohort_month").agg(people=("first", "size"),
                                            eligible_90=("eligible_90", "sum"), repeat_90=("repeat_90", "sum"),
                                            eligible_180=("eligible_180", "sum"), repeat_180=("repeat_180", "sum")).reset_index()
    for days in (90, 180):
        q5[f"rate_{days}"] = (q5[f"repeat_{days}"] / q5[f"eligible_{days}"].where(q5[f"eligible_{days}"] > 0))
        q5[f"complete_{days}"] = q5[f"eligible_{days}"] == q5.people
    save(q5, out, "q5_retention_by_cohort.csv")
    print("== Q5")
    print(f"people whose first qualifying purchase is before 2017 (no cohort): {existing:,}")
    for days in (90, 180):
        print(f"repeat within {days} days:", pct(int(cohort[f'repeat_{days}'].sum()), int(cohort[f'eligible_{days}'].sum())))

    # Q6: purchase month
    appr = ow[ow.order_approved_at.notna()].copy()
    appr["purchase_month"] = appr.order_purchase_timestamp.dt.to_period("M").astype(str)
    appr["final"] = appr.order_status.isin(FINAL)
    q6 = appr.groupby("purchase_month").agg(
        approved_final=("final", "sum"),
        canceled=("order_status", lambda x: (x == "canceled").sum()),
        unavailable=("order_status", lambda x: (x == "unavailable").sum()),
        open_left_out=("final", lambda x: (~x).sum())).reset_index()
    q6["canceled_rate"] = q6.canceled / q6.approved_final
    q6["unavailable_rate"] = q6.unavailable / q6.approved_final
    save(q6, out, "q6_cancellations_by_month.csv")
    fin = appr[appr.final]
    print("== Q6")
    print("canceled:", pct(int((fin.order_status == "canceled").sum()), len(fin)),
          "| unavailable:", pct(int((fin.order_status == "unavailable").sum()), len(fin)),
          f"| open approved orders left out: {int((~appr.final).sum()):,}")

    # Q7: purchase quarter x category. Rank all categories with revenue first, then join review stats.
    sale = ow[~ow.order_status.isin(NOT_A_SALE)][["order_id", "order_purchase_timestamp"]]
    sale = sale.assign(quarter=sale.order_purchase_timestamp.dt.to_period("Q").astype(str))
    items = itp.merge(sale, on="order_id")
    single = items[items.order_id.isin(ncat[ncat == 1].index)].groupby(["order_id", "quarter"]).cat.first().reset_index()
    rated = single.merge(orev, on="order_id")
    rated["low"] = rated.review_score <= 2

    def category_table(item_rows, rated_rows):
        t = item_rows.groupby("cat").price.sum().rename("revenue").to_frame()
        t["cume_dist"] = t.revenue.rank(method="max", pct=True, ascending=False)
        t["top_20pct"] = t.cume_dist <= 0.2
        s = rated_rows.groupby("cat").agg(reviewed_orders=("low", "size"), low_share=("low", "mean"),
                                          mean_score=("review_score", "mean"))
        t = t.join(s, how="left")
        t["reviewed_orders"] = t.reviewed_orders.fillna(0).astype(int)
        overall = rated_rows.low.mean()
        t["all_category_low_share"] = overall
        t["wilson_lower"] = np.where(t.reviewed_orders > 0, wilson_lower(t.low_share, t.reviewed_orders.clip(lower=1)), np.nan)
        t["flag"] = t.top_20pct & (t.reviewed_orders > 0) & (t.wilson_lower > overall)
        return t.reset_index().rename(columns={"index": "cat"}), overall

    parts = []
    print("== Q7 (window's last quarter, 2018Q3, holds only July and August)")
    for q in sorted(items.quarter.unique()):
        t, overall = category_table(items[items.quarter == q], rated[rated.quarter == q])
        parts.append(t.assign(quarter=q))
        names = sorted(t[t.flag].cat)
        print(f"  {q}: {len(t)} categories, {int(t.top_20pct.sum())} in top 20%, all-category low share"
              f" {overall * 100:.1f}%, flagged {len(names)}: {', '.join(names) if names else '-'}")
    save(pd.concat(parts), out, "q7_category_by_quarter.csv")
    t, overall = category_table(items, rated)
    top = t[t.top_20pct]
    print(f"whole window: {len(top)} of {len(t)} categories in top 20%; mean score {top.mean_score.min():.2f} to"
          f" {top.mean_score.max():.2f}; low share {top.low_share.min() * 100:.1f}% to {top.low_share.max() * 100:.1f}%;"
          f" all-category low share {overall * 100:.1f}%; flagged {int(top.flag.sum())}")

    print("== Descriptive facts (whole dataset)")
    ipo = it.groupby("order_id").size()
    nprod = it.groupby("order_id").product_id.nunique()
    print("single-item orders:", pct(int((ipo == 1).sum()), len(ipo)), "| single-product:", pct(int((nprod == 1).sum()), len(nprod)),
          "| single-category:", pct(int((ncat == 1).sum()), len(ncat)), "| multi-category:", int((ncat > 1).sum()))
    rpo = rv.groupby("order_id").size()
    multi = rv[rv.order_id.isin(rpo[rpo > 1].index)]
    tied = multi.groupby("order_id").review_creation_date.agg(lambda x: (x == x.max()).sum() > 1).sum()
    print("orders with >1 review:", int((rpo > 1).sum()), "| tied on latest creation date:", int(tied),
          "| review_ids on >1 order:", int((rv.groupby("review_id").order_id.nunique() > 1).sum()))
    na = o[o.order_approved_at.isna()]
    print("orders without approval:", len(na), na.order_status.value_counts().to_dict(),
          "| with payment rows:", int(na.order_id.isin(pay.order_id).sum()),
          "| orders with no payment rows:", int((~o.order_id.isin(pay.order_id)).sum()))
    print(f"CSV breakdowns written to {os.path.abspath(out)}")


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit("usage: python measure_spec.py <olist csv folder> [output folder]")
    main(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "measure_out")
