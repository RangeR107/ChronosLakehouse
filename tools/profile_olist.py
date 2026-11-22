"""Profile the Olist dataset to back the Chronos spec with measured facts.
Usage: python tools/profile_olist.py <dir_with_csvs>
"""
import sys
from pathlib import Path
import pandas as pd

d = Path(sys.argv[1])
def rd(name, **kw):
    return pd.read_csv(d / name, **kw)

orders = rd("olist_orders_dataset.csv")
items = rd("olist_order_items_dataset.csv")
pays = rd("olist_order_payments_dataset.csv")
revs = rd("olist_order_reviews_dataset.csv")
cust = rd("olist_customers_dataset.csv")
prods = rd("olist_products_dataset.csv")
sellers = rd("olist_sellers_dataset.csv")
trans = rd("product_category_name_translation.csv")

print("== ROW COUNTS")
for n, df in [("orders", orders), ("items", items), ("payments", pays), ("reviews", revs),
              ("customers", cust), ("products", prods), ("sellers", sellers), ("translation", trans)]:
    print(f"{n}: {len(df):,} rows, cols={list(df.columns)}")

ts_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
           "order_delivered_customer_date", "order_estimated_delivery_date"]
for c in ts_cols:
    orders[c] = pd.to_datetime(orders[c])

print("\n== ORDERS")
print("unique order_id:", orders.order_id.nunique())
print("purchase range:", orders.order_purchase_timestamp.min(), "->", orders.order_purchase_timestamp.max())
print("status counts:\n", orders.order_status.value_counts().to_string())
print("null counts in milestones:\n", orders[ts_cols].isna().sum().to_string())

dl = orders[orders.order_status == "delivered"]
print("\ndelivered orders:", len(dl))
print("delivered but missing delivered_customer_date:", dl.order_delivered_customer_date.isna().sum())
print("delivered but missing approved_at:", dl.order_approved_at.isna().sum())
print("delivered but missing carrier_date:", dl.order_delivered_carrier_date.isna().sum())
has = dl.dropna(subset=["order_delivered_customer_date"])
late = (has.order_delivered_customer_date.dt.normalize() > has.order_estimated_delivery_date.dt.normalize())
print(f"late vs estimate (date-level): {late.sum():,} of {len(has):,} = {late.mean():.2%}")
lag = (has.order_delivered_customer_date - has.order_purchase_timestamp).dt.total_seconds() / 86400
print("purchase->delivered days: median %.1f, p90 %.1f, p99 %.1f, max %.1f" % (lag.median(), lag.quantile(.9), lag.quantile(.99), lag.max()))

o = orders
print("\n== TIMESTAMP ORDER VIOLATIONS (all orders)")
print("approved < purchase:", (o.order_approved_at < o.order_purchase_timestamp).sum())
print("carrier < approved:", (o.order_delivered_carrier_date < o.order_approved_at).sum())
print("carrier < purchase:", (o.order_delivered_carrier_date < o.order_purchase_timestamp).sum())
print("delivered < carrier:", (o.order_delivered_customer_date < o.order_delivered_carrier_date).sum())
print("delivered < purchase:", (o.order_delivered_customer_date < o.order_purchase_timestamp).sum())
canc = o[o.order_status == "canceled"]
print("canceled orders:", len(canc), "| with approved_at:", canc.order_approved_at.notna().sum(),
      "| with carrier date:", canc.order_delivered_carrier_date.notna().sum(),
      "| with delivered date:", canc.order_delivered_customer_date.notna().sum())

print("\n== DAILY VOLUME")
daily = o.groupby(o.order_purchase_timestamp.dt.date).size()
d17 = daily[(pd.to_datetime(daily.index) >= "2017-06-01") & (pd.to_datetime(daily.index) < "2018-06-01")]
print("days with orders:", len(daily), "| Jun2017-May2018 median/day:", d17.median(), "mean:", round(d17.mean(), 1))
print("top 5 days:\n", daily.sort_values(ascending=False).head(5).to_string())
wd = o.groupby(o.order_purchase_timestamp.dt.day_name()).size()
print("orders by weekday:\n", wd.sort_values(ascending=False).to_string())
monthly = o.groupby(o.order_purchase_timestamp.dt.to_period("M")).size()
print("orders by month:\n", monthly.to_string())

print("\n== CUSTOMERS")
print("customer_id unique:", cust.customer_id.nunique(), "| rows:", len(cust))
print("customer_unique_id unique:", cust.customer_unique_id.nunique())
per = cust.groupby("customer_unique_id").customer_id.nunique()
print("unique customers with 2+ customer_ids (orders):", (per >= 2).sum(), "| 3+:", (per >= 3).sum())
loc = cust.groupby("customer_unique_id").agg(zips=("customer_zip_code_prefix", "nunique"),
                                             cities=("customer_city", "nunique"),
                                             states=("customer_state", "nunique"))
print("unique customers with >1 zip prefix:", (loc.zips > 1).sum(), "| >1 city:", (loc.cities > 1).sum(), "| >1 state:", (loc.states > 1).sum())
print("orders.customer_id all in customers:", orders.customer_id.isin(cust.customer_id).all())
print("states:", cust.customer_state.nunique())

print("\n== ITEMS / SELLERS")
print("orders with items:", items.order_id.nunique(), "| orders without items:", (~orders.order_id.isin(items.order_id)).sum())
spo = items.groupby("order_id").seller_id.nunique()
print(f"orders with 2+ sellers: {(spo >= 2).sum():,} of {len(spo):,} = {(spo >= 2).mean():.2%}")
ipo = items.groupby("order_id").order_item_id.max()
print(f"orders with 2+ items: {(ipo >= 2).sum():,} = {(ipo >= 2).mean():.2%}; max items in one order: {ipo.max()}")
print("item seller_ids all in sellers:", items.seller_id.isin(sellers.seller_id).all())
print("item product_ids all in products:", items.product_id.isin(prods.product_id).all())
print("unique sellers in items:", items.seller_id.nunique(), "| sellers file:", len(sellers))
print("price range:", items.price.min(), items.price.max(), "| freight range:", items.freight_value.min(), items.freight_value.max())
print("items with price<=0:", (items.price <= 0).sum(), "| freight==0:", (items.freight_value == 0).sum())

print("\n== PRODUCTS")
print("products missing category:", prods.product_category_name.isna().sum())
cats = set(prods.product_category_name.dropna())
print("categories:", len(cats), "| without English translation:", sorted(cats - set(trans.product_category_name)))

print("\n== PAYMENTS")
print("orders with payments:", pays.order_id.nunique(), "| orders without payments:", (~orders.order_id.isin(pays.order_id)).sum())
ppo = pays.groupby("order_id").size()
print(f"orders with 2+ payment rows: {(ppo >= 2).sum():,} = {(ppo >= 2).mean():.2%}")
print("payment types:\n", pays.payment_type.value_counts().to_string())
print("payment_value==0 rows:", (pays.payment_value == 0).sum())
item_tot = items.groupby("order_id").apply(lambda g: (g.price + g.freight_value).sum(), include_groups=False)
pay_tot = pays.groupby("order_id").payment_value.sum()
j = pd.concat([item_tot.rename("items"), pay_tot.rename("pays")], axis=1).dropna()
diff = (j["pays"] - j["items"]).abs()
print(f"orders where payments != items+freight by > 1.00 BRL: {(diff > 1).sum():,} of {len(j):,}")

print("\n== REVIEWS")
print("review rows:", len(revs), "| unique review_id:", revs.review_id.nunique(), "| unique order_id:", revs.order_id.nunique())
rpo = revs.groupby("order_id").size()
print("orders with 2+ reviews:", (rpo >= 2).sum())
rid = revs.groupby("review_id").order_id.nunique()
print("review_ids attached to 2+ orders:", (rid >= 2).sum())
print("score distribution:\n", revs.review_score.value_counts().sort_index().to_string())
print("review_creation_date sample:", revs.review_creation_date.iloc[0], "| answer ts sample:", revs.review_answer_timestamp.iloc[0])
