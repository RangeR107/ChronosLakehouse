"""Turn Olist's final records back into the events that would have produced them.

Spec: "Event model and the replay generator" and "Derived values and the rule behind each".
Every value comes from the Olist files. The only rule-made value is emitted_at for events Olist
gives no time for (cancellations and the 8 undated deliveries), and that rule is the spec's.

Uses the standard library only: this reads each file once, and the generator stays independent of
the pipeline's own tools.
"""
import csv
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from generator.events import make_event

TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Spec: events with no event time are emitted one day after the order's last observed timestamp.
NO_TIME_DELAY = timedelta(days=1)

# The order's own lifecycle timestamps. The estimated delivery date is a promise, not something
# that happened, and reviews are a separate stream, so neither counts as "last observed".
MILESTONES = ("order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
              "order_delivered_customer_date")

FILES = {
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
}


def read_csv(path: Path) -> list[dict]:
    """Rows as dicts of the CSV text, with empty cells as None (they become JSON null)."""
    with open(path, newline="", encoding="utf-8") as f:
        return [{k: (v if v != "" else None) for k, v in row.items()} for row in csv.DictReader(f)]


def load(olist_dir: Path) -> dict[str, list[dict]]:
    return {name: read_csv(Path(olist_dir) / file) for name, file in FILES.items()}


def group_by_order(rows: list[dict], sort_field: str) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["order_id"]].append(row)
    for children in grouped.values():
        # Sequence numbers are text in the CSV; sort them as numbers so "10" follows "9".
        children.sort(key=lambda r: int(r[sort_field]))
    return grouped


def time_after_last_milestone(order: dict) -> str:
    last = max(datetime.strptime(order[c], TS_FORMAT) for c in MILESTONES if order[c] is not None)
    return (last + NO_TIME_DELAY).strftime(TS_FORMAT)


def order_placed(order: dict, customer: dict, items: list[dict], payments: list[dict]) -> dict:
    # Payment rows travel with order_placed: all 160 never-approved orders still have payments,
    # which would never arrive if they rode on payment_approved.
    payload = {
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "customer_unique_id": customer["customer_unique_id"],
        "customer_zip_code_prefix": customer["customer_zip_code_prefix"],
        "customer_city": customer["customer_city"],
        "customer_state": customer["customer_state"],
        "order_purchase_timestamp": order["order_purchase_timestamp"],
        "order_estimated_delivery_date": order["order_estimated_delivery_date"],
        "items": [{k: i[k] for k in ("order_item_id", "product_id", "seller_id",
                                     "shipping_limit_date", "price", "freight_value")}
                  for i in items],
        "payments": [{k: p[k] for k in ("payment_sequential", "payment_type",
                                        "payment_installments", "payment_value")}
                     for p in payments],
    }
    t = order["order_purchase_timestamp"]
    return make_event("order_placed", [order["order_id"]], t, t, payload)


def milestone_event(event_type: str, order: dict, field: str) -> dict:
    t = order[field]
    return make_event(event_type, [order["order_id"]], t, t,
                      {"order_id": order["order_id"], field: t})


def order_events(order: dict, customer: dict, items: list[dict], payments: list[dict]) -> list[dict]:
    """Every event one order produces, in lifecycle order."""
    oid = order["order_id"]
    events = [order_placed(order, customer, items, payments)]
    if order["order_approved_at"] is not None:
        events.append(milestone_event("payment_approved", order, "order_approved_at"))
    if order["order_delivered_carrier_date"] is not None:
        events.append(milestone_event("handed_to_carrier", order, "order_delivered_carrier_date"))

    delivered_at = order["order_delivered_customer_date"]
    if delivered_at is not None:
        events.append(milestone_event("delivered", order, "order_delivered_customer_date"))
    elif order["order_status"] == "delivered":
        # 8 orders are delivered with no date: the event carries no time, and when it arrives is
        # the spec's documented simulation rule, not a business fact.
        events.append(make_event("delivered", [oid], None, time_after_last_milestone(order),
                                 {"order_id": oid, "order_delivered_customer_date": None}))

    final = {"canceled": "order_cancelled", "unavailable": "order_unavailable"}
    if order["order_status"] in final:
        # Olist records that an order ended canceled or unavailable, but never when.
        events.append(make_event(final[order["order_status"]], [oid], None,
                                 time_after_last_milestone(order), {"order_id": oid}))
    return events


def review_event(review: dict) -> dict:
    # The answer time is when the customer submitted the review; the creation date is when the
    # survey was sent (docs/decisions.md). Free-text comments are deliberately left out.
    t = review["review_answer_timestamp"]
    payload = {k: review[k] for k in ("order_id", "review_id", "review_score",
                                      "review_creation_date", "review_answer_timestamp")}
    return make_event("review_submitted", [review["order_id"], review["review_id"]], t, t, payload)


def build_events(data: dict[str, list[dict]], purchased_from: str | None = None) -> list[dict]:
    """All events of the truth replay, in no particular order (the writer sorts them).

    purchased_from keeps only orders purchased at or after that time, with their reviews. The
    Phase 1 truth slice starts at the reliable window (docs/decisions.md), so no order from 2016
    has later events landing inside the slice.
    """
    customers = {c["customer_id"]: c for c in data["customers"]}
    items = group_by_order(data["items"], "order_item_id")
    payments = group_by_order(data["payments"], "payment_sequential")

    events, kept = [], set()
    for order in data["orders"]:
        if purchased_from is not None and order["order_purchase_timestamp"] < purchased_from:
            continue  # same-format timestamps compare correctly as text
        oid = order["order_id"]
        kept.add(oid)
        events.extend(order_events(order, customers[order["customer_id"]],
                                   items.get(oid, []), payments.get(oid, [])))
    events.extend(review_event(r) for r in data["reviews"] if r["order_id"] in kept)
    return events
