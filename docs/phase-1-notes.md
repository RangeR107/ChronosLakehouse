# Phase 1 notes: the happy path

Phase 1 moves real data end to end, with no simulated defects yet: Olist orders are replayed as
an hourly event feed, landed in bronze, checked and deduplicated into silver, modelled into a
star schema in gold, summarised, and loaded into PostgreSQL for a dashboard.

**Done when** (spec, Phased build plan): "The truth test passes on 60 replayed days, and
rerunning any day gives identical output." Both are true:

- `tests/truth/test_truth_slice.py` replays Jan and Feb 2017 (60 business dates), runs every day
  through the pipeline, and compares 9 business tables row by row with a separate DuckDB build
  from the Olist CSVs: every row matches.
- The same test runs the first, a middle and the last day twice. The second run writes nothing,
  and every bronze, silver and gold table has the same row count and content hash.

## What was built

The pipeline is one function per Airflow task, each taking `(ctx, business_date)`.
`jobs/run_day.py` calls them in the DAG's order and does nothing else.

| Step | Where | In one sentence |
| --- | --- | --- |
| Replay generator | `generator/` | Turns Olist's final records back into events (`order_placed`, `payment_approved`, `handed_to_carrier`, `delivered`, `order_cancelled`, `order_unavailable`, `review_submitted`), writes them as hourly JSON Lines files with a completion marker, and plays the source database. |
| Source database | `sql/source_schema.sql` | PostgreSQL `source` schema: customers, sellers, products; a trigger stamps `updated_at` from a replay clock. |
| wait_for_landing, register_files | `jobs/land_to_bronze.py` | Wait for the day's marker; compare files with the bronze file manifest; report missing and unlisted files. |
| extract_dimensions | `jobs/extract_dimensions.py` | Pull changed master rows from PostgreSQL with a high-water mark and a one-day overlap. |
| land_to_bronze | `jobs/land_to_bronze.py` | Append every line of the new files, as received, then record the files in the manifest. |
| bronze_to_silver | `jobs/bronze_to_silver.py`, `jobs/contract.py` | Check each event against `contracts/events_v1.json`, quarantine failures with reasons, drop duplicates, type the rest. |
| build_dimensions | `jobs/build_dimensions.py`, `jobs/keys.py` | dim_customer (SCD Type 2), dim_geography, dim_seller, dim_product, dim_date, with SHA-256 surrogate keys. |
| build_facts | `jobs/build_facts.py` | fact_order_events (ledger), fact_order_fulfillment (accumulating snapshot), fact_order_items, fact_payments, fact_reviews. |
| recompute_aggregates | `jobs/recompute_aggregates.py` | Questions 1 and 2: late rate and days early by delivery month and region. |
| load_serving | `jobs/load_serving.py` | Copy the aggregate into PostgreSQL `serving` in one transaction. |
| Dashboard | `dashboard/app.py` | Streamlit page that reads only `serving`. |
| Shared MERGE | `jobs/merge.py` | Write only rows that changed, compared null-safely. |

## How to run it

Start the database (Docker Desktop must be running):

```bash
docker compose up -d chronos-db
```

Then generate and run one business date at a time. The order matters: the source database only
knows the past, like a real one, so day D must be processed before day D+1 is generated.
Generating all 60 days first would let day 1's extract see day 60's customer addresses.

```bash
for d in $(.venv/bin/python -c "from datetime import date, timedelta as t; print(*[date(2017,1,1)+t(n) for n in range(60)])"); do .venv/bin/python -m generator.replay --olist data/raw/olist --landing lake/landing --start $d --end $d --purchased-from 2017-01-01 && JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/python -m jobs.run_day --start $d --end $d || break; done
```

Then open the dashboard:

```bash
.venv/bin/streamlit run dashboard/app.py
```

Each `run_day` line printed is one business date: files and events landed, rows checked,
dimension and fact rows written, and rows served. The warehouse is in `lake/warehouse`; delete
`lake/` and the `source` and `serving` schemas to start over.

## How to test it

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/pytest tests/unit
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) CHRONOS_REQUIRE_OLIST=1 .venv/bin/pytest tests/truth
```

- `tests/unit` uses the hand-built fixture in `tests/fixtures/olist_mini` and runs in CI.
- `tests/truth` runs the same comparison twice: on the fixture (15 days, in CI) and on the real
  slice (60 days, about ten minutes, locally; CI skips it because the data is never committed).
  Both need PostgreSQL.

## The ideas, in plain words

**Three clocks.** Every event has an event time (when it happened, from Olist), an emission time
(when the source sent it) and an ingestion time (when bronze received it). The pipeline runs on
a replay clock: the run for 5 Jan 2017 "happens" at 6 Jan 00:00. Ingestion is stamped with that
time, not the laptop's, which is why a rerun produces identical rows.

**Bronze keeps everything; silver decides.** Bronze appends each line exactly as received, with
its file and line number. Silver applies the contract: a line that breaks it goes to quarantine
with the reasons, and the same event_id is never inserted twice.

**An accumulating snapshot.** fact_order_fulfillment has one row per order, revisited as the
order progresses. A run doesn't patch columns as events arrive; it rebuilds each touched order's
row from all of its events. So a delivery that arrives late fills the row the next day, and the
result doesn't depend on the order events arrived in.

**SCD Type 2.** The customer master only keeps the current address, so the warehouse keeps the
history. A new version starts when the address changes, dated by the purchase time of the first
order seen with it, and labelled as inferred (`effective_from_basis = 'first_order_seen'`). An
order points at the version that was effective when it was placed.

**Only write what changed.** Every gold table is recomputed and then compared with what is
already there; only different rows are merged. That is what makes a rerun commit nothing.

**Checked against an independent build.** The truth test doesn't trust the pipeline's own logic.
`tests/truth/reference.py` builds the same tables straight from the Olist CSVs in DuckDB SQL,
with its own key macro and city cleaning, and the two must agree on every row.

## Results on the slice

Local run, 23 Nov 2026, uncommitted working tree on top of `163f1f9`:
`CHRONOS_REQUIRE_POSTGRES=1 CHRONOS_REQUIRE_OLIST=1 pytest tests`: **101 passed** in 14 minutes.

| Table | Rows on day 60 | Matches the DuckDB reference |
| --- | --- | --- |
| fact_order_fulfillment | 2,679 orders (1,707 delivered, 20 canceled, 55 unavailable, 897 still open on 1 Mar) | yes |
| fact_order_items | 3,019 | yes |
| fact_payments | 2,841 | yes |
| fact_reviews | 1,591 | yes |
| dim_customer | 2,612 people, one version each (no address changes in two months) | yes |
| dim_geography | 3,916 places | yes |
| dim_seller, dim_product | 3,095 and 32,951 | yes |
| agg_delivery_by_month_region | 18 rows (Jan to Mar 2017 deliveries) | yes |

Of the 1,707 orders delivered by 1 Mar, only 2 were late: the slice holds the quickest deliveries
of Jan and Feb, because slow ones hadn't arrived yet. That is "as known on day 60", not Olist's
6.79%; the full replay reproduces the spec's numbers.

What the truth tests caught on the way, each now fixed and recorded in `docs/decisions.md`:

- A seller city in decomposed Unicode ("sa" + combining tilde + "o paulo") became a second São
  Paulo in dim_geography.
- Writing the whole replay before running the pipeline lost a customer's first address version:
  the source must only be one day ahead.
- An aggregate row stayed after its only order was cancelled the next day.

## Questions an interviewer could ask

**Why replay a finished dataset as events?** Olist only has final records. A pipeline's hard
problems are about arrival: late, duplicated, out of order. Replaying lets the project control
arrival while every business value stays real (`generator/olist.py`).

**How do you make reruns idempotent?** Four things: bronze skips files already in its manifest
(`register_files`); silver inserts only event_ids it doesn't have (`merge_new`); gold merges only
changed rows (`jobs/merge.py`); and every time the pipeline stamps comes from the replay clock
(`jobs/context.py: run_time`). The truth test runs days twice and compares content hashes.

**What happens if a run crashes halfway?** Each layer writes its "progress" table last: bronze
records files in the manifest after appending events, silver writes `events` after the typed
tables, and gold writes `fact_order_events` after the other facts. The next run starts from that
mark, so it redoes the unfinished work, and the insert-only or compare-first MERGEs make the
redo harmless.

**How does build_facts know which orders to rebuild?** It reads the latest `ingested_at` in
`fact_order_events` and rebuilds orders with newer silver events (`touched_orders`). No separate
state file can drift from the data.

**Why does each milestone come from its own event type?** So status is a function of which
events exist, not of which arrived last: cancelled or unavailable wins, then delivered, else
open (`milestones` in `jobs/build_facts.py`). Delivered-then-carrier arrival gives the same row
as carrier-then-delivered.

**Why surrogate keys from SHA-256 rather than a sequence?** A sequence depends on load order, so
a rebuild or a rerun would give different keys. A hash of the natural key gives the same key
everywhere, forever. Python's `hash()` is randomised per process, so it can't be used
(`jobs/keys.py`).

**Why are region and geography keys on the fact, not on the customer?** Each order carries its
own delivery address, which is observed; the customer's address history is inferred. Region
lives only in dim_geography, so it can't disagree with itself (spec: Data model).

**Why is the dashboard in PostgreSQL and not reading the lake?** The dashboard must see one
consistent state. load_serving swaps all rows in one transaction, so a reader sees the previous
load or the new one, never half of each. Phase 3 adds releases around the same load.

**Why a separate DuckDB build for the truth test?** If the test reused pipeline code, a bug in
that code would be in both sides and the test would pass. A different engine and separately
written SQL make agreement meaningful.

**What does Phase 1 not do yet?** No simulated defects, no dirty-period recompute, no inferred
members, no re-keying after a late address change, no releases. Those are Phases 2 and 3.
Runtime: the slice's dimension step recomputes all 32,951 products every day, which an
input-changed check would avoid (`docs/decisions.md`).
