# Decisions

Changes to the spec or to how it is built, agreed during the build. Each entry says what changed,
why, what else was considered, and which spec section it touches. The spec's own Decision log holds
the design decisions it started with; this file holds what happened after.

Status is one of: **agreed** (the spec asks for it, or the owner approved it), **proposed**
(made in a phase and waiting for the owner's OK; easy to revert), or **open** (needs a decision
before the phase that depends on it).

---

## 2026-11-22 · Iceberg reads follow `spark.wap.branch` (confirmed)

- **Status:** agreed (a test result, not a choice)
- **What:** the smoke test (`tests/unit/test_iceberg_smoke.py`) passes on PySpark 3.5.6 with
  iceberg-spark-runtime 1.10.0. With `spark.wap.branch = audit` set, `SELECT count(*)` returns 2,
  the branch's count, not main's.
- **Why it matters:** the run lifecycle (spec, Publishing, step 2) assumes jobs read the run's
  branch while the setting is on. Had the count been 1, every job would have had to name its
  branch explicitly.
- **Alternatives:** none needed. The design stays as written.
- **Spec section:** "Getting started: week 1" says the test hasn't been run. It now has; that
  sentence can go when the spec is next revised.

## 2026-11-22 · Spec numbers updated to the Kaggle download

- **Status:** agreed (the spec says to rerun both scripts on Kaggle and update what differs)
- **What:** reran `tools/profile_olist.py` (output in `docs/kaggle_profile.txt`) and
  `tools/measure_spec.py` on the Kaggle files. Everything matches the mirror except numbers that
  depend on the reviews file, which has 99,224 rows on Kaggle and 100,000 on the mirror:

  | Number | Spec (mirror) | Kaggle |
  | --- | --- | --- |
  | Orders with more than one review | 555 | 547 |
  | ... of which tie on the latest creation date | 157 | 156 |
  | review_ids on more than one order | 802 | 789 |
  | Q4 late orders: mean score, 1-star share | 2.26, 54.2% | 2.27, 53.8% |
  | Q4 on-time orders: mean score, 1-star share | 4.28, 6.8% | 4.29, 6.6% |
  | Q7 top-20% categories, mean score range | 3.97 to 4.22 | 3.99 to 4.22 |
  | Q7 top-20% categories, low-score share range | 11.8% to 16.8% | 11.4% to 16.2% |
  | Geolocation file rows | not profiled | 1,000,163 (still unused) |

  Unchanged, and checked: every row count but reviews, Q1 to Q3, Q5, Q6, the Q7 per-quarter flags
  (1 to 4 a quarter; cama_mesa_banho and moveis_decoracao each in 4 of 7 quarters), the money
  totals (R$13.49M items, R$2.24M freight), the 249 payment mismatches (232 more, 17 less), the 775
  orders without items (767 + 8), 14,994 zip prefixes with 39 multi-spelled, 27 customer and 23
  seller states. The measure_spec.py CSVs are byte-identical between two runs.
- **Alternatives:** keep the mirror numbers and note the gap (rejected: gold is built from Kaggle,
  so the sanity checks must be Kaggle's).
- **Spec sections changed:** Summary ("How claims are backed"); Shared definitions (Order review
  score); "Why questions 4 and 7 are defined this way"; Data sources (how measured, file table,
  Reviews bullet, Real defects table); Known limitations (reviews file); Sources (mirror link).

## 2026-11-22 · Airflow runs with LocalExecutor, not the quick start's CeleryExecutor

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** `docker-compose.yml` is the official Airflow 3.4.0 quick-start file with Redis, the
  Celery worker and Flower removed, and `AIRFLOW__CORE__EXECUTOR: LocalExecutor`.
- **Why:** runs are serial (`max_active_runs = 1`), so spreading tasks over Celery workers buys
  nothing, and two fewer containers matter on a laptop that also runs Spark. It removes a service
  rather than adding one.
- **Alternatives:** the quick start unchanged (works, heavier); `airflow standalone` in one
  container (lighter still, but further from how Airflow is deployed).
- **Spec section:** "Getting started: week 1" (Local services) says to add Airflow from the quick
  start; it should say "adapted from".

## 2026-11-22 · Airflow image `apache/airflow:3.4.0-python3.11`

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** the Python 3.11 variant of the 3.4.0 image instead of the default tag.
- **Why:** the project pins Python 3.11 (Glue 5.1). The image's providers are fixed by Airflow's
  constraints for that tag; it ships apache-airflow-providers-amazon 9.39.0, which satisfies
  "pin the Amazon provider with it".
- **Alternatives:** the default `3.4.0` tag (a different Python from the rest of the project).

## 2026-11-22 · A separate PostgreSQL 16.16 container for Chronos

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** `chronos-db` (PostgreSQL 16.16, host port 5433, database `chronos`) holds the source
  master and, from Phase 3, the metrics store and the dashboard serving tables (spec Revision 5).
  Inside `chronos`, three schemas keep the roles apart (planned by the owner for Phase 3):
  `source` (the customer, seller and product master), `metrics` (DataSentinel's check results,
  profiles and releases) and `serving` (the tables the dashboards read). Airflow keeps its own
  `airflow-db`.
- **Why:** the source database plays an operational system; Airflow's bookkeeping doesn't belong
  in it, and a reset of one shouldn't touch the other. CLAUDE.md pins PostgreSQL 16; 16.16 is
  that major version pinned to the exact minor, the same image Airflow's quick start uses. Port
  5433 avoids clashing with a PostgreSQL installed on the laptop. Schemas rather than separate
  databases, because the release load and the metrics it records can then share one
  transaction.
- **Alternatives:** one server with two databases (fewer containers, coupled lifecycles);
  separate databases for source, metrics and serving (a cross-database write can't be one
  transaction in PostgreSQL).

## 2026-11-22 · ruff skips `tools/`

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** `extend-exclude = ["tools"]` in `pyproject.toml`.
- **Why:** ruff 0.17's defaults flag five cosmetic issues in the reference scripts (unused
  variables, import order, a %-format). Editing them risks changing a reference number for no
  gain; they stay exactly as measured.
- **Alternatives:** fix the findings (touches reference code); per-rule ignores (noisier).

## 2026-11-22 · pandas 3.1.1 and numpy 2.4.6 pinned in an optional `[tools]` extra

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** `pip install -e ".[tools]"` installs the versions the reference scripts in `tools/`
  were run with. They are not core dependencies: the pipeline doesn't use them, and CI doesn't
  install them.
- **Why:** exact pins keep reference numbers reproducible, and keeping them out of the core
  dependencies stops pipeline code from quietly starting to rely on pandas. No other library was
  added. PyYAML was not added: `docs/metrics.yml` was validated with the copy inside the Airflow
  image.
- **Alternatives:** core dependencies (the first proposal; the owner moved them out).

## 2026-11-22 · SCD Type 2 versions on zip, city or state: 252 people, not 250

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** dim_customer keeps the spec's rule: a new version starts when the master's zip
  prefix, city or state changes. Under that rule **252** people get at least one new version and
  **6** change more than once. The spec's 250 and 5 counted zip-prefix changes only. The two extra
  people keep their zip prefix while the city name changes:
  - zip 73752: "planaltina de goias" → "planaltina" (GO)
  - zip 71065: "guara" → "brasilia" → "guara" (DF), which is also the 6th multiple changer
- **Why:** it is what the master recorded, and the SCD Type 2 consistency test compares the full
  address (zip, city, state) with each order's address, so a zip-only rule would fail that test
  for these two people.
- **Alternatives:** version on zip prefix and state only, taking city from the latest order
  (keeps 250, but the consistency test would have to ignore city); normalise city names first
  (doesn't help: these are different names, not spelling variants).
- **Spec sections changed:** Data sources ("Real address changes exist", which keeps the zip-only
  counts and adds the full-address ones), Data model (SCD Type 2 rules: 252), Known limitations
  (6 people change more than once). The "about 25 people" late-dimension-change rate still holds.

## 2026-11-22 · Spec Revision 5: zero cost, PostgreSQL serves the dashboards

- **Status:** agreed (owner wrote Revision 5)
- **What:** dashboards are served from PostgreSQL instead of Redshift Serverless, and Phase 4
  uses only the AWS free plan (S3, Glue, Glue Data Catalog, and Athena if the plan allows it,
  DuckDB locally if not). Releases still load in one transaction (BEGIN ... COMMIT in PostgreSQL).
- **Why:** the project must cost nothing, and Redshift is only available on paid AWS plans.
- **Alternatives:** Redshift Serverless (Revision 4; needs a paid plan); Athena alone for
  dashboards (no transactional release switch).
- **How it was applied:** Revision 5 was written on top of the original Revision 4, so it was
  merged with the Phase 0 edits above (Kaggle numbers, 252 people) rather than copied over them.
  The two sets of edits touch different lines except one Known-limitations line, which keeps "6
  people" and adds Revision 5's "No Redshift" bullet.
- **Effect on Phase 0:** none on its scope or "Done when". `chronos-db` will also hold the
  serving tables.
- **Watch in Phase 3:** the restatement report and audits read releases by snapshot id. If
  Phase 4 falls back from Athena to DuckDB, confirm DuckDB can read an Iceberg table at a given
  snapshot.
- **Spec sections:** none left to update. The Sources list still cites Redshift pricing and
  free-trial pages that nothing uses now; they can go in the next revision.


## 2026-11-22 · The Q7 regression test maps categories through the translation file

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** `measure_spec.py` groups by `product_category_name` (Portuguese, "unknown" when
  missing), while dim_product stores the English name. The metric regression test maps the CSV's
  names through `product_category_name_translation.csv`, leaving the 2 untranslated categories and
  "unknown" as they are.
- **Why:** the translation is one-to-one (71 Portuguese names, 71 distinct English names), so
  mapping never merges two categories, and the reference script stays as it is.
- **Alternatives:** make measure_spec.py output English names (changes the reference script for
  no gain); store Portuguese names in gold (worse for dashboards).

## 2026-11-22 · Q7 late-delivery rate added to measure_spec.py

- **Status:** agreed (owner, 22 Nov 2026; the owner gave the definition)
- **What:** `q7_category_by_quarter.csv` gains `delivered_orders` and `late_rate`. Per purchase
  quarter and category: among orders purchased that quarter whose items all share that category,
  the delivered orders with a delivery date, and the share of them late by calendar day (as in
  question 1).
- **Why:** the spec asks gold to show it beside each flagged category, and the metric regression
  test can only cover what the reference script produces.
- **Alternatives:** check it only in the DuckDB reference build of the truth test.
- **Spec section updated:** Business questions, question 7 ("Show each flagged category's
  late-delivery rate beside it" now says which orders count).

## 2026-11-22 · Tests split between CI (fixtures) and local runs (Olist data)

- **Status:** proposed (the owner asked for the split; this is how it would work)
- **Problem:** GitHub Actions can't see `data/raw/olist/`, which is never committed. The spec
  says CI runs the truth test on a 60-day slice, which needs the real files.
- **Proposal:**

  | Test | Data | Where it runs |
  | --- | --- | --- |
  | Unit tests, one per transformation | Tiny DataFrames built in the test | CI and locally |
  | Iceberg smoke test | Two hand-written rows | CI and locally |
  | The nine scenario tests | Scripted inputs with expected gold rows, in the test | CI and locally |
  | Idempotency test | One fixture day, processed twice | CI and locally |
  | Mini end-to-end test | A handful of hand-built orders shaped like Olist files, in `tests/fixtures/`, replayed through bronze to gold and compared with the DuckDB reference build run on the same fixtures | CI and locally |
  | Truth test, parts 1 to 3, on a 60-day slice | `data/raw/olist/` | Locally, before each phase is called done |
  | Metric regression test (gold vs measure_spec.py CSVs) | `data/raw/olist/` | Locally |
  | SCD Type 2 consistency test on every real order | `data/raw/olist/` | Locally (CI runs it on the fixtures) |
  | Full truth replay | `data/raw/olist/` | Locally, before each tagged version |

- **How:**
  - Tests that need the real files carry a pytest marker, `olist`. When `data/raw/olist/` is
    missing they are skipped with a reason, so CI shows them as skipped, not passed.
  - Setting `CHRONOS_REQUIRE_OLIST=1` turns that skip into a failure. Local runs that count as
    evidence set it, so a missing data folder can never look like a pass.
  - The metric regression test runs measure_spec.py into a temporary folder first, so it never
    compares against stale CSVs.
  - Each phase's notes record the local run: command, commit, date and result. CI can't show it.
- **Why fixtures are allowed:** they are test inputs that live only in `tests/`, never in the
  lake or in any metric. "Never invent business data" is about what the warehouse reports. The
  mini end-to-end test exists so the full pipeline path, including the DuckDB reference
  comparison, runs on every push even without the real data.
- **Alternatives:**
  - Download the data from Kaggle inside CI with an API token stored as a GitHub secret. It adds
    a credential, a tool the spec doesn't name and a dependency on Kaggle being reachable.
  - Commit a slice of Olist. CLAUDE.md forbids committing the data, and the licence's
    share-alike terms would apply to the repository.
  - A self-hosted runner on the laptop. It's free, but it runs CI jobs on your own machine, and
    it only works while the laptop is on.
- **Spec sections that need updating if agreed:** Testing and evaluation ("Tests that run in
  GitHub Actions on every push", and "CI runs all three on a 60-day slice") and the Phased build
  plan, Phase 2 "Done when" ("pass in CI" would become "scenario tests pass in CI; the truth test
  passes locally on a 60-day slice").

## 2026-11-22 · Phase 1 event model: choices the spec leaves open

- **Status:** proposed (each is reversible until bronze holds real replays)
- **review_submitted event time = `review_answer_timestamp`.** The spec lists both review
  timestamps for one event. The answer time is when the customer actually submitted it;
  `review_creation_date` is when the survey was sent. Both stay in the payload.
- **Payload values are Olist's CSV text, verbatim.** Prices stay `"58.90"`, not a float, and an
  empty CSV cell becomes JSON null. Silver types them with the contract, so bronze holds exactly
  what was received and no float rounding happens before typing.
- **order_placed also carries `customer_unique_id`** next to `customer_id`, plus the delivery
  address. Keying dim_customer needs the person, and an operational order record would carry the
  account it belongs to. Both are observed values.
- **Review free text (comment title and message) is left out** of the feed. No question uses it,
  and it is the one place personal text could appear.
- **"The order's last observed timestamp"** (the emission rule for cancellations and the 8
  undated deliveries) means the latest of purchase, approval, carrier handover and delivery. It
  excludes the estimated delivery date (a promise, not an event) and review times (a different
  event stream).
- **Logical key in the envelope** is a string: `order_id`, or `order_id:review_id` for reviews.
  The event_id hash input joins event_type, each key part and the version with 0x1F, as the spec
  says.
- **Landing layout:** `landing/date=YYYY-MM-DD/events_YYYY-MM-DDTHH.jsonl`, one file per hour that
  has events, and `_complete.json` written last. Locally `landing/` is a folder standing in for
  the S3 prefix; the file manifest uses SHA-256 where S3 would give an ETag.
- **What derives status:** cancelled and unavailable events win, then a delivered event, else the
  order is open. On the Kaggle data this reproduces every final status exactly (96,478 delivered,
  625 canceled, 609 unavailable, 1,729 open). Which open status an order was in (shipped,
  invoiced, ...) has no timestamp, so it is not in the feed; fact_order_fulfillment only has
  `final_status` and `is_open`, as the spec defines.

## 2026-11-22 · Two review facts the spec doesn't state

- **Status:** open (spec wording, and one possible DataSentinel flag)
- **85 review creation dates have a time of day.** The spec says creation dates have none. The
  order-review tie-breakers still work, but "Shared definitions: Order review score" and the Q4/Q7
  notes should say "almost never".
- **63 reviews were answered before their order was purchased** (64 created before the purchase
  date). This is a real source defect that the Real defects table doesn't list. Proposal: add it
  as a Flag, kept in the reviews fact and left out of nothing, since no metric uses review timing.
- **Spec sections:** Shared definitions (Order review score); Data sources (Real defects table).
