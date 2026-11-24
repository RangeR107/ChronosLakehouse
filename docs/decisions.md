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

- **Status:** agreed (owner, 23 Nov 2026). The real-slice truth test is one command,
  `make truth`, and each phase's notes record when it last passed.
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

## 2026-11-22 · PostgreSQL driver: psycopg 3

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** psycopg 3, pinned exactly, is the one PostgreSQL driver. The generator uses it to
  write the source master; extract_dimensions uses it to read changed rows and hands them to
  Spark.
- **Version:** `psycopg[binary]==3.3.6`, the version Airflow 3.4.0's constraints install in its
  image (checked inside the running container), not the newest 3.3.9. The Phase 2 Airflow image
  then has one copy, not two that disagree. The binary extra bundles libpq, so neither the laptop
  nor CI needs PostgreSQL's client library installed.
- **Why:** one dependency covers both writing and reading; Airflow's image already ships it.
- **Alternatives:** Spark's PostgreSQL JDBC driver for the extract (the generator would still
  need a Python driver, so two drivers); both.

## 2026-11-22 · The Phase 1 truth slice: Jan and Feb 2017 orders, truth as of day 60

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** the slice replays only orders purchased from 2017-01-01, the start of the reliable
  window, for 60 business dates (2017-01-01 to 2017-03-01). Truth is Olist as known at the end of
  day 60: an event counts if it was emitted by then. The DuckDB reference build applies that cut
  itself, from the Olist files and the spec's written emission rule, never with pipeline code.
- **Why:** about 2,600 orders, so the business tables and metrics hold real data, while the
  test stays fast. Starting at the window avoids orders from 2016 whose later events would land
  inside the slice.
- **Alternatives:** the first 60 feed days from 2016-09-04 (about 330 orders, all outside the
  reliable window, so metrics would be empty); the full 786-day replay only (strongest, slowest;
  still run before tagged versions).
- **Spec section:** Testing and evaluation ("CI runs all three on a 60-day slice") should name
  this slice and its cut-off rule.

## 2026-11-22 · The Phase 1 dashboard reads PostgreSQL `serving`

- **Status:** agreed (owner, 22 Nov 2026)
- **What:** Phase 1 loads gold aggregates into the `serving` schema of the `chronos` database in
  one transaction, and the Streamlit dashboard reads only that schema. There is no release
  manifest yet; Phase 3 wraps the same load in releases.
- **Why:** it is already the final shape of the design (spec Revision 5), so Phase 3 adds
  releases around the load instead of changing where dashboards read.
- **Alternatives:** DuckDB's Iceberg extension over gold (dashboards would read unpublished data
  until Phase 3); Spark over gold (slow start, needs Java in the dashboard).

## 2026-11-22 · Phase 1 runs with a Python runner; the Airflow DAG wraps the same steps in Phase 2

- **Status:** agreed (owner, 22 Nov 2026, with the two conditions below)
- **What:** in Phase 1 a business date runs through `run_day`, a plain Python runner in the
  project's venv.
  1. Every step is its own function named after its DAG task (`register_files`,
     `extract_dimensions`, `land_to_bronze`, `bronze_to_silver`, `build_dimensions`,
     `build_facts`, `recompute_aggregates`, ...). Each takes the business date as an argument and
     keeps no hidden state between calls. `run_day` only calls them in the DAG's order. The
     `chronos_daily` DAG later is a thin wrapper around the same functions, with no logic of its
     own.
  2. The custom Airflow image (Java 17, PySpark 3.5.6 and the Iceberg 1.10.0 jar on top of
     `apache/airflow:3.4.0-python3.11`) and the real `chronos_daily` DAG are built at the start
     of Phase 2, before the late-data scenarios, because backfill, retries and
     `max_active_runs = 1` are tested there.
- **Why:** Phase 1 is about correct tables, and the Airflow image has no Java or Spark. Keeping
  all logic in the step functions means the DAG adds orchestration only, so moving to Airflow
  can't change results.
- **Alternatives:** the custom image and DAG in Phase 1 (more setup before anything works).
- **Spec section:** the Phased build plan's Phase 2 row should list the custom Airflow image and
  the `chronos_daily` DAG as its first items.

## 2026-11-22 · PostgreSQL tests run in CI against a service container

- **Status:** proposed
- **What:** tests that need PostgreSQL carry the `postgres` marker and use a throwaway database
  per test, created and dropped by `tests/conftest.py`. CI starts `postgres:16.16` as a GitHub
  Actions service container and sets `CHRONOS_REQUIRE_POSTGRES=1`, so these tests run on every
  push and a skip there counts as a failure. Locally they skip with a reason when the database
  isn't running.
- **Why:** the replay-clock trigger and the generator's customer writes carry two of the spec's
  easy-to-break rules; they deserve CI, and PostgreSQL is already part of the stack. Service
  containers cost nothing on GitHub-hosted runners.
- **Alternatives:** run them only locally (the rules could break unnoticed between local runs).
- **Effect on the CI/local test split above:** PostgreSQL moves to "CI and locally"; only tests
  needing `data/raw/olist/` stay local.

## 2026-11-22 · Bronze: replay-time ingestion, events before manifest, raw lines kept

- **Status:** proposed
- **ingested_at is replay time, not wall-clock time:** the run for business date D stamps
  `D + 1 day 00:00:00`, just after the day closes on the replay clock. A retry then produces
  identical rows (the idempotency test compares checksums), and "rebuild as known at time T"
  means the same thing on every run. Alternative: the laptop's clock (every retry would differ).
- **Times are TIMESTAMP_NTZ:** Olist's and the replay's timestamps carry no zone and must never
  be converted. Spark 3.5's `TIMESTAMP_NTZ` is stored by Iceberg as `timestamp` (without zone),
  so nothing shifts them; the session zone is also pinned to UTC as a second guard. Alternative:
  `TIMESTAMP` (with local zone), where a Python datetime literal is silently shifted through the
  machine's zone.
- **Events are appended before the manifest:** two tables mean two commits. If a run dies
  between them, the retry finds the file's lines already in bronze, skips them, and writes only
  the missing manifest row. Manifest first could record a file as loaded that never was, losing
  it for good. Each day's lines go in one append, which Iceberg commits atomically.
- **Bronze keeps the raw line:** `raw` is the exact line received, with `source_file` and
  `line_number`; only `event_id` and `event_type` are read out, for auditing. All typing happens
  in silver, so a malformed line still reaches bronze and can be quarantined with its reason
  instead of disappearing at ingestion.
- **Only listed files load:** a file on disk that the marker doesn't list is reported as
  "unlisted" and never loaded; a listed file that isn't there is reported as "missing". A file
  whose checksum differs from the marker stops the run. Phase 3 turns these into the
  completeness check's Warn and Block results.
- **Local paths:** `lake/landing/` and `lake/warehouse/` stand in for S3 and are git-ignored.

## 2026-11-22 · extract_dimensions: upper bound at replay time, mark derived from bronze

- **Status:** proposed
- **Upper bound:** besides the spec's lower bound (updated_at later than the high-water mark
  minus one day), the extract reads only rows with updated_at earlier than the run's replay
  time (`D + 1 day 00:00:00`). The generator can run ahead of the pipeline, so without the bound
  day 1's extract would see rows written on day 60 and the replay would leak the future. In
  production the bound is simply "now".
- **The high-water mark is derived, not stored:** it is the latest updated_at already in the
  bronze extract table, so there is no separate state to get out of step with the data, and a
  failed run can't advance it.
- **Re-read rows are skipped:** the one-day overlap re-reads rows on purpose. Rows whose
  (natural key, updated_at) bronze already holds are not appended again, so a retry appends
  nothing and bronze keeps one row per state of each master row.
- **Values travel as text:** PostgreSQL returns every column as text and Spark casts it, so no
  driver-side conversion (time zones included) can change a value; zip prefixes stay codes with
  their leading zeros.
- **Tables:** `bronze.customers_extract`, `bronze.sellers_extract`, `bronze.products_extract`,
  each with `extracted_at` (replay time) and `run_id`.
- **Alternatives:** an unbounded extract (leaks future rows in a replay); storing the mark in a
  table of its own (one more thing to keep consistent); appending re-read rows and leaving
  deduplication to silver (bronze grows with copies, and a retry is no longer a no-op).

## 2026-11-22 · Silver: contract-driven checks, insert-only MERGEs, events written last

- **Status:** proposed
- **What a run reads:** bronze lines ingested after the latest `ingested_at` in `silver.events`,
  up to the run's replay time. Phase 1 has no releases, so this stands in for the spec's
  "last published watermark"; Phase 3 swaps in the release manifest's watermark. A run after a
  gap catches up on every day it missed.
- **Checks come from the contract:** `jobs/contract.py` reads `contracts/events_v1.json` and
  builds the parsing schema and every check from it: invalid JSON, unknown schema version or
  event type, missing required field, a value that can't be read as its type (array elements
  included). A failing line goes to `silver.quarantine` with all its reasons, e.g.
  `["missing:customer_id"]`. This is the spec's schema-contract check; its Block threshold
  (0.5% of a day's events) comes with DataSentinel in Phase 3.
- **Typed silver tables:** `silver.events` (every accepted event version, with its three clocks),
  `silver.orders`, `silver.order_items`, `silver.order_payments` (from order_placed) and
  `silver.reviews`. Milestone times stay in `silver.events.event_time`.
- **Deduplication before every MERGE:** within the batch the first copy of each event_id received
  is kept; against silver an anti-join drops what is already there. The MERGEs are insert-only,
  because an event_id names one immutable event version; a correction is a new version with its
  own event_id (Phase 2).
- **Order of writes:** order tables and quarantine first, `silver.events` last in one commit. The
  read mark comes from `silver.events` alone, so a crash midway makes the retry read the batch
  again, and the insert-only MERGEs fill in only what is missing.
- **Alternatives:** read `ingested_at = run time` exactly (a skipped day would never be read);
  a mark from events and quarantine together (a crash between the two writes could skip
  events); MERGE with WHEN MATCHED UPDATE (nothing should ever change an event version).

## 2026-11-22 · Payments with 0 installments or type not_defined are flagged, not quarantined

- **Status:** agreed (owner, 22 Nov 2026: option 1)
- **Finding:** the spec's DataSentinel check "Valid values" quarantines rows with fewer than 1
  installment or an unknown payment type. The Kaggle data has 2 credit-card payments with 0
  installments (on delivered orders, 2018-04-22 and 2018-05-15) and 3 payments of type
  `not_defined` with value 0.00 (on canceled orders, Aug and Sep 2018). The Real defects table
  lists neither. Quarantined, they would be missing from fact_payments and the truth test would
  fail against Olist. None fall in the Phase 1 slice.
- **Decision:** both become Real defects with severity **Flag**: the rows stay, `dq_flags`
  marks them, and only a day whose rate jumps far above the baseline blocks a run. It is how the
  spec already treats the 9 zero-value payments, and the truth test stays an exact comparison
  with what Olist recorded. The valid-values check still quarantines anything else outside its
  rules (price not above 0, review score outside 1 to 5, a payment type Olist never uses).
- **Alternative rejected:** quarantine them and have the truth test's reference build drop them
  too (the truth test would no longer compare with what Olist recorded).
- **Spec sections changed:** Data sources (Real defects table: two new rows); DataSentinel
  (Checks: Valid values names the two flagged exceptions).

## 2026-11-22 · build_dimensions: recompute, then MERGE only what changed

- **Status:** proposed
- **Recompute and compare:** each run derives every dimension from bronze extracts and silver as
  the run could see them (rows extracted by its replay time), then MERGEs only rows that differ
  from gold, compared null-safely. An unchanged rerun commits nothing. Recomputing the SCD Type 2
  chain each time is what puts a late address version (scenario 6) in the right place: the chain
  is derived again, so effective_to values around it are fixed in the same run.
- **Surrogate keys:** `jobs/keys.py`: first 8 bytes of SHA-256 over the natural key parts
  (joined with 0x1F), read as a signed BIGINT, computed in Spark SQL; a Python twin checks it.
  dim_customer keys on (customer_unique_id, effective_from), geography on (zip, normalised city,
  state), sellers and products on their ids.
- **Geography:** every delivery address in silver.orders plus every seller address; cities are
  normalised (lower case, single spaces, no accents, ´ as ') so "São Paulo " and "sao paulo" are
  one place. Region comes only from the IBGE state map.
- **Products:** English category from Olist's translation file, read directly as static
  reference data from `Context.reference_dir`; "unknown" for the 610 products without a
  category; the 2 untranslated categories keep their Portuguese names.
- **Calendar:** 2016-01-01 to 2019-12-31, with Black Friday 2016, 2017 and 2018 as sale days.
- **Not yet:** inferred members (`is_inferred` is always false until Phase 2's early-arriving
  facts) and re-keying facts after a late version (Phase 2, with build_facts).
- **Cost to fix later:** the 60-day slice now takes about 5 minutes, mostly recomputing all
  32,951 products and the calendar every day. Recomputing only when the inputs changed is a
  straightforward optimisation.
- **Alternatives:** incremental SCD Type 2 MERGE logic (closing and opening versions row by row:
  more code, and late versions need special cases); keys from Python's hash() (not stable).

## 2026-11-23 · build_facts: rebuild touched orders whole; the ledger holds the mark

- **Status:** proposed
- **What a run rebuilds** (superseded on 24 Nov: new events are found by event id; see "A blocked
  run's batches are held"): the orders with events silver received after the latest
  `ingested_at` in `gold.fact_order_events`, up to the run's replay time. Each touched order's
  rows are derived again from all of its current events, and merged only where they changed
  (`jobs/merge.py`, shared with build_dimensions). fact_order_events is written last, so a run
  that dies midway leaves the mark where it was; the rerun rebuilds the same orders.
- **Milestones and status:** each milestone comes from the current (highest) version of its own
  event type, whatever order events arrived in. Status: cancelled and unavailable win, then
  delivered, else open (`final_status` null, `is_open` true).
- **Lags:** elapsed days with the time of day (seconds / 86,400) from purchase to approval,
  approval to carrier, carrier to delivery and purchase to delivery. A lag is left empty when a
  flag says one of its timestamps is out of order (Real defects table: "left out of the lag").
  `days_early` is in calendar days, as question 2 defines it.
- **dq_flags on the snapshot:** carrier_before_approval, carrier_before_purchase,
  delivered_before_carrier, delivered_without_date, delivered_never_approved,
  canceled_with_delivery_date, no_items (not canceled or unavailable), no_payments,
  payment_mismatch (payments and items plus freight differ by more than 1 BRL). On fact_payments:
  zero_value, zero_installments, type_not_defined.
- **Keys:** product and seller keys come from the ids themselves, not a dimension lookup, so a
  fact gets the right key before its dimension row exists (Phase 2's inferred members use the
  same key). customer_key is the version effective at purchase; the delivery geography key is
  built from the order's own address.
- **Not yet (Phase 2):** dirty periods, re-keying facts after a late address version, removing
  item rows if a correction ever drops an item.
- **Alternatives:** rebuild every order every run (simple, but 99,441 orders every day on the
  full replay); MERGE each event as it comes (the result would depend on arrival order).
- **Spec section:** Data model could list the lag definitions and the snapshot's flag names.

## 2026-11-23 · First aggregate, serving load and dashboard

- **Status:** proposed
- **Aggregate:** `gold.agg_delivery_by_month_region` (questions 1 and 2) at the grain of
  measure_spec.py's `q1_q2_delivery_by_month_region.csv`: delivery month x delivery region plus
  an `all` slice. Percentiles interpolate linearly, as pandas does. Phase 1 recomputes it whole;
  it is a few hundred rows.
- **load_serving:** copies the gold aggregate into a staging table, then one PostgreSQL
  transaction MERGEs it into `serving.delivery_by_month_region`, deletes rows gold no longer
  has, and updates `serving.load_state` (run and business date). The serving DDL lives in
  `jobs/load_serving.py` for now; the spec's layout puts it in `sql/`, which is where it moves
  when releases arrive in Phase 3.
- **Dashboard:** `dashboard/app.py` (Streamlit 1.67.0), reading only `serving`. The spec's
  layout names no Phase 1 dashboard folder; the Phase 3 console goes in `sentinel/console/`.
- **Dependencies:** `duckdb==2.0.0` in `[dev]` for the truth test's reference build, and
  `streamlit==1.67.0` in a new `[dashboard]` extra, since it pulls in pandas. Both are tools the
  spec names; 2.0.0 was chosen over 2.0.1 because 2.0.1 was three days old.
- **Spec section:** Architecture, Repository layout (add `dashboard/`).

## 2026-11-23 · The Phase 1 truth test

- **Status:** proposed
- **What:** `tests/truth/test_truth_slice.py` replays the slice into a throwaway database and
  namespace, runs all 60 days through `run_day`, and compares fact_order_fulfillment,
  fact_order_items, fact_payments, fact_reviews, dim_customer, dim_geography, dim_seller,
  dim_product and the aggregate with `tests/truth/reference.py`, row by row on each grain. The
  reference is DuckDB SQL over the Olist CSVs, with its own surrogate-key macro and city
  normalisation, applying the day-60 cut-off from the spec's written emission rule.
- **Idempotency:** the first, a middle and the last day are each run twice; the second run must
  leave a row count and content hash of every bronze, silver and gold table unchanged and write
  nothing.
- **Markers:** `olist` and `postgres`. It skips without the data unless CHRONOS_REQUIRE_OLIST=1.
- **Not yet:** part 2 (event ledger against the ground-truth manifest) and part 3 (publication
  history) need Phase 2's defects and Phase 3's releases. Rerunning an *old* day after later
  days have run is not a retry: dimensions would be recomputed as that day knew them. Phase 2's
  Airflow backfill runs dates in order; what clearing an old task instance should do is
  decided there.

## 2026-11-23 · One seller city in decomposed Unicode (found by the truth test)

- **Status:** proposed
- **Finding:** one seller's city is "são paulo" written as `a` + U+0303 (combining tilde) rather
  than the single letter `ã`. The accent map didn't cover combining marks, so the pipeline made
  it a second São Paulo in dim_geography; the DuckDB reference (`strip_accents`) didn't. The
  truth test caught it as 1 missing and 1 extra geography key out of 3,916.
- **Fix:** `normalise_city` also removes Unicode combining marks (`\p{M}`), and the geography
  unit test includes the decomposed spelling.
- **Alternative:** full Unicode NFKD normalisation (Spark 3.5 has no built-in; a Python UDF would
  slow every run for one row).
- **Spec section:** Data sources, Real defects: could list "city names in decomposed Unicode:
  1 seller; None: normalised".

## 2026-11-23 · Two bugs the fixture truth test found

- **Status:** agreed (bug fixes: the spec asks for the right answer)
- **The replay must run one day ahead of the pipeline, not all at once.** The truth harness
  wrote all days' files and master rows first, then ran the pipeline. The customer master only
  holds the current address, so day 1's extract already saw the person's final address and the
  first SCD version was lost (person_a in the fixture). `generator.replay` now takes
  `after_day`, called once each date is written, and the truth test runs `run_day` there. The
  pipeline still never imports the generator. The real slice has no address changes, which is
  why only the fixture showed it.
- **Aggregates now delete rows a recompute no longer produces.** An order delivered on 10 Jan
  and cancelled on 11 Jan left a stale North row in the aggregate, because the MERGE only
  inserted and updated. `merge_changed(..., delete_missing=True)` deletes them; it is used for
  tables recomputed whole. Dimensions don't need it: what a run can see only grows.

## 2026-11-23 · Phase 2: the Airflow image and DAGs

- **Status:** proposed (the image and DAG were agreed for the start of Phase 2; these are the
  details)
- **Image:** `airflow_local/Dockerfile`, on `apache/airflow:3.4.0-python3.11`. Java 17 is
  copied from `eclipse-temurin:17.0.21_9-jre-noble`: the Airflow image's Debian release has no
  OpenJDK 17 package (only 21), and the newer 17.0.21.1 Temurin tag has no arm64 build.
  PySpark 3.5.6 is installed against Airflow's 3.4.0 constraints file. The Iceberg 1.10.0 jar
  is downloaded at build time and checked against Maven's SHA-1, and `CHRONOS_SPARK_JARS`
  points Spark at it.
- **Code and lake:** `jobs/`, `generator/`, `contracts/`, `sql/` and the Olist folder are
  mounted read-only at `/opt/chronos`. The container's lake is `lake/airflow/`, separate from
  the laptop's: the Iceberg Hadoop catalog stores absolute paths.
- **`chronos_daily`:** one task per step function, no logic of its own. `schedule=None`;
  `max_active_runs=1`; every task `depends_on_past` and `wait_for_downstream`; retries twice
  with backoff. `wait_for_landing` retries every 5 minutes for an hour instead of being a
  sensor.
- **`chronos_replay`:** plays the source system for a date, then triggers `chronos_daily` for
  that date and waits. Bounded `@daily` schedule (2016-09-04 to 2018-09-30).
- **Found while testing:**
  - An Airflow 3 backfill applies its own `--max-active-runs`, not the DAG's. Without
    `depends_on_past`, three dates ran at once.
  - An unbounded `@daily` made the scheduler create a run for today's wall-clock date.
- **Verified:** a 7-date backfill (`--max-active-runs 1 --reprocess-behavior failed`) ran in
  order, about a minute per date.
- **Not yet (Phase 3):** `open_run`, `sentinel_audit`, `publish_release`, `notify`.

## 2026-11-23 · Phase 2: reversible defects and the ground truth

- **Status:** proposed
- **Generator:** `generator/defects.py`, at the spec's suggested rates. Each decision is a
  SHA-256 of (seed, defect, event or order), so the same event gets the same defect in every
  replay and any slice. Tests can script defects exactly with `Defects.only({...})`.
- **Choices the spec leaves open:**
  - Each event gets at most one timing defect, in the order correction, late, out of order,
    duplicate, so delays can't add up.
  - A wrong delivery time is one day later than Olist's. A wrong payment value is 10% higher
    (10.00 when Olist's is 0). The correction arrives 1 to 3 days later.
  - Duplicates are resent 1 to 48 hours later.
  - Out of order: the carrier handover is sent 1 to 48 hours after the delivery event.
  - Held-back sellers: 0.2% of sellers, inserted 1 to 3 days after their first order in the
    replay.
- **Drain:** `DRAIN_DAYS = 11` (10-day late events plus a late file's day). After `feed_end`
  nothing new is emitted.
- **Ground truth:** `replay(..., manifest=path)` writes every injected defect as JSON outside
  the landing folder. Tests use it and `generator.replay.plan`; pipeline code never does.
- **What the default rates produce:**

  | Defect | Jan–Feb 2017 slice | Full window (2017-01 to 2018-08) |
  | --- | --- | --- |
  | Late events | 201 | 9,636 |
  | Duplicates | 225 | 9,584 |
  | Corrections | 22 | 1,017 |
  | Out of order | 18 | 830 |
  | Early-arriving customers | 1 | 185 |
  | Late address changes | 0 | 18 |
  | Late files | 0 | 10 |
  | Held-back sellers | 1 | 7 |

  The spec expects "about 25" late address changes. 10% of the changes inside the window
  gives 18.
- **Deferred to Phase 3:** the announced schema change (a new optional field, then a rename
  under schema_version 2). It tests the schema contract, which Phase 3's DataSentinel owns, and
  none of the nine scenarios needs it.
- **Spec section:** Event model, Reversible defects, could record these parameters.

## 2026-11-23 · Phase 2: what the pipeline does with each defect

- **Status:** agreed (owner, 23 Nov 2026), with the additions in the next entry
- **Late files:** `register_files` also checks the previous 10 dates' markers for listed files
  not yet loaded. A bronze row's `business_date` is its file's folder, read from the path; its
  `ingested_at` is the run that received it. Missing and unlisted files are reported as paths,
  because several folders are checked.
- **Inferred members:**
  - A person with an order but no master row gets a placeholder: `is_inferred`, no address,
    effective from their first order. That is exactly the key the real version will have, so
    the real row overwrites it in place, with no fact rewrite.
  - A seller named by an item but not in the master gets a placeholder with no location.
  - Surrogate keys are now null when any part is null: `concat_ws` skips nulls, and the
    placeholder's unknown location was getting a real-looking geography key.
- **Re-keying:** each run also rebuilds orders whose fact keys no longer match the current
  dimensions: the customer version effective at purchase changed (a late address change), or a
  seller's location became known. **Spec mismatch:** scenario 5's test says "with no fact
  rewrite". That holds for customers, but `fact_order_items.seller_geography_key` must be
  rewritten when a placeholder seller becomes real, because the spec puts the seller location
  key on the fact. The spec's own Kimball note (Design Tip #78) anticipates this.
- **Dirty periods:** before merging the snapshot, `build_facts` records in `gold.dirty_periods`
  the delivery months of every changed row, before and after the change, for questions 1 and
  2. `recompute_aggregates` recomputes only those months, each one whole, and deletes rows that
  disappear inside them.
- **Published metrics:**
  - `gold.fact_daily_metrics_published` is append-only, on the spec's grain, with
    `release_id = run_id` until Phase 3 adds releases. Metric ids are `<metric>.<measure>`
    from `metrics.yml`, such as `q1_late_delivery_rate.late_rate`.
  - `gold.agg_restatements` (question 8) holds first against latest published value, with
    numerators and denominators. It is served as `serving.restatements` and shown on the
    dashboard.
- **Spec mismatches to settle:**
  - Scenario 8 expects "main ... unchanged" after a crash. Without Phase 3's branches, gold is
    partly written. The Phase 2 test checks that serving is unchanged and that the rerun ends
    identical to an uninterrupted run; "main unchanged" is tested once runs write to branches.
  - Scenario 9: the test is a catch-up after a 4-day gap (no runs on 6 to 9 Jan, then
    continuing), which must equal running daily. The serial Airflow backfill itself was
    verified by hand, not in pytest.
  - Phase 2's "Done when" says the truth test passes in CI. CI runs it on the fixture with
    heavy defects; the real slice runs locally (the test split proposed on 22 Nov).
- **Open:** clearing an *old* date in Airflow after later dates have run re-derives dimensions
  as that date knew them. `depends_on_past` stops it from running early, not from running at
  all. To decide in Phase 3, with releases: a rerun of an old date should probably republish
  nothing.

## 2026-11-23 · The four Phase 2 spec mismatches, settled

- **Status:** agreed (owner, 23 Nov 2026: "Accept all four recommendations, with these
  additions")
- **1. Scenario 5, seller re-keying.**
  - Customers: no fact rewrite. The placeholder has the real row's key.
  - Sellers: `fact_order_items.seller_geography_key` is rewritten when a placeholder seller
    becomes real, and the rewrite goes through the dirty-period list.
  - `dirty_periods` now covers changed item rows too: each changed item dirties question 3's
    shipping-limit month, before and after the change, as `metrics.yml` defines. Question 3's
    aggregate comes later; its dirty months are recorded from now on.
  - The scenario 5 test asserts that the only existing rows that change are that seller's
    items, and that question 3's month is dirty for that run.
- **2. Scenario 8, "main unchanged".** Phase 2 tests that serving is unchanged and that the
  rerun finishes identical to an uninterrupted run.
  - `test_8_a_crash_halfway_leaves_main_unchanged` asserts gold is unchanged after the crash.
    It is marked `xfail(strict=True)` with the reason, so it fails loudly once branches make it
    pass, and someone has to remove the marker.
  - It is part of Phase 3's "Done when". The crash day moved to 15 Jan: on 12 Jan nothing
    changed, so the xfail passed by accident (caught by `strict`).
- **3. Scenario 9.**
  - Tested as a catch-up after a 4-day gap.
  - The serial Airflow backfill was checked by hand on 7 dates. The run list from Airflow's
    metadata database is in `docs/phase-2-notes.md`.
- **4. CI and local test split.** Accepted (entry of 22 Nov):
  - CI runs the fixture truth test with heavy defects.
  - `make truth` runs the real slice locally.
  - Each phase's notes have a "Truth test, last passed" table. `Makefile` is new; the owner
    asked for `make truth`.
- **5. Rerunning an already-published date.**
  - It republishes nothing. It logs "already published in release X" and exits successfully,
    without blocking.
  - Logic bugs are fixed by a full rebuild: keep bronze, reset silver and gold, and replay every
    date in order.
  - Documented in `docs/runbook.md`. The rebuild is tested
    (`test_a_full_rebuild_from_bronze_gives_the_same_tables`): from a copy of a defected run's
    bronze it gives exactly the clean run's business tables.
  - The skip itself is built in Phase 3, because it needs the release manifest to know what was
    published.
- **Spec sections changed:**
  - Late-arriving data (scenarios 5, 8 and 9)
  - Testing and evaluation (CI and local)
  - Orchestration (reruns and rebuilds)
  - Phased build plan (Phase 2 and 3 "Done when")

## 2026-11-23 · Two bugs the Phase 3 release tests found

- **Status:** agreed (bug fixes: the spec asks for the right answer)
- **Great Expectations cached what it checked, and later runs read that cache.**
  - What happened: GX's Spark data source calls `persist()` on every DataFrame it validates by
    default, and never releases it. Spark then answered later reads of the same table from that
    cache, even reads of another branch or snapshot id.
  - The effect: the 5 Jan run wrote 2 orders to silver on its branch, but its own reads still
    saw 4 Jan's cached, empty silver. So `build_facts` built nothing, and the audit compared
    0 with 0 and passed. The publish then moved silver forward without gold. The order ledger
    already held the events, so later runs never rebuilt those orders.
  - How it was found: only the multi-day release tests showed it. Reading the branch snapshot
    by its id returned 0 while the snapshot's summary said 2 records. That pointed at Spark's
    cache, not at Iceberg.
  - The fix: `add_spark(..., persist=False)` in `sentinel/checks/rules.py`.
    `tests/unit/test_rules.py` replays two daily runs, and fails ("day 2 reads a stale table")
    with persist left on.
  - Alternatives: clearing Spark's cache after the audit (it treats the symptom, and any later
    check that caches would bring the bug back), or not using GX (the spec names it).
  - Lesson: a check must never change what later reads see. A library that caches is a hidden
    write to shared state.
- **An open dashboard read could stall a run indefinitely.**
  - What happened: `ensure_serving_schema` ran `ALTER TABLE serving.load_state ADD COLUMN IF
    NOT EXISTS release_id` on every load. PostgreSQL takes an exclusive lock for that even when
    the column exists, so it queued behind any open transaction that had read the table. In the
    test that was the test's own connection, so the test hung for two hours.
  - The fix: `release_id` is part of `CREATE TABLE`. The `ALTER` runs only on databases made
    before Phase 3, and only with `lock_timeout = 5s`, so a run fails with an error instead of
    hanging. The release tests now close each read's transaction.
- **Spec sections:** none change. Both are implementation bugs; the design (reads follow
  `spark.wap.branch`) holds, as the Phase 0 smoke test showed.

## 2026-11-23 · Volume and freshness warn until they have 8 weeks of history

- **Status:** agreed with the owner
- **What:** the same-weekday checks (volume, freshness) only warn until they have all 8
  same-weekday points. Other checks keep the 4-week warm-up.
- **Why:** the real-slice truth test was blocked on 29 Jan by volume. Event lines per day in
  that slice:
  - 1 to 4 Jan: 0, because the slice starts with an empty backlog.
  - Mid-Jan to mid-Feb: Olist really grew, and the same weekday rose 50 to 100% week on week.
  - Modified z against 4 to 7 points from that ramp reached 22. Six days would have blocked.
  - From about 12 Feb every day is below 1.5. The days with all 8 points score -0.3 to 0.9.
  - With one point a week, "4 weeks of history" is only 4 points: too few to judge a series
    that is still growing.
- **Alternatives:**
  - Keep the spec and make the truth test treat anomaly checks as warnings only. Rejected: it
    would hide exactly this kind of false alarm.
  - Model the trend (for example, compare with a growth-adjusted median). Rejected for now:
    more to explain, and the evaluation replay will show whether it is needed.
- **Code:** `volume_decision` in `sentinel/anomaly/scores.py` uses `config.VOLUME_WEEKS`.
  `tests/unit/test_scores.py` checks that 7 weeks still only warns.
- **Spec section changed:** DataSentinel, "Anomaly detection" (Warm-up).

## 2026-11-23 · Known-defect checks use a binomial test, not a modified z-score

- **Status:** agreed with the owner
- **What:** every known-defect check scores the day's defect count with the binomial tail at the
  pooled 28-day baseline rate (floored by the rule of three), and blocks below 0.001. Before,
  this was only the fallback when the modified z had no spread. Modified z stays for volume.
- **Why:** after the volume fix, the real slice blocked on 8 Feb on `known_defect:no_items`.
  - The orders are real Olist `unavailable` orders: 0 to 6 a day out of 40 to 110.
  - A modified z on daily rates ignores how many orders each rate rests on. Its median and
    spread jump with single orders: 5 of 95 scored 7.3, and 4 of 49 on 11 Feb scored 11.7.
  - The binomial tail gives about 0.04 and 0.02 for those days: ordinary.
  - Over the whole dataset (576 days with a 28-day baseline), the binomial test flags 6 days
    for no_items. Three are 28 to 30 Aug 2018, the last days of the data. The evaluation's
    tuning period sets the final limit.
- **Alternatives:** keep modified z but score only days with a minimum number of defects.
  Rejected: an ad-hoc cut-off, and it still ignores the order count.
- **Code:** `defects()` in `sentinel/checks/anomalies.py`; `DEFECT_Z_LIMIT` removed from
  `sentinel/config.py`. `tests/unit/test_anomalies.py` checks 8 Feb's numbers pass, a real spike
  blocks, and no baseline only warns.
- **Spec sections changed:** DataSentinel, Checks table ("Known source defects") and "Anomaly
  detection" (the no-spread bullet is replaced); Known limitations (defect-rate baselines).

## 2026-11-23 · Freshness publishes the day as incomplete; it never blocks

- **Status:** agreed (the spec asks for it; the code had drifted from the spec)
- **What:** the spec's action for Freshness is "Block publishing the day as complete; dashboards
  show staleness". I had built it as a block of the whole release. Now a freshness failure is a
  warning. The release is published with `complete = false` in `metrics.releases`, as for a
  listed file that is missing (`INCOMPLETE_CHECKS` in `sentinel/release.py`).
- **How it showed:** the real-slice truth test passed every day of Jan and Feb 2017, then
  blocked on 2 Mar. That is the first drain day, when only late events about older orders
  arrive, so the data really is stale.
- **Still to do (Phase 3 console):** show `complete` on the dashboards.
- **Noted for the evaluation:** in the drain, volume fell to 0 lines and still scored only about
  -2.5. The January ramp makes the same-weekday spread wide. Missing files are caught first by
  the marker check; the evaluation replay measures how much this weakness matters.
- **Spec sections:** none change; the code now follows the Checks table.

## 2026-11-23 · No `depends_on_past` in chronos_daily; open_run guards a half-finished publish

- **Status:** proposed
- **What:**
  - `chronos_daily` no longer sets `depends_on_past` and `wait_for_downstream` (added in
    Phase 2). Dates stay serial through `max_active_runs = 1` (the spec's rule) and, for a
    backfill, `--max-active-runs 1`.
  - `publish_release` marks the release `publishing` before main moves.
  - `open_run` raises `PublishUnfinished` while any release in the namespace is `publishing`.
  - `chronos_replay` treats a failed `chronos_daily` as done (`allowed_states` includes
    failed), so the producer keeps producing.
- **Why:**
  - The first Phase 3 Airflow run (8 Jan, on the Phase 2 lake) hung at `open_run`. Airflow: "the
    previous task instance has not run yet". `open_run` didn't exist on 7 Jan, so a task added
    to a `depends_on_past` DAG waits forever.
  - Worse, with releases `depends_on_past` is wrong. A blocked date fails `sentinel_audit`, which
    would stop every later date. The spec says the next date picks the blocked date up.
  - Any failure before publishing leaves main untouched, so the next date can safely start
    from it. The one failure that must stop later dates is a publish that died between
    fast-forwards: main is then half moved. The `publishing` status catches exactly that case.
- **Alternatives:**
  - Keep `depends_on_past` and mark new tasks as done for old runs by hand. Rejected: it has to
    be repeated whenever a task is added, and blocked dates would still stop everything.
  - `depends_on_past` on `publish_release` only. Rejected: the next date's earlier steps would
    still branch from a half-moved main.
- **Tests:** `test_a_crash_between_fast_forwards_leaves_serving_and_completes_on_rerun` now
  checks that the release is `publishing` and that the next date's `open_run` refuses to start.
- **Docs:** `docs/runbook.md` (a failed day, a blocked day, a publish that died midway);
  `docs/phase-2-notes.md` has an update note.
- **Spec sections:** none change. The spec already says runs are serial through
  `max_active_runs = 1` and that the next date picks a blocked date up.

## 2026-11-24 · Faster audits: one GX suite per table, and maintenance_weekly built now

- **Status:** proposed
- **Why now:** a run took 27.6 s on day 46 of the real slice and kept growing. At that rate the
  evaluation replay would take more than a day. Per step on 15 Feb:
  - GX key checks: 10.5 s
  - build_facts: 4.0 s
  - everything else: about 13 s
- **Measured on that warehouse (all 21 tables):**
  - GX, one validation per expectation: 9 to 11 s.
  - GX, one suite per table: 3.8 s.
  - The same checks in plain Spark: 1.5 s.
  - Compacting every table halved both (GX 5.3 s, plain 1.1 s). Each table had 42 data files
    and 225 metadata files after 46 days.
- **What:**
  - `grain_checks` validates one ExpectationSuite per table. The check names are unchanged.
  - `jobs/maintenance.py` builds the spec's maintenance job: compact data files and
    manifests, expire snapshots that no tag or branch needs, remove orphan files. It reports
    what each job changed per table.
  - It runs after every replayed Sunday: from `jobs/run_day.py`, and in Airflow as the
    `maintenance_weekly` DAG triggered by `chronos_replay`.
- **Two choices the spec left open:**
  - *Snapshot expiry.* The spec says "older than the restatement window" without giving a
    number. Every release is a tag kept 10 years, so every release stays readable whatever the
    expiry. Iceberg stamps snapshots with wall-clock time, so an age cut-off would expire nothing
    during a replay. So expiry removes every snapshot no ref needs (`retain_last` 1).
  - *The lock.* The spec says maintenance "shares a one-slot pool with the daily DAG". A pool
    holds one task, not a whole run, so maintenance could start between two tasks of an open
    run. So `maintain()` also refuses (`LakeBusy`) while any run branch exists or a release is
    `publishing`. The `chronos_lake` pool holds only `open_run` and the maintenance task, so
    no run opens while maintenance commits.
- **Alternatives:** plain Spark instead of GX for the key checks (faster still, but the spec
  names GX); checking only the keys a run touched (faster, but harder to defend than "the
  whole table that would be published").
- **Tests:** `test_maintenance_keeps_every_release_and_refuses_during_a_run`.
- **Spec updated:** Orchestration, maintenance_weekly: how expiry works with tags, that it
  refuses while branches are open (besides the pool), blocked runs' branches, and when it runs.

## 2026-11-24 · Release test fixture: the producer writes each date just before its run

- **Status:** agreed (owner, 24 Nov 2026: fix the placeholder edge case)
- **What:** `tests/scenarios/test_releases.py` used to write all 10 days of the replay first.
  On 7 Jan that gave `person_a` two current dim_customer rows, and the audit blocked the day
  (duplicate fact rows; reconciliation off by one):
  - the 5 Jan extract couldn't see the customer row, which already held the 7 Jan address
    with a 7 Jan replay time, so the 5 Jan order used an inferred placeholder;
  - the real row then arrived effective from 7 Jan, with a different key.
  The fixture now writes each date just before running it, like the truth test.
- **The edge case behind it, fixed (agreed with the owner):** a real source can do the same. If
  an early-arriving customer's address changes again before their row is extracted, the real
  row starts later than the first order, and the old placeholder stayed current next to it.
  The audit blocked such a day, so nothing wrong was published.
  - Now `customer_versions` keeps a placeholder for the gap whenever a person's first order is
    earlier than their first known address. The chain closes it where the real row starts.
  - The placeholder has the same key, so the MERGE updates the old row in place. The early
    order stays attached to it: no address is known for that period.
  - Test: `test_a_late_customer_row_that_starts_after_the_first_order_closes_the_placeholder`.
  - Spec updated: Data model, "Inferred members".

## 2026-11-24 · A blocked run's batches are held until someone releases or rejects them

- **Status:** agreed with the owner
- **What:**
  - A batch is what one run landed in bronze, named by its ingested_at.
  - When the audit blocks, the gate holds every batch the run processed
    (`metrics.batch_holds`). Later runs leave held and rejected batches out.
  - `python -m jobs.batches release|reject <date>` decides. A released batch is taken in by the
    next run, then marked `published` with that release.
  - Volume is now judged on the lines the run itself landed (the spec's "each ingest day"),
    not on the whole batch.
- **Why:** the earlier rule, "the next run picks the blocked day's data up", only works for causes
  that go away on their own (a stray file, which is never loaded anyway). The evaluation's
  anomalies leave bad data in bronze. Taken in again, it either:
  - blocks every later run (a broken schema is 50% of two days' lines, 33% of three; volume
    grows with every day), or
  - is published a day late without being judged again, against the evaluation's target that
    no bad data reaches a release.
- **Alternatives:** next run judges only its new data (rejected: publishes blocked data
  unjudged); re-judge everything (rejected: the cascade above).
- **Cost:** a person must decide on each block. In the evaluation nobody does, so the report
  also counts the good data that false alarms held back: the real price of a false alarm.
- **Code:** `jobs/batches.py`; `bronze_to_silver.read_batch`; `sentinel/checks/batch.py`
  (`window`, `ingest_day_lines`); the gate (`hold`); `publish_release` (`mark_published`).
- **build_facts finds new events by event id, not by a time mark.** A released batch is older
  than events gold already holds, so the 23 Nov rule ("events silver received after the latest
  `ingested_at` in `fact_order_events`") would skip it for good. Now `touched_orders` takes the
  silver events, up to run time, that `fact_order_events` doesn't hold (a left anti-join on
  `event_id`). The ledger is still written last, so a crash midway still redoes the same
  orders. This supersedes the "What a run rebuilds" line of the 23 Nov build_facts entry.
- **Tests:** `test_a_blocked_day_is_held_until_released`; `tests/unit/test_batches.py`.
- **Spec sections changed:** Publishing ("What a run processes"), the Checks table (Volume),
  the decision summary. Runbook: "The audit blocked a day".

## 2026-11-24 · Landing problems are reported once, when they are new

- **Status:** proposed (found by the evaluation's smoke run)
- **What:** the completeness check looks back over 10 days of landing folders, so it can see
  late files. It used to report everything it saw on every run. Now:
  - an unlisted file blocks only the run that first sees it, and its batch is then held;
  - a missing listed file warns only on its own date, so only that date's release is marked
    incomplete;
  - a file still missing 2 days later alerts once, that day.
- **Why:** in the smoke run, one duplicate file sent under an unlisted name on 16 Jan blocked
  every run from 16 to 22 Jan with nothing new wrong. It was the same cascade as blocked
  batches, this time in landing. A single missing file also marked every later release
  incomplete. The spec's wording is per event: "Warn and publish with the date marked
  incomplete, and alert if it is still missing after 2 days".
- **Code:** `landing()` and `reported_unlisted()` in `sentinel/checks/rules.py` (earlier
  reports come from `metrics.check_results`).
- **Test:** `test_a_blocked_day_is_held_until_released` now leaves the stray file in place;
  6 Jan still publishes.

## 2026-11-24 · Maintenance ignores a blocked run's kept branches

- **Status:** proposed (found by the evaluation's smoke run)
- **What:** `maintain()` refused while any run branch existed. A blocked run keeps its branches
  for inspection, so after the first block maintenance never ran again. Now it ignores the
  branches of runs recorded as `blocked`; they are dead ends, never fast-forwarded. Once every
  batch such a run held is decided, maintenance drops its branches.
- **Test:** `test_a_blocked_day_is_held_until_released` runs maintenance while 5 Jan is held,
  and again after the release, when the branches must be gone.

## 2026-11-24 · The evaluation replay: design and scoring rules, fixed before any results

- **Status:** proposed (decided before the test period ran; changing these after seeing test
  results would be tuning on the test set)
- **Isolation:** its own database (`chronos_eval`: source, serving, metrics), lake (`lake/eval/`)
  and namespaces (`eval_`). The harness is `tests/evaluation/evaluate.py`, under tests/ because
  it reads the ground-truth manifest.
- **Periods:**
  - Tuning: 1 Jan to 30 Sep 2017, reversible defects only. Thresholds may change here.
  - Test: 1 Oct 2017 to 31 Aug 2018, with weekends, Black Friday and Christmas.
- **Anomalies (`generator/anomalies.py`):** 30 of each of 6 types on 180 distinct test days,
  placed by SHA-256 draws (seed 13); the other 155 test days are clean. Black Friday stays clean.
  The types are missing files, a duplicate file under an unlisted name, a volume spike, a
  payment or price mix shift, a schema break and a null burst.
- **Nobody intervenes:** held batches stay held. The report counts the good data that false
  alarms held back.
- **Scoring:**
  - *Outcome per day:* blocked (a blocking check failed), warned (published, with a failing
    warn or alert), or passed.
  - *Detected:* a check meant for that type failed on the anomaly's own run. A run judges only
    its own batch, so later runs aren't credited (they would credit other anomalies' checks).
  - *False block:* a clean day that blocked.
  - *False warning:* a clean day with a failing warning that a late or missing file in the
    manifest doesn't explain.
  - *Bad data published:* any event id the anomaly added or changed is in published silver
    (main) at the end.
- **Known limitation:** the evaluation measures these six types only (spec: Known limitations).

## 2026-11-24 · Tuning: volume counts orders, with a floor on the spread

- **Status:** agreed (owner, 24 Nov 2026), on the tuning period only (1 Jan to 30 Sep 2017)
- **What the first tuning run showed:** 273 clean days; 12 blocked and 250 warned. Every false
  block was volume, on real calendar days: Good Friday (14 Apr), Mother's Day week (6 to 15
  May), Independence Day (7 and 8 Sep). Two causes:
  - *Lines are the wrong measure.* Most lines are follow-up events (approved, shipped,
    delivered, reviewed) for earlier days' orders, so on clean days they ranged from 0.49x to
    2.24x the same weekday's median. A duplicated feed of orders adds only 36% to 73% to the
    lines, inside that range.
  - *A tight history makes ordinary days extreme.* On 8 Sep the 8-week MAD was 8.5 around 803,
    so an 8% dip scored -6.3.
  - *A side effect:* a blocked day is never published, so it leaves the baseline, and the
    weekday then had 7 points and stayed warn-only for 8 weeks.
- **What changed (`sentinel/config.py`, `sentinel/checks/anomalies.py`):**
  - Volume is the number of new orders the run landed itself (`silver.events`, `order_placed`,
    `ingested_at` = run time). Orders are demand itself, and a released batch still doesn't
    count as a spike.
  - The MAD is at least 10% of the median (`VOLUME_MAD_FLOOR`), and the limit is |z| > 5.
  - History is the latest 8 published same weekdays within 12 weeks.
- **Result on the tuning profiles:** 0 false blocks (lines, 3.5, no floor: 24 when every day
  counts; 12 in the real run). Orders on clean days ranged from 0.28x to 2.25x; a feed
  duplicated two to four times is 3x to 5x.
- **Alternatives:**
  - Keep lines and raise the limits: 1 false block, but most spikes are missed.
  - A holiday calendar: only 3 of the 12 days are holidays, and Mother's Day week is a week.
- **Honest note:** the measure was chosen knowing the anomaly catalogue (spec: Testing and
  evaluation). The held-out test period, never looked at, decides whether it works.
- **Spec updated:** Checks table (Volume row) and "Anomaly detection: methods and why".

## 2026-11-24 · Tuning: drift PSI on smoothed counts, thresholds per feature

- **Status:** agreed (owner, 24 Nov 2026), on the tuning period only
- **What the first tuning run showed:** category drift warned on 244 of 273 clean days (median
  PSI 0.68). A category absent today counted as a share of 1e-4, and with 71 categories the
  absent ones alone made the score.
- **What changed:**
  - PSI adds 0.5 to every bin's count first (`PSI_SMOOTHING`).
  - Thresholds come from the clean tuning distribution, per feature (`DRIFT_WARN`,
    `DRIFT_BLOCK`). The largest clean values were: payment type 0.41, price 0.36, review score
    0.22, category 1.0.
  - So payment type, price and review score warn from 0.25 and block from 0.5. Category warns
    from 1.25 and never blocks: a few hundred items over 71 bins is noisy by itself.
  - This is the spec's "Block from 0.25 once tuned", with the tuned values.
- **Alternatives:**
  - Merge small categories into "other": fewer bins, but another tuning choice to defend.
  - A sample-size-aware test (chi-square): a bigger change than the spec asks for.
- **Spec updated:** Checks table (Distribution drift row) and "Drift thresholds".

## 2026-11-24 · Alerts: recorded always, posted to Slack when a webhook is set

- **Status:** agreed (owner, 24 Nov 2026: "whatever's best")
- **What (`sentinel/alerts.py`):**
  - Every task's last failed try calls `task_failed` (Airflow `on_failure_callback`). It names
    the DAG, the task, the date and the error; a blocked audit's error lists the blocking checks.
  - `notify`, the last chronos_daily task (spec step 13), sends the success summary: the
    release, its warnings, whether it is complete, and any alert check (a file still missing
    after 2 days).
  - Each notice is stored in `metrics.notifications`, which the console shows. When
    `CHRONOS_SLACK_WEBHOOK` is set (in `.env`, never committed), it is also posted, with the
    console link.
- **Why:**
  - A webhook needs no account on our side and no new dependency (the standard library posts
    it).
  - Recording first means the console has every notice even without Slack.
  - A failed post is recorded, never raised: the data is safe either way.
- **Alternatives:** email needs an SMTP account and password; Airflow's Slack provider is
  another dependency for one HTTP POST.
- **Test:** `tests/unit/test_alerts.py`, against a local HTTP server.

## 2026-11-24 · The evaluation restarted from scratch after tuning

- **Status:** agreed (a record of what happened, not a choice)
- **What:** the evaluation's database and lake were dropped, and the tuning period was replayed
  with the tuned checks. Its 12 false blocks had held batches that nobody releases, and those
  would have changed what the test period saw.
- **First tuning run, for the record:** 11 passed, 250 warned and 12 blocked.

## 2026-11-24 · The announced schema change

- **Status:** agreed (spec: reversible defects, "Announced schema change", once each); the
  choice of fields and dates is proposed
- **What:**
  - *New optional field, from 1 Feb 2017:* order items carry `product_category_name`, Olist's
    real category for the product. The version 1 contract lists it as optional ("announced"),
    and `silver.order_items` stores it. Tables created earlier get the column by `ALTER TABLE
    ADD COLUMN` (`ADDED_COLUMNS` in `jobs/bronze_to_silver.py`): in Iceberg that is a metadata
    change only, and older rows read it as null.
  - *Schema version 2, from 15 Feb 2017:* `order_placed.customer_zip_code_prefix` is renamed
    `customer_zip_code` (`contracts/events_v2.json`, `renamed_from`).
  - *Silver:* judges each line by its own version's contract, then copies renamed fields back
    to the earlier name (`to_earliest_names` in `jobs/contract.py`). Nothing downstream changes.
  - *Generator (`schema_changes` in `generator/defects.py`):* decides the format by each
    event's first send. Every copy of an event stays byte-identical, and a late event keeps the
    version it was first sent in. The day's marker names the newest version its lines use.
- **New check:** `schema_contract:new_optional_field` warns when a line carries a field its
  contract doesn't list (`new_fields` in `sentinel/checks/rules.py`). An announced field raises
  nothing.
- **Why these fields:**
  - The new field had to be a real Olist value that the feed didn't carry yet. Review free
    text is excluded (personal text), so it is the product's category.
  - The rename is on a top-level field, so mapping it back stays a one-line rule.
- **Tests:**
  - The generator test: copies stay identical.
  - Two silver tests: a mixed v1/v2 feed lands identically, and unknown fields are named.
  - Both truth tests now run with the change inside their slices, and the fixture's passes.
- **Not in the evaluation:** the evaluation feed keeps it off (`EVAL_DEFECTS` in
  `tests/evaluation/evaluate.py`), because the evaluation started before the change existed.
  The new warning wasn't running for the test period either (below).
- **Spec updated:** Event model ("Schema versions") and the reversible-defects table.

## 2026-11-24 · The test period crashed on 7 Oct, and was resumed

- **Status:** agreed (a record of what happened, not a choice)
- **What happened:** while the test period was running, I added `contracts/events_v2.json`.
  The running process reads the contract files on every run, but its silver code was the old
  one, which used only the newest contract. So on 7 Oct every version 1 line failed to parse,
  and the run crashed before its audit. Nothing was published or recorded for 7 Oct.
- **Fix:** I finished the version-aware silver code and resumed from 7 Oct, with the
  evaluation feed unchanged (schema change off).
- **A second crash, on 20 Dec, was a real generator bug:**
  - 19 Dec had both an anomaly and a late file. The late file was written from the day's
    original events, not the anomaly's, so its checksum didn't match the marker, and
    `land_to_bronze` refused it (`ChecksumMismatch`). The checksum check worked as designed.
  - Fix: `replay` writes an anomaly day's late files from the anomaly's events. Test:
    `test_an_anomaly_days_late_file_matches_its_marker`.
  - I deleted the wrong file and resumed from 20 Dec. Nothing had been written for 20 Dec.
- **What the test period ran with:** the same blocking checks and thresholds throughout. From 20
  Dec, the resumed process also ran the new warn-only `schema_contract:new_optional_field`
  check. It can't block, and the evaluation report counts where it fired.
- **Lesson:** don't change files a long run reads at run time; the contracts are data, not code.

## 2026-11-24 · A publish that read a table's refs without its run branch

- **Status:** agreed (owner, 24 Nov 2026: turn off the catalog cache)
- **What happened:** on the evaluation's 12 Jun 2018, the audit passed, but `publish_release`
  stopped with `KeyError: 'run_2018-06-12'`: one table's refs, as read, had no branch for the
  run. Read again straight afterwards, every table had it.
- **Recovery, the runbook's "A publish died midway":** the release was left `publishing`, so
  no later date could start. I reran `publish_release`, `load_serving` and `clean_up` for 12
  Jun, and it published (20 tables). Then the replay resumed from 13 Jun.
- **Likely cause (the owner's diagnosis, confirmed in a test):** Iceberg's Spark catalog table
  cache.
  - *How it goes wrong:* a session that has loaded a table keeps that copy of its metadata. A
    commit made outside that copy (another session, another process) stays invisible until the
    cache entry expires.
  - *The test:* `tests/unit/test_spark_catalog.py`. One session reads a table's refs, a second
    session creates a branch, and the first session reads again. With the cache on, that read
    missed the branch (the test failed); with it off, it passes.
  - *Not proven:* exactly which outside commit hit the real run is not known. A single replay
    process ran alone, so this is the likely cause, not a proven one.
- **Changes:**
  - `jobs/spark.py` sets `spark.sql.catalog.local.cache-enabled=false`, so every read loads
    the table's current metadata. That costs a metadata read per table access, which is small
    at our sizes.
  - `run_refs` in `sentinel/release.py` stays as a second line of defence. A missing branch is
    read again after `REFRESH TABLE`; if it is still missing, it raises `MissingBranch`. Either
    way the release stays `publishing` until the rerun.
- **Phase 4:** the Glue Data Catalog session gets the same setting, kept in `build_session`.
- **Spec updated:** Publishing, "Failure handling", and the local Spark session in "Getting
  started".

## 2026-11-24 · Evaluation scoring: bad data is the anomaly day's own copy

- **Status:** proposed (a measurement fix in the harness, made after the first test-period
  scoring, and disclosed here because it changed a reported number)
- **What was wrong:** "published with bad data" matched event ids alone. A reversible duplicate
  resends an event with Olist's content in a later day's file, under the same event id, so
  blocked anomalies looked leaked (schema breaks: 30 of 30, all of them blocked).
- **Now:** an anomaly's data counts as published only if the copy in published silver came from
  the anomaly day's own folder (`source_file`). Missing files have no bad rows; they are scored
  as "published, marked incomplete".
- **Detection, blocks and false alarms:** unchanged by this fix.

## 2026-11-24 · Warm-up counts days since the first release

- **Status:** agreed (the spec asks for it: "until a check has 4 weeks of history"). A bug found
  by the held-out test period (`docs/evaluation/report.md`, finding 1)
- **What was wrong:** drift and known-defect checks warmed up while fewer than 28 *published*
  days sat in their trailing window. With many days blocked, that never ended: price shifts
  scored a PSI of 2 to 3.7 and only warned. The spec says "until a check has 4 weeks of
  history".
- **Now:** `history_age` counts days since the first published release, and warm-up ends at
  28.
- **Not re-evaluated:** the held-out numbers in the report were measured before this fix.
  Re-scoring the stored results post-hoc (`evaluate.py report --rescore-warm-up`): all 30 price
  and payment shifts would have blocked, and so would 5 more clean days (false blocks 3 to 8).
  An earlier rough estimate said 7; it also counted drift results that pass with too few rows. Olist's data ends in 2018,
  so there is no second held-out period to measure it cleanly.
- **Test:** `test_warm_up_counts_days_since_the_first_release_not_published_days_in_the_window`.
- **Cost, seen on the tuning period:** 1 Feb 2017 now blocks (`known_defect:no_items`, tail
  0.0005 against 0.001). It is real Olist data, so a false block: 1 of 273 tuning days, up from
  0. Before the fix it fell inside the old warm-up.
  - *Not tuned away:* the test period has been seen, so changing `RATE_TAIL_LIMIT` now would
    be tuning on it.
- **The real-slice truth test** now does what the runbook says a person does with a false
  alarm: it releases the blocked day. The next run takes the batch in, and
  `test_every_held_batch_was_released_and_published` checks that nothing is left held. The
  business tables must still equal the reference.
