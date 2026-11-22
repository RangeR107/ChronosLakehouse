# Chronos

A batch lakehouse that replays real Olist marketplace orders as a late, messy event feed, builds a
star schema from it, and publishes only audited releases. This is a portfolio project for Amazon
Data Engineer intern interviews: the owner must be able to explain and defend every line.

## Sources of truth

- `docs/spec.md` is the specification (Revision 5). Read the relevant section before building
  anything. Its Decision log and Known limitations explain why things are the way they are.
- `tools/measure_spec.py` is the reference implementation of metrics 1 to 7. Its CSV output is
  what the metric regression test compares gold with. `tools/profile_olist.py` profiles the raw files.
- The Olist CSVs live in `data/raw/olist/` (downloaded by the owner from Kaggle; never committed).

If the spec is ambiguous, contradicts itself, or turns out to be wrong once code runs, stop and
say so. Don't silently deviate. Record every agreed change in `docs/decisions.md` (date, what,
why, alternatives) and point out which spec section needs updating.

## How to work

- Build one phase at a time, in order (spec: "Phased build plan"). Finish a phase's "Done when"
  criteria, with tests passing, before starting the next. Stop at the end of each phase and wait.
- The project must cost nothing. Everything runs locally until Phase 4, which uses an AWS
  free-plan account only (S3, Glue, Glue Data Catalog, and Athena if the free plan allows it).
  Never propose a paid service, a paid plan upgrade, or anything that could bill the owner.
  Don't create AWS resources, apply Terraform, or ask for AWS credentials before Phase 4.
- Dashboards are served from PostgreSQL, not Redshift (see "Zero cost" in the spec).
- Work in small steps with one commit each. Commit messages say what changed and why.
- Ask before adding any dependency, service or tool that the spec doesn't name. In particular:
  no EMR, Kinesis, OpenSearch, dbt, MWAA or Redshift.
- At the end of each phase, write `docs/phase-N-notes.md`: what was built, how to run it, how to
  test it, and the questions an interviewer could ask about it, with short answers that point
  to the code. Write for someone learning data engineering, in plain language.
- Comments explain why, not what. Prefer small, readable functions over clever ones.

## Environment (macOS, Apple Silicon)

- Use the project's virtual environment for every Python command: `.venv/bin/python`,
  `.venv/bin/pip`, `.venv/bin/pytest`. Create it with `python3.11 -m venv .venv` if it's missing.
- Spark needs Java 17; Java 23 is also installed and must not be used. Before running anything
  that starts Spark, check `java -version`. If it isn't 17, prefix the command with
  `JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
- Docker Desktop runs PostgreSQL and Airflow. If a docker command fails, ask the owner to
  start Docker Desktop rather than working around it.

## Pinned versions

Python 3.11 · PySpark 3.5.6 · iceberg-spark-runtime-3.5_2.12:1.10.0 · Java 17 · Airflow 3.4.0
installed with its constraints file (and the Amazon provider pinned with it) · PostgreSQL 16 in
Docker. Every Iceberg table is created with format-version 2. Glue 5.1 later, pinned explicitly.

## Rules from the spec that are easy to break

- Never invent business data. The only simulated things are arrival behaviour (timing,
  duplicates, out-of-order, temporary wrong values that get corrected, late files, late
  dimension rows) and the evaluation-only anomalies. See "What's real and what's simulated".
- Pipeline code (`jobs/`, `sentinel/`, `dags/`) must never read the generator's ground-truth
  manifest. Only `tests/` may. The completion marker is the producer's delivery contract and
  is allowed; the ground-truth manifest is not.
- Stable hashing only: SHA-256 for event_id and surrogate keys. Never Python's `hash()`.
- Deduplicate on event_id before every MERGE.
- `updated_at` in PostgreSQL is set by the replay-clock trigger; `address_effective_from` carries
  business time. Never use one for the other.
- Payment rows travel with `order_placed`, not `payment_approved`.
- Runs are serial (`max_active_runs = 1`) and read bronze from the last published watermark.
- The truth test compares business tables with a reference build written separately, in
  DuckDB SQL. Never build the expected result with the pipeline's own code.
- Use the reliable window (purchases 2017-01-01 to 2018-08-31) and the shared definitions in
  "Business questions and metric definitions" for every metric.

## First thing in Phase 0

Run the Iceberg smoke test from the spec ("Getting started: week 1") as a pytest. If the first
count shows reads do not follow `spark.wap.branch`, stop and report it: the jobs must then read
their branch explicitly, and that changes the design.
