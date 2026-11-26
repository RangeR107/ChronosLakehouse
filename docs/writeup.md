# Chronos: a short write-up

## The problem

Real data pipelines don't fail on clean data. They fail on events that arrive a day late or
twice, on a master row that lands after the order that names it, on a value that is wrong and
then corrected, on a file that never comes, and on a job that dies halfway through publishing.
Most portfolio pipelines can't show they survive any of this, because they never see it.

Chronos replays about 100,000 real orders from Olist, a Brazilian marketplace, as a daily event
feed that does all of those things on purpose. It turns the feed into a star schema, and proves
two claims:

1. **The answer is right.** Whatever arrives late, twice or wrong, the business tables end up
   equal to what Olist recorded.
2. **Nobody sees a wrong or half-built day on the way.** A day becomes visible only as a
   complete release that passed its audit.

## How the claims are backed

- **Disturb arrival, never content.** Every business fact comes from Olist. The generator only
  changes *how and when* data arrives, and every disturbance is reversible:
  - a late event still arrives;
  - a duplicate repeats the same `event_id`;
  - a wrong value is followed by a correction with Olist's value.

  So a correct pipeline must end with exactly Olist's tables. The truth test checks that, row
  by row, against a reference built separately in DuckDB SQL, so the pipeline is never graded
  by its own code.
- **Deterministic defects.** Whether an event is delayed is a SHA-256 of a seed and the event's
  identity. The same event gets the same defect in every replay, so every failure reproduces.
- **Write, audit, publish.** Each run writes silver and gold on its own Iceberg branches.
  DataSentinel audits the branch. Only then does main move: every table fast-forwards and is
  tagged, and PostgreSQL loads the release in one transaction. Dashboards see yesterday's
  release or today's, never something in between.
- **Hold, don't drop.** A blocked day's batches are held, so the next day doesn't take the bad
  data in again or block in a cascade. A person releases or rejects them; until then, later
  days publish without them.

## DataSentinel, measured honestly

DataSentinel's thresholds were tuned on January to September 2017, which has no anomalies. They
were then tested once on October 2017 to August 2018, a period never looked at while tuning,
with 180 anomalies of six types placed by a seeded hash.

- **Detected:** 175 of 180 (97%), every one on its own day's run.
- **Falsely blocked:** 3 of 155 clean days (1.9%).
- **Black Friday:** published with a warning instead of a false block, because sale days
  downgrade volume blocks to warnings.

The test period also found a real bug. The checks' warm-up counted *published* days, so with
half the test days blocked it never ended, and 28 price and payment shifts were published with
only a warning. That is fixed, and the report gives both sets of numbers side by side. The fix
was made after seeing the test period, so its re-scored numbers are labelled as not held out.

## What was hard

- **Stale reads, three times:**
  - Great Expectations cached a DataFrame and served it across Iceberg branches, so a build
    saw yesterday's data.
  - Iceberg's Spark catalog cached a table, so a publish missed the branch its own run had
    made.
  - A test's idle PostgreSQL transaction held a lock that hung an `ALTER TABLE` for two hours.

  Each was fixed and has a test, and each is now an interview story about caches and
  isolation.
- **The obvious metric was the wrong one.** On real data, the daily count of event lines swung
  from 0.49x to 2.24x on ordinary days, because most lines are follow-ups to earlier orders.
  Twelve clean days blocked. Counting new orders instead, with a floor on the spread, gave zero
  false blocks on tuning.
- **A real source only knows the past.** Generating 60 days first and then running the
  pipeline let day 1 see day 60's addresses. The producer and the pipeline now take turns, one
  date at a time, exactly as the Airflow DAGs do.
- **Airflow 3 surprises.** A task added to a DAG with `depends_on_past` waited forever for a
  "previous run" that never existed. The fix moved the ordering guarantee into the pipeline
  itself: `open_run` refuses to start while any release is still publishing.

## What's next

- **The move to AWS.** S3, Glue 5.1, the Glue Data Catalog and Athena, as the spec designs, was
  skipped to use no AWS credits. The estimate it was skipped on is about $40 to $65 of the
  free plan's $100: one Glue job per business date over 785 dates. A light version would cost
  about $2 to $5: local Spark on S3 and the Glue catalog, plus one Glue run.
- **Three evaluation findings, not acted on.** Volume during heavy blocking, the days after
  Black Friday, and null delivery dates that the known-defect check counts on the wrong orders.
  Each needs a new held-out period to justify, and Olist has no later data.
- **A labelled load test,** copying the replay K times with new ids, to report runtime at
  K times the real volume without claiming K times the business.

## Numbers

| What | Value |
| --- | --- |
| Orders in Olist | 99,441 |
| Orders the evaluation replayed (Jan 2017 to Aug 2018) | 99,092, through the full pipeline, with anomalies |
| Truth test, real slice | 60 business dates plus an 11-day drain, 2,679 orders; every business table equal to the reference. The full history has not been run as a truth replay |
| Tests | 160 locally (14 need the Olist files, so CI runs 146) |
| Daily run, on a laptop | 17 to 30 seconds |
| DataSentinel, held out | 97% of 180 anomalies detected; 1.9% of clean days falsely blocked |
| Cost | $0 |
