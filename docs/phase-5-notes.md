# Phase 5 notes: finishing for a stranger

Phases 0 to 3 built the pipeline and proved it. Phase 4, the move to AWS, was skipped on
25 Nov 2026 so no AWS credits are used (`docs/decisions.md`). Phase 5 makes the project easy
to pick up: a stranger should be able to run it from the README, and understand what it shows,
in five minutes.

**Done when** (spec, Phased build plan): "A stranger can run it from the README and understand
the results in five minutes."

| Deliverable | Where | Status |
| --- | --- | --- |
| README | `README.md` | rewritten: the results first, then how it works, how to run it, the limits |
| Architecture diagram | `README.md`, "How it works" | Mermaid flowchart (GitHub draws it) |
| Data-model diagram | `README.md`, "The star schema" | Mermaid entity-relationship diagram |
| Decision log | `docs/decisions.md` | every decision agreed; the AWS skip and the review facts recorded |
| Short write-up | `docs/writeup.md` | two pages: the problem, the evidence, what was hard, what's next |
| Demo video | `docs/demo-script.md` | a four-minute script; **the owner records the video** |
| One-command demo | `jobs/demo.py`, `make demo` | new: 30 dates through the whole pipeline, with one block |
| Optional load test | – | not done (see "What isn't done") |

## Results

**The stranger's walk-through** (25 Nov 2026, on the owner's laptop). The 114 files that would
be committed, with no data, no lake, no `.venv` and no local notes, were copied into an empty
folder. Only the Olist CSVs were added, where the README's step 1 puts them. Then the README
was followed from step 2:

| Step | Result |
| --- | --- |
| `make setup` | exit 0 in 23 seconds (pip's download cache was warm; a first install also downloads PySpark, about 300 MB) |
| `make db` | the container was already running; PostgreSQL 16.16 answered on port 5433 |
| `make demo` | exit 0 in 574 seconds (9.6 minutes): 29 dates published, 10 January blocked, held, released, and taken in by 11 January |
| Dashboard and console | opened with the same commands after the project's own run, whose lines were identical: the dashboard showed 238 delivered orders, none late, as of `release_2017-01-30`; the console showed 29 published, 1 blocked, 0 waiting, the 10 January block under Alerts and blocks, and the incomplete releases under Open problems |

- **The output is the same on every run.** The demo ran twice, once in the project and once in
  the copy, and printed identical lines for all 30 dates. Defects are SHA-256 draws, not random.
- **The dashboard's numbers were checked against the raw CSV,** with DuckDB: 243 orders were
  delivered by 30 January 2017, none after the estimate. The 5 the dashboard lacks are
  deliveries the feed was still delaying when the demo stopped.
- **Disk:** about 2 GB, as the README says: the virtual environment 1.0 GB, the PostgreSQL image
  0.7 GB, the Olist files 120 MB, the demo lake 99 MB, the Iceberg jar 45 MB.
- **Both diagrams render** with Mermaid 11 (checked in a browser). The flowchart was switched
  to top-to-bottom: left-to-right it was 1,008 pixels wide and 128 high, too small to read
  once GitHub shrinks it.

**What the walk-through changed:**

- **Spark's and Great Expectations' progress bars are off** (`jobs/spark.py`,
  `sentinel/checks/rules.py`). They were drawn into the demo's output and split its date lines.
- **The demo records each date's notice,** as `chronos_daily` does, so the console's "Notices
  sent" is no longer empty.
- **The README's example output** is now copied from a real run. The first draft had
  placeholder lines written before the demo had run, and they were far off (612 events on
  9 January, where the real run has 16).
- **`make test`'s time** was wrong in the README. With the Olist files present it also runs the
  real-slice truth test: 160 tests, about 55 minutes. CI runs the other 146 in about 40.
- **The demo's message for an unchecked block.** It said "a stray resend, checked" for any
  block, which is only true of the scripted one.
- **`--keep-held` became `--ask`.** The old flag left a block held to the end of the demo, so
  releasing it afterwards changed nothing: no later run was left to take the lines in. `--ask`
  pauses on the block instead. Tested on 12 dates in a separate database, with an invalid
  answer (asked again) and then a reject: 11 January published without 10 January's lines,
  and the hold ended `rejected`.

**The longer demo, `make demo DAYS=90`** (25 Nov 2026, in the project, after the changes
above): exit 0 in 2,004 seconds (33 minutes), about 22 seconds a date.

- **88 dates published, 2 blocked.** 10 January is the scripted stray file. 1 February blocked
  on `known_defect:no_items` once the 28-day warm-up had ended: the same false alarm on real
  data that the truth test meets (`docs/decisions.md`, the warm-up fix). The demo released it
  and printed "unchecked: the demo releases every block".
- **The dashboard** served `release_2017-03-31`: 3,996 delivered orders, 52 late (1.3%); in
  March, 2.9% late in the South and in the Northeast. Olist's file has 4,016 delivered by
  31 March with the same 52 late; 20 deliveries were still delayed by the feed.
- **The restatement report** had 90 values published more than once, 71 of which changed.
- **The console's notices:** 88 run summaries and 2 "blocked" notices, none sent to Slack
  (`delivered = not_configured`, no webhook set).
- **The output is clean:** 131 lines in all, with no progress bars.
- **Two dates have one event fewer** than in the 30-day run (24 and 30 January). Each is a
  carrier hand-off that the out-of-order defect sends 1 to 48 hours after its order's delivery
  (`inject_events` in `generator/defects.py`). Those two deliveries happen after 30 January,
  so the 30-day feed doesn't contain them, and the hand-offs stay on their own dates. Runs of
  the same length match exactly.

## What was built

| Piece | Where | In one sentence |
| --- | --- | --- |
| The demo | `jobs/demo.py` | Plays producer and pipeline in turns, one date at a time, into its own database and lake, and prints one line per date. |
| Make targets | `Makefile` | `setup`, `db`, `demo` (`DAYS=90` for a longer run), `dashboard`, `console`, `test`, `truth`, `airflow`: the README's commands, with Java 17 found for you. |
| Console default | `sentinel/console/app.py` | The console opens on the database `CHRONOS_DB_NAME` names, so `make console` shows the demo. |
| Quieter output | `jobs/spark.py`, `sentinel/checks/rules.py` | No Spark or Great Expectations progress bars: in Airflow logs and the demo's output they buried the useful lines. |
| README | `README.md` | Results in one minute, two diagrams, setup from the Kaggle download to the dashboards, a repository map, the limits. |
| Write-up | `docs/writeup.md` | The project in two pages, with a table of the numbers. |
| Video script | `docs/demo-script.md` | What to show and say, section by section. |
| Spec and decisions | `docs/spec.md`, `docs/decisions.md` | Updated for skipping AWS: nothing claims S3, Glue or Athena ran. |

### How the demo works

- **It plays both sides, in turns,** the way the `chronos_replay` DAG does. The producer writes
  one business date's files and source rows (`generator/replay.py`), then `run_day` runs the
  pipeline for that date, then the next date is written. The source database only ever knows
  the past.
- **The feed carries the spec's reversible defects** (`generator/defects.py`): late,
  duplicate, out-of-order and corrected events, and late files.
- **One date is made to block.** On 10 January the producer resends a file under a name its
  completion marker doesn't list (`Anomalies(..., scripted={"2017-01-10": "duplicate_file"})`).
  That changes arrival only, never business data. DataSentinel's completeness check blocks the
  date, and its lines are held.
- **The demo then does what the runbook says a person does:** it releases the held lines
  (`decide(ctx, d, RELEASED)` in `jobs/batches.py`), and the next date's release takes them in.
  Any other block, which a longer run can hit after the warm-up, is released too, and the demo
  prints that nobody checked it. With `--ask` it pauses on each block instead, and you
  answer release, reject or keep held, after looking at it in the console.
- **Each date ends as `chronos_daily` ends.** A published date records its run summary
  (`notify` in `sentinel/alerts.py`); a blocked one records a "blocked" notice, as Airflow's
  failure callback would. Both show in the console, and go to Slack only if
  `CHRONOS_SLACK_WEBHOOK` is set.
- **Everything it writes is its own:** the `chronos_demo` database and `lake/demo/`.
  `--fresh` drops both first. It never touches the Airflow lake (`chronos`) or the evaluation
  (`chronos_eval`).

## How to run it

From a fresh clone, with the Olist CSVs in `data/raw/olist/` and Docker running:

```bash
make setup
```

```bash
make db
```

```bash
make demo
```

```bash
make dashboard
```

```bash
make console
```

`make demo DAYS=90` replays to the end of March 2017 instead. To decide each block yourself,
with the console open:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/python -m jobs.demo --fresh --ask
```

Outside the demo, the same decision is the runbook's command (`jobs/batches.py`), for example
against the Airflow lake's database:

```bash
.venv/bin/python -m jobs.batches release 2017-01-10
```

## How to test it

The walk-through above is the test: an empty folder, the committed files, and the README's
commands in order. To repeat it:

```bash
git clone https://github.com/RangeR107/ChronosLakehouse.git /tmp/chronos-check
```

Then put the Olist CSVs in `/tmp/chronos-check/data/raw/olist/` and run `make setup`,
`make db`, `make demo`, `make dashboard` and `make console` from that folder.

Phase 5 changed two lines of shared code (the progress bars), so the whole suite still applies:

```bash
make test
```

```bash
make truth
```

Results on 25 Nov 2026, on the working tree on top of `5078b54`, with every Phase 5 change in
place:

- **`make truth`:** 14 passed in 27.5 minutes. Every business table still equals the DuckDB
  reference, and no held batch was left unreleased.
- **Unit tests** (`pytest tests/unit`): 114 passed in under 4 minutes.
- **Lint** (`ruff check .`): clean.
- **The scenario tests and the fixture truth test** weren't rerun locally; CI runs them on the
  next push.

## Truth test, last passed

| Run | Date | Code | Result |
| --- | --- | --- | --- |
| `make truth` (real Jan to Feb 2017 slice, the spec's defects on) | 25 Nov 2026 | working tree on top of `5078b54`, with the Phase 5 changes | 14 passed, 27.5 minutes |

Update this row each time `make truth` passes, with the commit it ran on.

## The ideas, in plain words

- **A README is a product.** A stranger reads the top and decides in a minute whether to
  continue. So the results come first, each claim points to the test that backs it, and the
  limits are stated as plainly as the results.
- **Claim only what ran.** AWS was designed but never deployed, so the README, spec and
  résumé lines say "designed for AWS", never "built on". The truth test's full-history run
  hasn't happened, so the README says the slice is 60 days.
- **A demo should show a failure, not hide one.** A demo where everything passes shows
  nothing about the quality gate. The scripted stray file makes one date block, so the
  stranger sees the hold, the release and the next day picking the data up.
- **Real data explains odd numbers.** The demo's dashboard shows no late deliveries. That
  isn't a bug: in January 2017 Olist delivered every order before its estimate. Checking it
  against the raw CSV, rather than assuming, is the habit the truth test is built on.

## Questions an interviewer could ask

**Walk me through the architecture in two minutes.** Use the README's flowchart. A producer
replays Olist as hourly files plus a completion marker, and master rows into a PostgreSQL
source. Each business date, bronze keeps raw lines, silver types and deduplicates them, gold
builds the star schema, all on the run's own Iceberg branch. DataSentinel audits the branch;
only a passing run is published (main fast-forwarded and tagged) and loaded into PostgreSQL in
one transaction for the dashboards.

**Why didn't you deploy to AWS?** Cost: the estimate was $40 to $65 of the free plan's
credits, almost all Glue (`docs/decisions.md`, 2026-11-25). Everything the claims rest on (the
truth test, the release guarantees, the evaluation) runs locally. The code was written for
Glue 5.1's versions (PySpark 3.5.6, Iceberg 1.10.0, `jobs/spark.py`), so the move is mostly
configuration: the catalog becomes the Glue Data Catalog and the warehouse an S3 path.

**What would change at 100 times the volume?** Today a date takes 17 to 30 seconds on a
laptop. At 100 times, the first places to measure are the MERGEs into silver and gold and the
dirty-period recompute (`jobs/recompute_aggregates.py`), and the laptop would give way to Glue
or another cluster. The spec's optional load test (copying the replay with new ids)
would give a real number; it wasn't run, so no scale is claimed.

**Your demo releases a blocked day automatically. Isn't that cheating?** It releases only
after printing why. For the scripted stray file the reason is known: the file is a resend,
and the event ids deduplicate. Any other block is released with the word "unchecked", and
`--ask` hands every decision to you. The point of the demo is to show the hold and the
release, which the runbook (`docs/runbook.md`) says a person does.

**Why does the dashboard say nothing was late?** Because nothing was. Olist delivered 243
orders by 30 January 2017, none of them after its estimate. The dashboard shows 238: the other
5 deliveries are events the feed is still delaying. In a longer run they land on a later date,
and the restatement report shows the change.

**How would a stranger know your numbers are right?** The truth test compares every business
table with a reference built separately in DuckDB SQL from the raw CSVs
(`tests/truth/reference.py`), never with the pipeline's own code. The README says which runs
back which claim: the 60-day real slice and the CI fixture.

**What are the project's weaknesses?** The README's "Limits": never deployed to AWS; the
truth test covers 60 days, not the full history; about 100,000 orders is small; the
evaluation covers six anomaly types, and three fixes it suggested weren't made because no
second held-out period exists.

## What isn't done yet

- **The demo video.** The script is ready (`docs/demo-script.md`); the owner records it.
- **The optional load test.** Not run, so no claim beyond about 100,000 orders.
- **A truth replay of the full history.** The evaluation replayed it with anomalies, which
  isn't a truth test.
