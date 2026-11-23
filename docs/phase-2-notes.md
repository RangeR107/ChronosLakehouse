# Phase 2 notes: late data

Phase 1 proved the pipeline gets the right answer when data arrives on time. Phase 2 makes the
feed realistic: events arrive late, twice, out of order, first wrong and then corrected; master
rows arrive after the orders that use them; whole files arrive a day late. The claim to prove is
unchanged: whatever happens on the way, the business tables end up exactly equal to Olist.

**Done when** (spec, Phased build plan): "All nine scenario tests and the truth test pass in CI
with defects on."

- The nine scenario tests (`tests/scenarios/test_scenarios.py`) run on the fixture, in CI.
- The truth test runs with defects on in two places: on the fixture with heavy defect rates, in
  CI; and on the real Jan–Feb 2017 slice at the spec's rates, locally (CI can't see the data).
- Two scenario tests check slightly less than the spec says, because the spec assumes Phase 3's
  branches (see "What isn't done yet").

## Results

Local runs, 23 Nov 2026, uncommitted working tree on top of `163f1f9`. 117 tests in all:

- Full suite (`CHRONOS_REQUIRE_POSTGRES=1 CHRONOS_REQUIRE_OLIST=1 pytest tests`): 114 of 116
  passed in 25 minutes. The 2 failures were a generator bug: held-back sellers that no order in
  the slice names were never loaded.
- After the fix, plus a regression test for it, the real-slice truth test passed
  (13 of 13, 12 minutes).
- The scenario and fixture tests, which that fix doesn't touch, had passed in the full run.

**Real slice with the spec's defect rates.** 60 business dates plus an 11-day drain,
2,679 orders:

| Defect | Injected |
| --- | --- |
| Late carrier and delivery events | 201 |
| Duplicates | 225 |
| Corrections | 22 |
| Delivered before the handover | 18 |
| Early-arriving customers | 1 |
| Held-back sellers | 1 |

Every business table equals the DuckDB reference row by row. The event ledger holds exactly the
versions and arrival times the generator planned, and rerunning the first, middle and last days
changes nothing.

**Fixture with heavy defect rates (runs in CI).** 13 duplicates, 9 late files, 2 late events,
2 early-arriving customers, 2 held-back sellers, a correction, an out-of-order delivery and a
late address change, all on 7 orders. It still ends equal to the reference.

**Airflow.** A 7-date backfill of `chronos_replay` triggered `chronos_daily` for each date
strictly in order, about a minute per date. The source database held exactly Olist's 26 people
for those dates' 40 orders.

What the tests caught on the way, each fixed and recorded in `docs/decisions.md`:

- A placeholder seller's unknown location was getting a geography key, because `concat_ws`
  skips nulls.
- Held-back sellers that no order named were never loaded.
- Airflow ran three dates at once (backfill ignores the DAG's `max_active_runs`), and an
  unbounded `@daily` schedule created a run for today's date.

## What was built

| Piece | Where | In one sentence |
| --- | --- | --- |
| Airflow image | `airflow_local/Dockerfile` | Airflow 3.4.0 with Java 17, PySpark 3.5.6 and the Iceberg jar, so tasks can run Spark. |
| `chronos_daily` | `dags/chronos_daily.py` | One task per step function, strictly one date at a time. |
| `chronos_replay` | `dags/chronos_replay.py` | Plays the source for a date, then runs `chronos_daily` for it and waits. |
| Defect injector | `generator/defects.py` | Late, duplicate, out-of-order and corrected events, late master rows, late files; all deterministic and recorded. |
| Replay plan | `generator/replay.py` (`plan`) | Works out the whole feed once (events by day, late files, master writes), then writes one date at a time. |
| Late files | `jobs/land_to_bronze.py` | `register_files` looks back 10 days for files a marker listed that arrived later. |
| Inferred members | `jobs/build_dimensions.py` | Placeholder customers and sellers for facts that arrive before their master rows. |
| Re-keying | `jobs/build_facts.py` (`stale_orders`) | Rebuilds orders whose keys no longer match the dimensions. |
| Dirty periods | `jobs/build_facts.py` (`dirty_periods`) | Records which delivery months each change touches, before and after. |
| Recompute only what's dirty | `jobs/recompute_aggregates.py` | Recomputes those months whole; nothing else. |
| Published metrics, restatements | `jobs/recompute_aggregates.py` | Every recomputed value is appended per run; question 8 compares first with latest. |
| Dashboard | `dashboard/app.py` | Adds the restatement report. |

## How to run it

With Airflow (Docker Desktop running):

```bash
docker compose up -d --build
```

Then open http://localhost:8080 (user `airflow`, password `airflow`), unpause `chronos_daily`
and `chronos_replay`, and backfill the replay:

```bash
docker compose exec airflow-scheduler airflow backfill create --dag-id chronos_replay --from-date 2017-01-01 --to-date 2017-03-12 --max-active-runs 1 --reprocess-behavior failed
```

Dates run strictly in order, about a minute each. Turn on the spec's defects for a replay from
the command line with `--defects --feed-end <last date> --manifest <path>`
(`python -m generator.replay --help`).

## How to test it

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/pytest tests/unit tests/scenarios
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) CHRONOS_REQUIRE_OLIST=1 CHRONOS_REQUIRE_POSTGRES=1 .venv/bin/pytest tests
```

The scenario tests take about 8 minutes; the whole suite with the real slice about 25.

## The ideas, in plain words

**Disturb arrival, never content.** Every defect is reversible: a late event still arrives, a
duplicate repeats the same event_id, a wrong value is followed by a correction carrying Olist's
value. So a correct pipeline must end with exactly Olist's tables, and the truth test can demand
that.

**Deterministic defects.** Whether an event is delayed is decided by a SHA-256 of a seed and the
event's identity, not by a random number generator's sequence. The same event gets the same
defect in every replay and every slice, so failures reproduce.

**A correction is a new version.** Version 2 of an event names version 1 in
`supersedes_event_id`. Silver keeps both; the facts use the highest version; the event ledger
shows the history; the restatement report shows what the dashboards first said.

**Inferred members.** When an order arrives before its customer's master row, the dimension gets
a placeholder with the key the real row will have. When the real row arrives, it overwrites the
placeholder in place, and the fact never moves.

**Re-keying.** When a customer's address change arrives late, orders placed after the move were
pointing at the old version. Each run finds facts whose keys no longer match the dimensions and
rebuilds just those orders.

**Dirty periods.** A late delivery changes one month's late rate. Rather than recompute every
month every day, `build_facts` writes down which months its changes touched, and only those are
recomputed, each one whole, because a median can't be patched one row at a time.

## Questions an interviewer could ask

**How do you know the defects didn't change the answer?** The truth test runs the replay with
defects on, lets every delay land (an 11-day drain), and compares every business table row by
row with a DuckDB build straight from the Olist files (`tests/truth/`). The ledger part checks
that every event version arrived exactly once, when the generator says it did.

**What happens when the same event arrives twice?** Silver drops it, both within the batch and
against what it already holds, before any MERGE (`jobs/bronze_to_silver.py`, `merge_new`).
Scenario 1.

**What if the delivery arrives before the carrier handover?** Each milestone comes from its own
event type, so arrival order doesn't matter (`milestones` in `jobs/build_facts.py`). Scenario 3
checks the row on the day only the delivery was known, and at the end.

**How do corrections show up for users?** The dashboard shows the corrected value; the
restatement report shows the first published value next to the latest, with numerators and
denominators, so you can see why a rate moved (`restatements` in
`jobs/recompute_aggregates.py`).

**An order arrives for a customer the warehouse has never seen. What happens?** Inferred member
(`customer_versions` in `jobs/build_dimensions.py`). Scenario 5.

**A customer's address change reaches the database three days late. What happens?** The new
version's effective date is in the past, so orders since then point at the wrong version. The
next run notices (`stale_orders`) and re-keys only that person's orders. Scenario 6 checks that
exactly one order changed.

**Why do recomputed months get recomputed whole?** Shares, medians and ranks depend on every row
in the period; patching one row would give a wrong median.

**What if the job dies halfway?** Each layer writes its progress mark last, so the rerun redoes
the unfinished work, and serving only changes in `load_serving`'s one transaction. Scenario 8
kills `build_facts` midway and checks both.

**Why does Airflow need `depends_on_past` if `max_active_runs` is 1?** Because an Airflow 3
backfill applies its own concurrency limit; in testing, three dates ran at once until
`depends_on_past` enforced the order (`docs/decisions.md`).

## What isn't done yet

- **Scenario 8, "main unchanged":** without Phase 3's write-audit-publish branches, a crashed run
  leaves gold partly written. The test checks that serving is unchanged and that the rerun
  finishes identical to an uninterrupted run.
- **Scenario 9:** tested as a catch-up after a 4-day gap. The serial Airflow backfill was
  checked by hand on 7 dates, not in pytest.
- **The announced schema change** moves to Phase 3, with the schema contract checks.
- **Rerunning an old date** after later ones (an Airflow "clear") is not yet safe for dimensions;
  to be decided with releases.
- **Full replay:** not yet run with defects; it is the evidence for "99,441 orders".
