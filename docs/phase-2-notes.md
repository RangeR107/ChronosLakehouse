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

**Airflow, checked by hand (scenario 9's serial backfill).** Two backfills of `chronos_replay`
(1 to 3 Jan, then 4 to 7 Jan, `--max-active-runs 1 --reprocess-behavior failed`). Each replay run
triggered `chronos_daily` for the same date and waited for it. The run list below comes from
Airflow's metadata database (`dag_run` and `task_instance`, 23 Nov 2026, times UTC):

| DAG | Logical date | State | Started | Ended | Tasks succeeded |
| --- | --- | --- | --- | --- | --- |
| chronos_replay | 2017-01-01 | success | 06:44:43 | 06:56:35 | 2/2 |
| chronos_daily | 2017-01-01 | success | 06:50:34 | 06:56:27 | 9/9 (wait_for_landing on its 2nd try) |
| chronos_replay | 2017-01-02 | success | 06:56:36 | 06:57:41 | 2/2 |
| chronos_daily | 2017-01-02 | success | 06:56:41 | 06:57:34 | 9/9 |
| chronos_replay | 2017-01-03 | success | 06:57:42 | 06:58:48 | 2/2 |
| chronos_daily | 2017-01-03 | success | 06:57:47 | 06:58:43 | 9/9 |
| chronos_replay | 2017-01-04 | success | 07:15:05 | 07:16:11 | 2/2 |
| chronos_daily | 2017-01-04 | success | 07:15:10 | 07:16:06 | 9/9 |
| chronos_replay | 2017-01-05 | success | 07:16:11 | 07:17:33 | 2/2 |
| chronos_daily | 2017-01-05 | success | 07:16:18 | 07:17:22 | 9/9 |
| chronos_replay | 2017-01-06 | success | 07:17:34 | 07:18:54 | 2/2 |
| chronos_daily | 2017-01-06 | success | 07:17:38 | 07:18:50 | 9/9 |
| chronos_replay | 2017-01-07 | success | 07:18:54 | 07:20:15 | 2/2 |
| chronos_daily | 2017-01-07 | success | 07:19:00 | 07:20:13 | 9/9 |

- **Order:** each date started only after the previous one ended; no two dates overlap.
- **Retries:** the only retry is 1 Jan's `wait_for_landing`. Its first attempt failed and the
  retry succeeded, which is that task working as a sensor: the day's marker wasn't there yet on
  the first try. 1 Jan took longer because its run was queued behind a stray run (below).
- **Data:** afterwards, the source database held exactly Olist's 26 people for the 40 orders of
  1 to 7 Jan 2017, and `serving.load_state` showed `run_2017-01-07`.

To reproduce the list, after a backfill:

```bash
docker exec chronoslakehouse-airflow-db-1 psql -U airflow -d airflow -c "SELECT dag_id, logical_date, state, start_date, end_date FROM dag_run ORDER BY logical_date, dag_id DESC"
```

What the tests caught on the way, each fixed and recorded in `docs/decisions.md`:

- A placeholder seller's unknown location was getting a geography key, because `concat_ws`
  skips nulls.
- Held-back sellers that no order named were never loaded.
- Airflow ran three dates at once (backfill ignores the DAG's `max_active_runs`), and an
  unbounded `@daily` schedule created a run for today's date.

## Truth test, last passed

| Run | Date | Code | Result |
| --- | --- | --- | --- |
| Full suite including the real slice with the spec's defect rates (`CHRONOS_REQUIRE_OLIST=1 CHRONOS_REQUIRE_POSTGRES=1 pytest tests`, which includes what `make truth` runs) | 23 Nov 2026 | working tree on top of `163f1f9`, before the Phase 2 commits | 119 passed, 1 expected failure (scenario 8's main-unchanged), 29 minutes |

Update this row each time `make truth` passes, with the commit it ran on.

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
`depends_on_past` enforced the order (`docs/decisions.md`). *Update, Phase 3:* `depends_on_past` was
removed. With releases, a blocked date must not stop later dates, and the backfill's
`--max-active-runs 1` keeps dates serial (`docs/decisions.md`, 23 Nov 2026).

## What isn't done yet

- **Scenario 8, "main unchanged":** without Phase 3's write-audit-publish branches, a crashed run
  leaves gold partly written. `test_8_a_crash_halfway_leaves_main_unchanged` is a strict expected
  failure saying so; removing its marker is part of Phase 3's "Done when". The rest of scenario 8
  (serving unchanged, rerun identical) passes now.
- **Scenario 9:** tested as a catch-up after a 4-day gap. The serial Airflow backfill was
  checked by hand on 7 dates (run list above), not in pytest.
- **Rerunning a published date** should republish nothing and exit successfully. It needs the
  release manifest, so it is built in Phase 3; until then, don't clear an old date after later
  ones have run. Logic bugs are fixed by a full rebuild from bronze (`docs/runbook.md`), which
  is tested.
- **Question 3's aggregate** isn't built yet, but its dirty months (item shipping-limit months,
  including seller re-keying) are recorded from now on.
- **The announced schema change** moves to Phase 3, with the schema contract checks.
- **Full replay:** not yet run with defects; it is the evidence for "99,441 orders".
