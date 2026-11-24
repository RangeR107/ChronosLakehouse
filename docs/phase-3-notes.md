# Phase 3 notes: DataSentinel and releases

Phase 2 made the feed late and messy and proved the business tables still end up exactly right.
Phase 3 makes sure nobody ever *sees* a wrong or half-built day on the way. Each run writes on
its own Iceberg branches. DataSentinel audits the run there, and only a run that passes the
audit becomes a release that dashboards can read. A bad day is held back, recorded and shown to
a person, and the days after it carry on.

**Done when** (spec, Phased build plan): "The evaluation report on the held-out period is
written, the release consistency test passes, scenario 8's main-unchanged check passes (its
expected-failure marker removed), and rerunning a published date republishes nothing."

| Criterion | Where | Status |
| --- | --- | --- |
| Evaluation report on the held-out period | `docs/evaluation/report.md` | written |
| Release consistency test | `tests/scenarios/test_releases.py` | passes |
| Scenario 8, main unchanged | `tests/scenarios/test_scenarios.py` | passes, marker removed |
| Rerunning a published date republishes nothing | `open_run` in `sentinel/release.py`; Airflow 5 Jan below | passes |

## Results

**The evaluation** (`docs/evaluation/report.md`; local run, 24 Nov 2026):

| Period | Days | What happened |
| --- | --- | --- |
| Tuning, 2017-01 to 2017-09, clean | 273 | 0 blocked, 32 warned (before tuning: 12 blocked, 250 warned) |
| Test, 2017-10 to 2018-08, held out, as measured (before the warm-up fix) | 155 clean, 180 anomalies | 175 of 180 anomalies detected (97.2%), all on their own day's run; 109 of 150 blocked; 3 clean days blocked (1.9%), 22 warned |
| Same test period, current code, re-scored post-hoc (not held out) | 155 clean, 180 anomalies | 175 of 180 detected; 139 of 150 blocked; 8 clean days blocked (5.2%), 17 warned |

- **Black Friday 2017** published, with a volume warning instead of a block (z = 52).
- **Four findings** from the test period are in the report:
  - a warm-up bug, now fixed, which let 28 price and payment shifts publish;
  - volume needing 8 published points while half the days were blocked;
  - the days after Black Friday;
  - null delivery dates slipping past the known-defect check.

**Tests** (local, 24 Nov 2026):

- **Unit tests:** all passed in the full-suite run below.
- **Scenario and release tests:** 18 of 18 passed.
- **Fixture truth test,** with the schema change on: 13 of 13 passed.
- **Full suite with the real slice** (`CHRONOS_REQUIRE_OLIST=1 CHRONOS_REQUIRE_POSTGRES=1
  pytest tests`, 36 minutes): 144 passed. The real-slice truth test errored, because the
  warm-up fix made 1 Feb 2017 block (a false alarm on real data, `docs/decisions.md`).
- **After the truth test was taught to release a blocked day,** as the runbook says a person
  does, both truth tests passed: 28 of 28 in 31 minutes. That includes
  `test_every_held_batch_was_released_and_published`, and every business table still equals the
  DuckDB reference.
- **Final full suite, after the catalog-cache change** (24 Nov 2026): 160 of 160 passed in 53
  minutes, including both truth tests and `tests/unit/test_spark_catalog.py`. An Airflow run
  overlapped with it, so the cache change's cost on run time isn't isolated.

**Airflow** (from its metadata database; dates replayed on 23 Nov 2026):

- `chronos_replay` and `chronos_daily` ran 1 to 15 Jan 2017 in order, every run `success`, each
  about two minutes. `maintenance_weekly` ran after Sunday 15 Jan, also `success`.
- **5 Jan was cleared and rerun after later dates.** Its `chronos_daily` run took 19 seconds:
  `open_run` found the release and skipped the rest. A rerun of a published date republishes
  nothing.
- **Two failed runs dated 23 Nov 2026** are the accidental wall-clock run (`docs/decisions.md`).
  Their writes were reset, and `chronos_replay` now refuses dates outside Olist's range.
- **`notify` in a live run, 24 Nov 2026 (times UTC):**
  - After the containers were recreated with the alert settings, `chronos_replay` was
    backfilled for 16 Jan 2017.
  - Every `chronos_daily` task succeeded on its first try, 17:26:17 to 17:28:30. `notify` ran
    last, 17:28:25 to 17:28:29.
  - It wrote one row to `metrics.notifications`: kind `published`, summary "2017-01-16:
    published release_2017-01-16 (incomplete), 1 warnings", delivered `not_configured` (no
    Slack webhook is set).
  - The release is marked incomplete because of a freshness warning, which only ever warns.
  - `is_sunday` skipped maintenance (16 Jan is a Monday).
- **The failure callback** has not fired in a live run (nothing failed). It is tested in
  `tests/unit/test_alerts.py`.

## What was built

| Piece | Where | In one sentence |
| --- | --- | --- |
| Run branches | `jobs/context.py` (`on_run_branch`), `sentinel/release.py` (`open_run`) | Every silver and gold write of a run goes to branch `run_<date>`; main doesn't move. |
| The audit | `sentinel/gate.py` | Runs every check on the branch, stores each decision, then lets the run publish or raises `SentinelBlocked`. |
| Rule checks | `sentinel/checks/rules.py` | GX grain checks, referential integrity, silver-to-gold reconciliation, quarantine shares, unannounced fields, and file completeness against the marker. |
| Anomaly checks | `sentinel/checks/anomalies.py`, `sentinel/anomaly/scores.py` | Volume, freshness, known-defect rates, drift and null rates, each scored against published history only. |
| Thresholds | `sentinel/config.py` | Every number in one place, tuned on the tuning period only. |
| Releases | `sentinel/release.py` (`publish_release`) | Fast-forwards every table's main to the branch, tags it `release_<date>`, and records the snapshot ids. |
| Held batches | `jobs/batches.py` | A blocked run's data waits until a person releases or rejects it; later runs skip it. |
| Metrics store | `sql/metrics_schema.sql` | Check results, profiles, releases, held batches, quarantine samples and notifications, in PostgreSQL. |
| Console | `sentinel/console/app.py` | Streamlit: open problems, releases, check history, quarantine, alerts, restatements. |
| Alerts | `sentinel/alerts.py` | Failure callbacks and a success summary, stored for the console and posted to Slack if a webhook is set. |
| Maintenance | `jobs/maintenance.py`, `dags/maintenance_weekly.py` | Weekly compaction, snapshot expiry and orphan cleanup that never touches an open run or a release. |
| Schema change | `contracts/events_v2.json`, `jobs/contract.py`, `generator/defects.py` | A new optional field, then a renamed field under schema version 2, both landing unchanged in silver. |
| Evaluation replay | `generator/anomalies.py`, `tests/evaluation/evaluate.py` | 180 injected anomalies on a held-out year, scored against a ground-truth manifest only tests read. |

## How to run it

The daily pipeline on your laptop, one date at a time (PostgreSQL running in Docker):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/python -m jobs.run_day --start 2017-01-01 --end 2017-01-31
```

With Airflow, as in Phase 2 (`docker compose up -d --build`, then backfill `chronos_replay` with
`--max-active-runs 1`). Every replayed Sunday also triggers `maintenance_weekly`.

The console:

```bash
.venv/bin/streamlit run sentinel/console/app.py --server.port 8502
```

Pick the database and namespace in the sidebar: `chronos` with an empty prefix is the main lake,
`chronos_eval` with `eval_` is the evaluation.

When a day is blocked, look at it in the console, then decide (`docs/runbook.md`):

```bash
.venv/bin/python -m jobs.batches release 2017-05-07
```

The evaluation (about 2 hours for tuning, 3 for the test period):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/python tests/evaluation/evaluate.py run --until 2017-09-30
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/python tests/evaluation/evaluate.py report --period tuning
```

## How to test it

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) CHRONOS_REQUIRE_POSTGRES=1 .venv/bin/pytest tests/unit tests/scenarios
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) CHRONOS_REQUIRE_OLIST=1 CHRONOS_REQUIRE_POSTGRES=1 .venv/bin/pytest tests
```

The release tests (`tests/scenarios/test_releases.py`) cover a crash between two fast-forwards,
a blocked day held and then released, and maintenance during and after a run.

## The ideas, in plain words

**Write, audit, publish.** A run writes where nobody reads (its branches), gets checked there,
and only then is made visible (main moves to the branch). Readers see yesterday's release or
today's, never something in between.

**A release is a set of snapshots.** Facts and dimensions are only right together. A release
records one snapshot id per table, and the serving database loads them in one transaction, so a
dashboard never mixes today's facts with yesterday's customers.

**Severity levels.** Not every problem should stop the line:
- *Quarantine:* a single unusable row is kept aside with its reason.
- *Flag:* a known source defect is marked on the row.
- *Warn:* the release is published, with the problem recorded.
- *Block:* nothing is published.

Most problems warn; only what would make the numbers wrong blocks.

**Judge a day against good days only.** Baselines are built from published days. A blocked day
never becomes part of the "normal" it was judged against.

**Hold, don't drop.** When a run is blocked, the lines it read are held. The next day's run
starts from the last release but skips held lines, so one bad file doesn't block every day
after it. A person then releases the lines (the next run takes them in) or rejects them.

**Robust scores, like for like.** Volume uses a modified z-score (median and MAD, which outliers
can't drag) against the same weekday. Tuning on real data changed two things:
- *Count new orders, not event lines.* Lines are mostly follow-up events for earlier days.
- *Give the spread a floor of 10% of the median.* Eight quiet weeks otherwise make an ordinary
  dip look extreme.

**Rates of rare things are counts.** A known defect is a few orders out of a few hundred. The
check asks how likely today's count is at the usual rate, using a binomial tail, instead of
comparing noisy daily percentages.

**Tune on one period, judge on another.** Thresholds were set on January to September 2017,
which has no anomalies. The test period (October 2017 to August 2018) was never looked at
while tuning. Its 180 anomalies were placed by a seeded hash, and only the test harness knows
where.

## Questions an interviewer could ask

**What stops a dashboard from seeing a half-built day?** Jobs write on the run's branch; main
only moves in `publish_release`, and serving only loads a published release in one transaction
(`jobs/load_serving.py`). Scenario 8 kills a job midway and checks main is unchanged.

**What if the publish itself dies halfway, with some tables fast-forwarded and others not?**
The release is recorded as `publishing` before the first fast-forward. Rerunning skips tables
already moved and finishes the rest. Until then, `open_run` refuses to start any other date
(`PublishUnfinished`). This is tested in `test_a_crash_between_fast_forwards...`.

**A day is blocked. Does every later day block too?** No. Its lines are held
(`jobs/batches.py`), so the next run reads past them. `test_a_blocked_day_is_held_until_released`
checks that 6 Jan publishes without 5 Jan, and 7 Jan includes 5 Jan once it is released.

**Wouldn't Black Friday trigger your volume check?** On known sale days (`SALE_DAYS` in
`jobs/build_dimensions.py`, read from `dim_date`), volume downgrades to a warning. See the
evaluation: 24 Nov 2017 scored z = 52 and was published with a warning. The days after it
were not on the calendar, and three of them blocked; the report suggests marking the whole week.

**How did you pick thresholds, and what is the false-alarm rate?**
- *The starting point* was the spec's textbook values. On the clean tuning period they blocked
  12 of 273 days and warned on 250.
- *What I changed:* the volume measure and its spread floor, and drift's smoothing and
  per-feature limits. Each change is backed by tuning-period numbers (`docs/decisions.md`).
- *After tuning:* 0 false blocks and 32 warning days on tuning.
- *On the held-out test period, as measured before the warm-up fix:* 1.9% of clean days
  blocked and 14% warned, with 97% of
  anomalies detected. Most warnings were freshness, which never blocks.
- *Current code, re-scored post-hoc:* 5.2% of clean days blocked and 11% warned; blocks rise
  from 109 to 139 of 150 anomalies (`docs/evaluation/report.md`).

**Why is drift only a warning for categories?** With 71 categories and a few hundred items a
day, the mix moves a lot by chance alone. Clean days reached a PSI of 1.0, so a block there
would be a coin toss.

**What does maintenance do, and why can't it break a run?**
- *What it does:* compacts small files, expires snapshots no tag or branch needs, and removes
  orphan files.
- *Why it's safe:* it refuses while a run's branches are open or a publish is unfinished, and
  in Airflow it shares a one-slot pool with `open_run`. Release tags keep every published
  snapshot readable.

**How do you handle a schema change?**
- *A new optional field* is announced in the contract first, then stored in silver
  (`ALTER TABLE ADD COLUMN`, a metadata change in Iceberg).
- *A renamed field* comes with a new `schema_version` and a new contract file. Each line is
  checked against its own version, and the field is mapped back to its old name, so gold never
  notices.
- *A field nobody announced* raises a warning, not a block (`new_fields`).

**How do you know the evaluation isn't graded by the pipeline itself?** The ground-truth
manifest says where every anomaly is. Only `tests/evaluation/evaluate.py` reads it; pipeline
code never does (CLAUDE.md rule).

## What isn't done yet

- **Findings 2 to 4 of the evaluation report** are candidate changes, not made: they would need
  a held-out period to justify, and Olist has no more data.
- **The failure callback** hasn't fired in a live Airflow run; only its unit test covers it.

- **Email alerts:** Slack and the console only (an SMTP account would be another credential).
- **The evaluation measures six anomaly types**, not every way data can go wrong (spec: Known
  limitations).
- **The schema change isn't part of the evaluation feed:** it was added after the evaluation
  started. It is covered by the truth tests and silver tests instead.
