# Phase 0 notes: foundations

Phase 0 builds nothing that moves data yet. It makes sure the ground is solid: the data is what
the spec says, every metric has one written definition, and the local tools can do the hard
things later phases depend on.

**Done when** (spec, Phased build plan): "Your numbers match or update Data sources, and the shared
and all eight metric definitions are written." Both are true:

- The numbers were rerun on the Kaggle download. They match, except the review-based ones, which
  the spec now carries in Kaggle's version (see "What the data check found").
- `docs/metrics.yml` holds the 9 shared definitions and all 8 metrics.

## Truth test, last passed

Not applicable: Phase 0 builds no tables. The truth test starts in Phase 1.

## What was built

| Piece | Where | In one sentence |
| --- | --- | --- |
| Iceberg smoke test | `tests/unit/test_iceberg_smoke.py` | Proves local Spark can MERGE, write to a branch, read it three ways and publish it by fast-forward. |
| Test Spark session | `tests/conftest.py` | PySpark 3.5.6 with the Iceberg 1.10.0 jar, a fresh warehouse per test run. |
| Metric definitions | `docs/metrics.yml` | Every metric's population, numerator, denominator, grain, slices, exclusions, dirty-period rule and expected numbers. |
| Repository skeleton | `generator/`, `jobs/`, `sentinel/`, `dags/`, `sql/`, `tests/`, `infra/`, `contracts/` | The spec's folder layout, empty for now. |
| Project config | `pyproject.toml` | Pinned dependencies, pytest and ruff settings. |
| CI | `.github/workflows/ci.yml` | Runs `ruff check .` and `pytest` on every push, on Python 3.11 and Java 17. |
| Local services | `docker-compose.yml`, `.env.example` | Chronos PostgreSQL and Airflow 3.4.0 in Docker. |
| Decision record | `docs/decisions.md` | What changed from the spec, what is proposed, and what is still open. |

## How to run it

```bash
source .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d
```

- Airflow UI: <http://localhost:8080>, user `airflow`, password `airflow` (local only).
- Chronos PostgreSQL: `localhost:5433`, database `chronos`, user and password `chronos` (local only).
  From the terminal: `docker compose exec chronos-db psql -U chronos -d chronos`.
- Stop everything with `docker compose down`. Add `-v` to also delete the databases' data.

The first `pytest` run downloads the Iceberg jar (about 47 MB) from Maven Central into `~/.ivy2`,
so it needs the network once.

## How to test it

```bash
ruff check .        # lint
pytest              # the smoke test, about 10 seconds after the first run
docker compose ps   # every service should say (healthy)
curl -s http://localhost:8080/api/v2/monitor/health
```

Checked on 22 Nov 2026: ruff clean, 1 test passed, all six containers healthy, Airflow 3.4.0 on
Python 3.11 with the Amazon provider 9.39.0, PostgreSQL 16.16.

**Not checked yet:** the GitHub Actions workflow has not run, because nothing has been pushed.
It will run on the first push.

## The smoke test, in plain words

Iceberg tables keep every commit as a snapshot, and they can have **branches**: named lines of
snapshots that you write to without changing `main`, the version everyone reads. That gives
**write-audit-publish**: write a day's data to a branch, check it, and only then move `main` to it.

The test does exactly that on a tiny table:

1. Create a table (Iceberg format version 2, because Athena can't read version 3), insert a row,
   and MERGE an update into it.
2. Create a branch called `audit` and set `spark.wap.branch = audit`. Insert a second row.
3. Count rows four ways: with the setting on (2), with it off (1, so main is untouched), with
   `VERSION AS OF 'audit'` (2) and through the name `smoke.branch_audit` (2).
4. Call `fast_forward` to move main to the branch. Main now has 2 rows.

The first count is the one that mattered. The pipeline's jobs assume that while
`spark.wap.branch` is set, **reads** come from the branch too, not just writes. It returned 2, so
the design stays as written.

## What the data check found

Both profiling scripts were rerun on the Kaggle download. Everything matches the spec except:

1. **The reviews file.** Kaggle's has 99,224 rows; the mirror the spec was first measured on has
   100,000. So: 547 orders with several reviews (spec said 555), 156 ties (157), 789 shared
   review_ids (802), late orders average 2.27 stars with 53.8% 1-star (2.26, 54.2%), on-time
   orders 4.29 and 6.6% (4.28, 6.8%), and Q7's top categories score 3.99 to 4.22 with low-score
   shares of 11.4% to 16.2% (3.97 to 4.22; 11.8% to 16.8%). The spec now carries these.
2. **Address changes: 250 or 252?** The spec's SCD Type 2 rule versions a customer when zip,
   city **or** state changes, and says 250 people get a new version. Under that exact rule it's
   252 (and 6 change more than once, not 5). The 250 counts zip changes only. The 2 extra people
   keep their zip but their city name changes. Decided: keep the full-address rule, so 252 people
   get a new version; the spec now says so (`docs/decisions.md`).

Q1 to Q3, Q5 and Q6 match the spec exactly, and so do the Q7 flags per quarter.

## Questions for you before Phase 1

These are in `docs/decisions.md` with options and a recommendation:

- The proposed setup choices: LocalExecutor, the Python 3.11 Airflow image, a separate Chronos
  PostgreSQL, ruff skipping `tools/`, and pinning pandas and numpy.
- How the metric regression test handles Q7's Portuguese category names, and the late-delivery
  rate that `measure_spec.py` doesn't produce.
- Confirm the dataset's licence on the Kaggle page yourself (the spec asks the owner to).

## Questions an interviewer could ask

**What does your smoke test prove?**
That one machine can do every Iceberg operation the pipeline needs: MERGE, branch writes, three
ways of reading a branch, and fast-forward. `tests/unit/test_iceberg_smoke.py`.

**Why was the first count the most important?**
Jobs read tables they wrote earlier in the same run. If reads ignored `spark.wap.branch`, a job
would read main and miss its own run's data, silently. Every job would then have to name its
branch. The assertion message in the test says this.

**What is write-audit-publish?**
Write to a branch, audit the branch, then publish by moving main to it. Readers only ever see
main, so they never see unaudited data. The last three steps of the smoke test are a miniature.

**Why `fast_forward` and not copying the data into main?**
Fast-forward only moves a pointer, so it is instant and copies nothing. It also refuses if main
has moved since the branch was made, which stops a publish from overwriting someone else's
commit. That is why the spec runs one date at a time.

**Why Iceberg format version 2?**
Athena can't read version 3 tables. The test asserts the table really is version 2.

**Why pin PySpark 3.5.6, Iceberg 1.10.0 and Python 3.11?**
They are what AWS Glue 5.1 runs, so code tested on the laptop behaves the same on Glue in Phase
4. `pyproject.toml`, `tests/conftest.py`.

**Your Kaggle numbers differed from your spec. What did you do?**
Found the cause (a different reviews file), checked that everything else matched exactly, updated
the spec to Kaggle's numbers, and recorded each change. `docs/decisions.md`.

**Why write metric definitions in a file before writing any code?**
Most arguments about a number are arguments about its definition: which orders count, which date
a row belongs to, what happens to ties. One file means a dashboard, a test and a person all use
the same rule. `docs/metrics.yml`.

**Why the reliable window?**
Late 2016 is sparse, and Sep and Oct 2018 have only 20 orders, so monthly rates there would swing
on a handful of orders. Every metric counts only purchases from Jan 2017 to Aug 2018.

**Why does retention leave recent customers out of the denominator?**
Someone who first bought in August 2018 has had no chance to come back within 90 days in this
data. Counting them as "didn't return" would drag the rate down falsely. That is censoring.
`docs/metrics.yml`, `q5_retention`.

**Why does the order review score need three tie-breakers?**
Review creation dates almost never have a time of day (85 of 99,224 do), so 156 orders have
two reviews on the same latest date. Without a full ordering, two runs could pick different reviews and the metric would change
for no reason. `docs/metrics.yml`, `order_review_score`.

**Why is Chronos's PostgreSQL separate from Airflow's?**
Chronos's database plays an operational source system. Airflow's metadata is the orchestrator's
own bookkeeping. Keeping them apart means resetting one can't damage the other.
`docker-compose.yml`.

**Why LocalExecutor when the official file uses Celery?**
Celery spreads tasks across worker machines. Runs here are strictly one at a time, so it would
only add Redis and a worker container to the laptop. Proposed in `docs/decisions.md`.

**Why doesn't the linter check `tools/`?**
Those scripts produce the reference numbers the spec quotes. Keeping them exactly as measured is
worth more than tidier style. `pyproject.toml`.
