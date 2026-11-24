# Runbook

What to do when something goes wrong, and why. Each procedure says which phase made it true.

## A day's run failed

Phase 3: nothing was published, so main is as the last release left it. You don't have to rerun
the date: the next date's run starts from the last published release and picks the failed date's
data up with its own (spec: Publishing). That holds for a crash; a *blocked* audit is different
(below). Rerunning it is also safe, because every step is
idempotent and runs on a fresh branch from main. Fix the cause either way, or the next date fails
the same way.

In Airflow, a failed or blocked date doesn't stop later dates; there is no `depends_on_past`
(`docs/decisions.md`). Dates still run one at a time: `max_active_runs = 1`, and a backfill
must be given `--max-active-runs 1`.

## The audit blocked a day

DataSentinel found a blocking problem. Nothing is published, the branches are kept, and
`metrics.releases` has the date as `blocked`, with the failed checks. See why in
`metrics.check_results` for that run_id.

The batches the run processed are now **held** (`metrics.batch_holds`): later dates run and
publish without them. Decide what happens to them:

- A false alarm, or the cause is fixed (say a stray file was removed): release them. The next
  run takes them in, and they are published with its release.

  ```bash
  python -m jobs.batches release 2017-01-05
  ```

- The data is bad: reject them. They are never processed.

  ```bash
  python -m jobs.batches reject 2017-01-05
  ```

Add `--namespace` for the evaluation lake. Never force-publish the blocked branch. Why holds
exist: a blocked batch's bad data stays in bronze. Taking it in again automatically would block
every later run, or publish it unjudged a day later (`docs/decisions.md`, 2026-11-24).

## A publish died midway

`publish_release` fast-forwards one table at a time, so some tables may have moved and others
not. The release stays `publishing`, the dashboards still show the previous release, and every
later date's `open_run` fails with "still publishing". That stop is on purpose: no run may
branch from a half-moved main.

Rerun `publish_release` for that date (in Airflow, clear that task). It skips the tables
already moved, finishes the rest, and marks the release published. Then let `load_serving` and
`clean_up` run, and later dates go ahead.

## Someone reruns a date that was already published

Decided on 23 Nov 2026 (`docs/decisions.md`): a rerun of an already-published date republishes
nothing. The run logs "already published in release X" and exits successfully. It doesn't block
and it doesn't fail, so a cleared task can't leave a red run behind.

Why: a run computes dimensions as its date knew them. Rerunning an old date after later dates
have run would put the old state back, then the next run would put the new one back: published
numbers would flicker for no reason.

**Status:** implemented in Phase 3, with releases: `open_run` checks the release manifest.

## A logic bug changed gold, and the fix is merged

Don't rerun individual dates; rebuild. Bronze holds every line ever received, exactly as
received, with its arrival time, so silver and gold can always be rebuilt from it:

1. Stop the daily DAG (pause `chronos_daily` and `chronos_replay`) so nothing writes meanwhile.
2. Drop the silver and gold namespaces. Keep bronze: the events, the file manifest and the
   master extracts.
3. Run every business date again, in order, from the first date bronze holds to the last one
   published:

   ```bash
   JAVA_HOME=$(/usr/libexec/java_home -v 17) .venv/bin/python -m jobs.run_day --start 2017-01-01 --end 2017-03-12
   ```

   Each run reads only the bronze lines ingested by its own replay time, so it sees exactly what
   that date's original run saw. No files are reloaded (the manifest has them all) and no
   source rows are re-extracted (bronze's extracts already go past every date's upper bound).
4. Compare the rebuilt gold with the previous one before serving it, and record the differences
   (they are the bug's effect).

**Tested:** `test_a_full_rebuild_from_bronze_gives_the_same_tables` in
`tests/scenarios/test_scenarios.py` copies a defected run's bronze into a fresh namespace,
replays every date and checks the business tables equal the clean run's.

**Phase 3** settles how a rebuild is published: the rebuilt dates become new releases, so the
restatement report shows what the bug had changed.

## The truth test fails

Run `make truth`. The failure message names the table, how many rows are missing, extra or
different, and the first three differing rows with both values. The reference build is
`tests/truth/reference.py`; decide which side is wrong before changing either. Every truth-test
failure so far was a real pipeline bug (`docs/decisions.md`).

## Alerts

Every failed task (after its last retry), every blocked audit and every published run leaves a
notice in `metrics.notifications`; the console's "Alerts and blocks" tab lists them. To also get
them in Slack, create an incoming webhook in your Slack workspace and put it in `.env` (which is
never committed):

    CHRONOS_SLACK_WEBHOOK=https://hooks.slack.com/services/...

Then recreate the Airflow containers so they see it: `docker compose up -d`. The `delivered`
column says what happened to each post (`not_configured`, `slack 200`, or `failed: ...`). A
failed post never fails the run.
