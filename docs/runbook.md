# Runbook

What to do when something goes wrong, and why. Each procedure says which phase made it true.

## A day's run failed

Rerun it. Every step is idempotent (spec: Late-arriving data, scenario 7): bronze skips files in
its manifest, silver inserts only event_ids it lacks, gold merges only rows that changed, and
every stamp comes from the replay clock. A run that died midway left its progress marks where
they were, so the rerun redoes exactly the unfinished work.

In Airflow, clear the failed task (or the whole run) for that date. Later dates wait for it:
every task has `depends_on_past` and `wait_for_downstream`.

## Someone reruns a date that was already published

Decided on 23 Nov 2026 (`docs/decisions.md`): a rerun of an already-published date republishes
nothing. The run logs "already published in release X" and exits successfully. It doesn't block
and it doesn't fail, so a cleared task can't leave a red run behind.

Why: a run computes dimensions as its date knew them. Rerunning an old date after later dates
have run would put the old state back, then the next run would put the new one back: published
numbers would flicker for no reason.

**Status:** implemented in Phase 3, with releases (it needs the release manifest to know what
was published). Until then, don't clear an old date after later dates have run.

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
