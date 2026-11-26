# Demo video script (about 4 minutes)

A plan for recording the demo the spec asks for in Phase 5. Record the screen, with your
voice over it.

Before recording, run `make demo DAYS=90` (about 35 minutes), so the results are ready. The
default 30 days shows no late deliveries on the dashboard, because Olist had none in January
2017; by the end of March there are 52.

## 1. The problem (30 seconds)

Show the README's "The results, in one minute".

> "Pipelines break on late, duplicated and wrong data, and on jobs that die halfway. Chronos
> replays 100,000 real Olist orders as a daily feed that does all of that on purpose, and
> proves two things: the tables end up exactly right, and nobody ever sees a half-built day."

## 2. How it works (45 seconds)

Show the README's flowchart.

> "Each day lands as hourly files and a completion marker. Bronze keeps the raw lines, silver
> types and deduplicates them, and gold is the star schema. Every write goes to that day's own
> Iceberg branch. DataSentinel audits the branch, and only a passing day becomes a release that
> the dashboards load in one transaction."

## 3. The demo run (60 seconds)

Show the terminal output of `make demo`, scrolled to 10 January.

> "Here the producer resent a file under a name its marker doesn't list. DataSentinel blocked
> the day: nothing was published, and its data was held. After checking it was a stray resend,
> it's released, and the next day's release includes it. Every other day published, even with
> late and duplicate events in the feed."

Then scroll to 1 February.

> "Once the warm-up ends, the checks can block on their own. This one is a false alarm on real
> data, and I left it in rather than tune it away on the test period. The demo releases it, and
> says plainly that nobody checked it."

## 4. The console and the dashboard (60 seconds)

Run `make console`. Show these tabs:
- **Releases:** the list of releases, and which ones are marked incomplete.
- **Alerts and blocks:** the 10 January block.
- **Check history:** pick a check and show its scores over time.

Then run `make dashboard`. Show the late-delivery rate by month and region (52 of 3,996
delivered orders late, about 3% in the South and the Northeast in March), and the restatement
report.

> "The restatement report is the honest part: it shows what the dashboard first said and what
> it says now that the late events have landed."

## 5. The evidence (45 seconds)

Show `docs/evaluation/report.md`'s side-by-side table, then the truth test passing
(`make truth`, or a saved run).

> "On a held-out year with 180 injected anomalies, DataSentinel detected 97% and falsely blocked
> 1.9% of clean days. The test period also found a warm-up bug, which I fixed and report
> separately, without re-tuning on the test data. The truth test proves every business table
> equals a reference built straight from Olist's files."

## Close (10 seconds)

> "Everything is in the repository, with every decision and its alternatives in
> docs/decisions.md."
