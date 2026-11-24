# DataSentinel evaluation report

How well does DataSentinel catch bad days without blocking good ones? This report answers that
on a held-out year of the real Olist feed. It was written on 24 Nov 2026. The raw results are
`tuning.json` and `test.json` next to this file (as measured), and `tuning_rescored.json` and
`test_rescored.json` (current code, re-scored). The harness is `tests/evaluation/evaluate.py`;
the design and scoring rules were fixed before the test period ran (`docs/decisions.md`).

## Setup

- **Feed:** the real Olist orders as the daily event feed, with the spec's reversible defects
  (late, duplicate, out-of-order and corrected events, late files, late master rows).
- **Isolation:** its own database (`chronos_eval`), lake (`lake/eval/`) and namespace
  (`eval_`). It shares nothing with the main lake.
- **Tuning period:** 1 Jan to 30 Sep 2017, 273 days, no anomalies. Thresholds were set here
  only.
- **Test period:** 1 Oct 2017 to 31 Aug 2018, 335 days, including Black Friday (24 Nov 2017)
  and Christmas. Never looked at while tuning.
  - *Anomalies:* 30 of each of 6 types on 180 distinct days, placed by a seeded SHA-256 draw.
  - *Clean days:* the other 155, including Black Friday.
- **Nobody intervenes:** a blocked day's batches stay held, as if no one ever answered.
- **Scoring:**
  - *Detected:* a check meant for that type fails on the anomaly's own run.
  - *False block:* a clean day that blocked.
  - *False warning:* a clean day with a failing warning that a late or missing file in the
    manifest doesn't explain.

| Anomaly | What it does | Checks meant to catch it |
| --- | --- | --- |
| missing_files | 1 to 12 hourly files listed in the marker but never sent | completeness: missing files |
| duplicate_file | one file sent again under a name the marker doesn't list | completeness: unlisted files |
| volume_spike | every order of the day sent 2 to 4 more times under new ids | volume |
| mix_shift | prices x3 to x5, or 80% of payments switched to voucher | drift: price, payment type |
| schema_break | a required field renamed, same schema_version | schema contract |
| null_burst | 30% to 60% of one field nulled | schema contract, known defects, null rates |

## Two systems, two sets of numbers

The numbers below describe one of two versions of DataSentinel. Every headline says which.

- **As measured:** the code that ran the evaluation, before the warm-up fix (finding 1). These
  are the held-out results: nothing was changed after looking at the test period.
- **Current code, re-scored:** today's code. The warm-up fix changes only which failures block,
  so the stored scores were re-scored with today's rule
  (`evaluate.py report --period test --rescore-warm-up`, giving `test_rescored.json`).
  - *Post-hoc, not held out:* the fix was made after seeing the test period.
  - *Approximate:* a day the re-score turns into a block would really have been held, so
    later days' batches and baselines would have differed a little.

**Test period, side by side** (alerted = a check meant for that type failed, warned or
blocked):

| Day type | Days | Alerted (both) | Blocked, as measured | Blocked, current code | Bad data published, as measured | Bad data published, current code |
| --- | --- | --- | --- | --- | --- | --- |
| missing_files | 30 | 30 | 0 | 0 | n/a: published marked incomplete (30) | n/a: published marked incomplete (30) |
| duplicate_file | 30 | 30 | 30 | 30 | 0 | 0 |
| volume_spike | 30 | 30 | 22 | 23 | 8 | 7 |
| mix_shift | 30 | 30 | 2 | 30 | 28 | 0 |
| schema_break | 30 | 30 | 30 | 30 | 0 | 0 |
| null_burst | 30 | 25 | 25 | 26 | 5 | 4 |
| **All anomalies** | **180** | **175** | **109** | **139** | **41 of 150** | **11 of 150** |

| Clean days (155) | As measured | Current code, re-scored |
| --- | --- | --- |
| Blocked (false blocks) | 3 (1.9%) | 8 (5.2%) |
| Warned | 22 (14.2%) | 17 (11.0%) |
| Passed | 130 | 130 |

- **Blocks by other checks:** under the current code, one more volume spike and one more null
  burst block, through a known-defect rate rather than a check meant for their type. That is
  why null bursts show 26 blocked but 25 alerted.
- **The 5 new false blocks** are known-defect rates on 30 May, 13 Jun, 27 Jun, 12 Jul and
  29 Aug 2018. They fall in Olist's last, thinner months.
- **Tuning period (273 clean days):** as measured, 0 blocked and 32 warned. Current code,
  re-scored: 1 blocked (1 Feb 2017, a known-defect rate) and 31 warned.

## Results as measured (held out)

The rest of this section is the evaluation as it ran: held-out evidence for the code before
the warm-up fix.

**Outcome per day:**

| Day type | Days | Blocked | Warned | Passed |
| --- | --- | --- | --- | --- |
| missing_files | 30 | 0 | 30 | 0 |
| duplicate_file | 30 | 30 | 0 | 0 |
| volume_spike | 30 | 22 | 8 | 0 |
| mix_shift | 30 | 2 | 28 | 0 |
| schema_break | 30 | 30 | 0 | 0 |
| null_burst | 30 | 25 | 2 | 3 |
| **clean** | **155** | **3** | **22** | **130** |

**Per anomaly type:**

| Type | Detected | Blocked | Published | With bad data | Published, marked incomplete |
| --- | --- | --- | --- | --- | --- |
| missing_files | 30 / 30 | 0 | 30 | n/a (a gap, not bad rows) | 30 |
| duplicate_file | 30 / 30 | 30 | 0 | 0 | 0 |
| volume_spike | 30 / 30 | 22 | 8 | 8 | 1 |
| mix_shift | 30 / 30 | 2 | 28 | 28 | 4 |
| schema_break | 30 / 30 | 30 | 0 | 0 | 0 |
| null_burst | 25 / 30 | 25 | 5 | 5 | 0 |

**Headline, as measured (before the warm-up fix):**
- **Detected:** 175 of 180 anomalies (97.2%), every one by its own day's run (time to
  detection: 0 extra runs). The warm-up fix doesn't change this.
- **Blocked:** 109 of the 150 anomalies meant to block (73%). Missing files are designed to warn
  and publish the day as incomplete, and all 30 did. Current code, re-scored: 139 of 150 (93%).
- **Clean days:** 3 of 155 falsely blocked (1.9%) and 22 warned (14.2%). 14 of the warnings
  are freshness, which only ever warns. Current code, re-scored: 8 blocked (5.2%) and 17 warned.
- **Black Friday:** published, with a volume warning (z = 52, against a median of 134.5 orders).
  The known-sale-day rule turned what would have been a block into a warning. Same under the
  current code.
- **Bad data published:** 41 of 150 days (excluding missing files): 28 price or payment shifts,
  8 volume spikes, 5 null bursts. Finding 1 explains most of them. Current code, re-scored: 11.

**For comparison, the tuning period after tuning, as measured:** 273 clean days, 0 blocked, 32
warned. Before tuning it was 12 blocked and 250 warned.

## What the test period found

These are findings, not fixes tuned on the test set. Numbers marked *post-hoc* are re-scored
from stored check results and are not held-out evidence.

1. **Warm-up never ended when many days were blocked (a bug, now fixed).**
   - *What happened:* drift scored 2 to 3.7 on price and payment shifts, far past its block
     line of 0.5, but only warned. Warm-up counted the *published* days in the 28-day trailing
     window. With about half the test days blocked and held, that count stayed between 11 and
     26, so drift and the known-defect checks never left warm-up.
   - *The spec says otherwise:* "until a check has 4 weeks of history".
   - *Fix:* warm-up now counts days since the first published release (`history_age` in
     `sentinel/checks/anomalies.py`).
   - *Re-scored post-hoc (table above):* all 30 price and payment shifts would have blocked,
     and so would 5 more clean days (false blocks 3 to 8, 5.2%), all in Olist's thin last
     months. This changes the trade-off; it isn't a free win.
   - *A correction:* an earlier rough estimate here said 7 more clean days. It also counted
     drift results that pass with too few rows, whose stored "score" is a row count.
   - *On the tuning period* the fix adds 1 false block: 1 Feb 2017, an unusual count of
     unavailable orders.
2. **Volume spikes during heavy blocking only warned (8 of 30).**
   - *Why:* volume needs 8 published same-weekday points within 12 weeks. With half the days
     blocked, those 8 days had 4 to 7 points, so the check stayed warn-only.
   - *Candidate change, not made:* a shorter minimum once the check has had 8 points before.
     It needs a new held-out period to justify.
3. **The days after Black Friday blocked.**
   - *What:* all 3 false blocks were volume, on 26 Nov, 2 Dec and 4 Dec 2017. The promotions
     lasted longer than the one day the sale calendar knows.
   - *Candidate change:* mark Black Friday week, or the following weekend, as a sale period in
     `dim_date`.
4. **Null delivery dates slipped through (all 5 null-burst misses).**
   - *What:* the misses were nulls in `delivered.order_delivered_customer_date`. That field is
     optional in the contract (Olist has 8 real undated deliveries), so nothing is quarantined.
   - *Why the backstop failed:* the known-defect check `delivered_without_date` counts defects
     only on orders *placed* in the run's batch. A delivery event belongs to an order placed
     days earlier.
   - *Candidate change:* count lifecycle defects on the orders the batch touched.
5. **A new warning, mid-test.** From 20 Dec, the run included the warn-only check for fields a
   contract doesn't list. It fired on 23 schema breaks (the renamed field) and never on a clean
   day.

## What happened during the run

The test period was run in four parts; every incident is in `docs/decisions.md`.

- **7 Oct:** I added a contract file while the run was going; the old silver code read it and
  crashed. I resumed with the same code and feed.
- **20 Dec:** a real generator bug. An anomaly day's late file was written from the original
  events, so its checksum didn't match the marker, and `land_to_bronze` refused it. Fixed, with
  a regression test, then resumed.
- **12 Jun:** a publish read a table's refs without its run branch. The release stayed
  `publishing`, which stopped later dates by design. Finishing the publish by the runbook
  released it.

The blocking checks and thresholds were the same for every test day.

## Limits

- **Six anomaly types only.** Real incidents are messier (spec: Known limitations).
- **Nobody released or rejected held days.** In production a person would. Released false
  blocks would return to the baselines, which makes findings 1 and 2 less severe.
- **The schema change isn't in this feed.** It was built after the evaluation started; the
  truth tests cover it.
- **No second held-out period.** Olist's data ends in 2018, so the fixes above can only be
  estimated post-hoc, not re-evaluated cleanly.
