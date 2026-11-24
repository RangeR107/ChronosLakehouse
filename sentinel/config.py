"""DataSentinel's thresholds, in one place (spec: DataSentinel, "Checks").

Starting values from the spec. Tune them on the tuning period only, and record every change with
its reason in docs/decisions.md; never adjust them after looking at the held-out test period
(spec: Testing and evaluation).
"""
QUARANTINE_BLOCK_SHARE = 0.005   # schema contract and valid values: block above 0.5% of a day
RATE_TAIL_LIMIT = 0.001          # known source defects: binomial tail at the baseline rate
# Volume: orders placed that the run landed, modified z against the same weekday (tuned on
# 2017-01..09, docs/decisions.md). 5 and a MAD floor of 10% of the median gave no false blocks;
# 3.5 with no floor gave 12, on holidays and on weeks whose history happened to be tight.
VOLUME_Z_LIMIT = 5.0
VOLUME_MAD_FLOOR = 0.10          # the spread is at least 10% of the median
VOLUME_WEEKS = 8                 # ... over the latest 8 published same weekdays
VOLUME_LOOKBACK_WEEKS = 12       # ... found within 12 weeks, so one blocked day doesn't
                                 # leave a weekday warn-only for 8 weeks
FRESHNESS_Z_LIMIT = 3.5          # hours from the day's latest event to its run, same weekday
# PSI per feature, tuned on the clean tuning period with smoothed counts (docs/decisions.md).
# Its largest clean values: payment type 0.41, price 0.36, review score 0.22, category 1.0.
# Category has 71 bins, so a day of a few hundred items is noisy by itself: it only warns.
PSI_SMOOTHING = 0.5              # added to every bin's count before PSI, so an empty bin is finite
DRIFT_WARN = {"payment_type": 0.25, "price": 0.25, "score": 0.25, "category": 1.25}
DRIFT_BLOCK = {"payment_type": 0.5, "price": 0.5, "score": 0.5}   # category never blocks
NULL_RATE_WARN = 0.05            # null-rate change: warn when the share moves by 5 points
BASELINE_DAYS = 28               # trailing window for pooled baselines (defects, drift, nulls)
MISSING_FILE_ALERT_DAYS = 2      # a listed file still missing after 2 days raises an alert

# Price buckets for the price-level drift check, in BRL.
PRICE_BUCKETS = [0, 25, 50, 100, 200, 500, float("inf")]
