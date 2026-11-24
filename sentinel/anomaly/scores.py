"""Anomaly scores (spec: DataSentinel, "Anomaly detection: methods and why").

Plain functions over numbers, so each can be checked by hand and in unit tests:

- modified_z: robust distance from the median, for volume.
- rate_tail: for known-defect counts (a few out of tens or hundreds of orders a day), the binomial
  tail probability of today's count at the pooled baseline rate, floored by the rule of three.
- psi: Population Stability Index between two category distributions, for drift.
- decide: wraps a score into a decision that records its method and baseline, with warm-up.
"""
import math
from dataclasses import dataclass, field
from statistics import median

from sentinel import config

# Iglewicz and Hoaglin's constant: 0.6745 makes the median absolute deviation comparable to a
# standard deviation for normal data.
MAD_SCALE = 0.6745
PSI_FLOOR = 1e-4  # a share of zero would make log(0); treat it as this small instead

WARM_UP_DAYS = 28   # spec: until 4 weeks of history a check only warns
# Volume and freshness compare with the same weekday, one point a week: they warn until all
# config.VOLUME_WEEKS points are there. On the real slice, 4 to 7 points taken during Olist's
# early-2017 growth made normal days score up to 22 (docs/decisions.md).


def modified_z(value: float, history: list[float],
               mad_floor: float = 0.0) -> tuple[float, float, float] | None:
    """(score, median, MAD), or None when the history has no spread (MAD is zero).

    mad_floor keeps the spread at least that share of the median: eight quiet weeks can have a
    MAD of 1% of the median, and then an ordinary 8% dip scores past 5 (8 Sep 2017).
    """
    m = median(history)
    mad = max(median(abs(h - m) for h in history), mad_floor * m)
    if mad == 0:
        return None
    return MAD_SCALE * (value - m) / mad, m, mad


def rate_tail(count: int, rows: int, base_count: int, base_rows: int) -> tuple[float, float]:
    """(P(X >= count) for X ~ Binomial(rows, p), p) with p the pooled baseline rate.

    p has a floor of 3 / base_rows (the rule of three: a 95% upper bound on the rate when no
    defect was seen), so a baseline of all zeros doesn't make any single defect "impossible".
    """
    p = max(base_count / base_rows if base_rows else 0.0, 3 / base_rows if base_rows else 1.0)
    p = min(p, 1.0)
    if count <= 0:
        return 1.0, p
    if p >= 1.0:
        return 1.0, p
    # Sum the tail in log space; days hold at most a few thousand rows, so this is cheap.
    log_p, log_q = math.log(p), math.log1p(-p)
    tail = 0.0
    for k in range(count, rows + 1):
        term = (math.lgamma(rows + 1) - math.lgamma(k + 1) - math.lgamma(rows - k + 1)
                + k * log_p + (rows - k) * log_q)
        tail += math.exp(term)
        if term < -50 and k > rows * p:  # the remaining terms are negligible
            break
    return min(tail, 1.0), p


def psi(expected: dict[str, float], actual: dict[str, float], smoothing: float = 0.0) -> float:
    """Population Stability Index of actual against expected, both as counts or shares.

    smoothing is added to every bin's count first. Without it, a bin empty today counts as a
    share of PSI_FLOOR, and with 71 categories the empty ones alone put clean days at 0.68.
    """
    keys = set(expected) | set(actual)
    if smoothing:
        expected = {k: expected.get(k, 0) + smoothing for k in keys}
        actual = {k: actual.get(k, 0) + smoothing for k in keys}
    e_total = sum(expected.values()) or 1.0
    a_total = sum(actual.values()) or 1.0
    total = 0.0
    for k in keys:
        e = max(expected.get(k, 0) / e_total, PSI_FLOOR)
        a = max(actual.get(k, 0) / a_total, PSI_FLOOR)
        total += (a - e) * math.log(a / e)
    return total


@dataclass
class Decision:
    status: str            # "pass" or "fail"
    method: str            # modified_z, rate_limit, psi, warm_up, threshold
    observed: float | None
    expected: str          # the baseline it was compared with, in words
    warn_only: bool = False
    detail: dict = field(default_factory=dict)


def warm_up(history_days: int) -> bool:
    """True while a check has too little history to block (it can still warn)."""
    return history_days < WARM_UP_DAYS


def volume_decision(value: float, history: list[float], z_limit: float,
                    mad_floor: float = 0.0) -> Decision:
    """Modified z-score of today's count against the same weekday's history.

    With no spread in the history, falls back to the rate limit on the count itself.
    """
    if not history:
        return Decision("pass", "warm_up", value, "no history yet", warn_only=True)
    scored = modified_z(value, history, mad_floor)
    if scored is None:
        m = median(history)
        return Decision("fail" if value != m else "pass", "no_spread", value,
                        f"every day in history was {m:g}",
                        warn_only=len(history) < config.VOLUME_WEEKS)
    z, m, mad = scored
    return Decision("fail" if abs(z) > z_limit else "pass", "modified_z", round(z, 3),
                    f"median {m:g}, MAD {mad:g}, |z| <= {z_limit:g}",
                    warn_only=len(history) < config.VOLUME_WEEKS,
                    detail={"median": m, "mad": mad, "weeks": len(history)})
