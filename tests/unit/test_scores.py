"""DataSentinel's anomaly scores against values worked out by hand (spec: Anomaly detection)."""
import math

import pytest

from sentinel.anomaly.scores import modified_z, psi, rate_tail, volume_decision


def test_modified_z_matches_the_formula():
    # median 3; absolute deviations 2, 1, 0, 1, 2 have median 1; 0.6745 * (10 - 3) / 1
    z, m, mad = modified_z(10, [1, 2, 3, 4, 5])
    assert (m, mad) == (3, 1)
    assert z == pytest.approx(4.7215)


def test_modified_z_is_undefined_without_spread():
    assert modified_z(4, [0, 0, 0, 0]) is None


def test_an_outlier_in_the_history_barely_moves_the_baseline():
    calm = modified_z(150, [100, 102, 98, 101, 99, 100, 103, 97])
    spiked = modified_z(150, [100, 102, 98, 101, 99, 100, 103, 900])
    # The 900 doubles the mean (100 to 200) but moves the median only from 100 to 100.5.
    assert (calm[1], spiked[1]) == (100, 100.5)


def test_rate_tail_floors_a_zero_baseline_with_the_rule_of_three():
    tail, p = rate_tail(3, 100, base_count=0, base_rows=1000)
    assert p == pytest.approx(0.003)
    exact = 1 - sum(math.comb(100, k) * p**k * (1 - p) ** (100 - k) for k in range(3))
    assert tail == pytest.approx(exact)


def test_rate_tail_of_no_defects_is_one():
    assert rate_tail(0, 500, base_count=2, base_rows=10_000)[0] == 1.0


def test_psi_of_a_known_shift():
    # (0.9 - 0.5) ln(0.9 / 0.5) + (0.1 - 0.5) ln(0.1 / 0.5)
    assert psi({"a": 50, "b": 50}, {"a": 90, "b": 10}) == pytest.approx(0.8789, abs=1e-4)
    assert psi({"a": 3, "b": 1}, {"a": 30, "b": 10}) == pytest.approx(0)


def test_volume_decision_warns_only_until_eight_weeks():
    short = volume_decision(500, [100, 110, 90], z_limit=3.5)
    assert short.status == "fail" and short.warn_only and short.method == "modified_z"
    # Seven weeks is past the general 4-week warm-up, but same-weekday checks need all 8.
    seven = volume_decision(500, [100, 110, 90, 105, 95, 100, 102], z_limit=3.5)
    assert seven.status == "fail" and seven.warn_only
    full = volume_decision(500, [100, 110, 90, 105, 95, 100, 102, 98], z_limit=3.5)
    assert full.status == "fail" and not full.warn_only
    assert volume_decision(101, [100, 110, 90, 105, 95, 100, 102, 98], 3.5).status == "pass"


def test_mad_floor_keeps_a_quiet_history_from_flagging_ordinary_days():
    # 8 Sep 2017 in the tuning period: a MAD of 8.5 around 803 made an 8% dip score -6.3.
    quiet = [795, 800, 803, 803, 806, 810, 812, 790]
    assert abs(modified_z(740, quiet)[0]) > 5
    z, _, mad = modified_z(740, quiet, mad_floor=0.10)
    assert mad == pytest.approx(80.3) and abs(z) < 1


def test_smoothing_keeps_empty_bins_from_dominating_psi():
    base = {f"c{i}": 10 for i in range(70)}
    today = {f"c{i}": 1 for i in range(35)}           # half the categories absent today
    assert psi(base, today) > 2 * psi(base, today, smoothing=0.5)
    assert psi(base, base, smoothing=0.5) == pytest.approx(0)
