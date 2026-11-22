"""Event identity: event_id must be the same in every process, forever (spec: Event identity)."""
import os
import subprocess
import sys

import pytest

from generator.events import event_id, make_event

# Expected values computed outside Python, with:
#   printf 'order_placed\x1fe481f51cbdc54678b7cc49136f2d6af7\x1f1' | shasum -a 256
ORDER_ID = "e481f51cbdc54678b7cc49136f2d6af7"
ORDER_PLACED_V1 = "c993c495d8b6eddcf2a3eeb9833fd9cfeee913173d7e72ca01bef8bcb659b966"
REVIEW_KEY = ["73fc7af87114b39712e6da79b0a377eb", "7bc2406110b926393aa56f80a40eba40"]
REVIEW_V1 = "3de6227aaf8b1edeb49e818f6274b160173fc0e53edfeac0275c424aacbba3d1"


def test_event_id_matches_sha256_computed_outside_python():
    assert event_id("order_placed", [ORDER_ID], 1) == ORDER_PLACED_V1
    assert event_id("review_submitted", REVIEW_KEY, 1) == REVIEW_V1


def test_event_id_is_stable_across_processes():
    # Python randomises str hashes per process (PYTHONHASHSEED); event_id must not depend on it.
    code = f"from generator.events import event_id; print(event_id('order_placed', ['{ORDER_ID}'], 1))"
    ids = set()
    for seed in ("1", "2"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                             text=True, check=True)
        ids.add(out.stdout.strip())
    assert ids == {ORDER_PLACED_V1}


def test_version_and_type_change_the_id_but_a_resend_does_not():
    v1 = event_id("delivered", [ORDER_ID], 1)
    assert event_id("delivered", [ORDER_ID], 1) == v1  # a duplicate keeps its id
    assert event_id("delivered", [ORDER_ID], 2) != v1  # a correction gets a new id
    assert event_id("handed_to_carrier", [ORDER_ID], 1) != v1


def test_two_reviews_on_one_order_get_different_ids():
    a = event_id("review_submitted", [ORDER_ID, "review_a"], 1)
    b = event_id("review_submitted", [ORDER_ID, "review_b"], 1)
    assert a != b


def test_make_event_builds_the_envelope():
    e = make_event("order_cancelled", [ORDER_ID], None, "2017-10-11 21:25:13", {"order_id": ORDER_ID})
    assert list(e) == ["event_id", "event_type", "logical_key", "version", "supersedes_event_id",
                       "schema_version", "event_time", "emitted_at", "payload"]
    assert e["event_time"] is None and e["version"] == 1 and e["schema_version"] == 1
    assert e["logical_key"] == ORDER_ID


def test_make_event_rejects_unknown_types():
    with pytest.raises(ValueError):
        make_event("order_returned", [ORDER_ID], None, "2017-10-11 00:00:00", {})
