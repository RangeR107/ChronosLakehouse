"""Deterministic surrogate keys (spec: Data model, SCD Type 2 rules).

key = the first 8 bytes of SHA-256 over the natural key parts, read as a signed BIGINT. Reruns
produce identical keys; Python's hash() would not (it is randomised per process). The parts are
joined with the ASCII unit separator, which can't occur in them, as event_id does.
"""
import hashlib

from pyspark.sql import Column
from pyspark.sql import functions as F

SEPARATOR = "\x1f"


def surrogate_key(*parts: Column) -> Column:
    # conv() with a negative target base reads the 16 hex digits as a signed 64-bit number, so
    # keys whose first bit is set become negative BIGINTs instead of overflowing to null.
    digest = F.sha2(F.concat_ws(SEPARATOR, *parts), 256)
    return F.conv(F.substring(digest, 1, 16), 16, -10).cast("bigint")


def surrogate_key_py(*parts: str) -> int:
    """The same key computed in plain Python, for tests and for checking by hand."""
    digest = hashlib.sha256(SEPARATOR.join(parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)
