"""Event identity and the event envelope (spec: Event model, "Event identity and versions")."""
import hashlib

SCHEMA_VERSION = 1

# The ASCII unit separator can't occur in event types, Olist ids or version numbers, so joining
# with it can never make two different inputs produce the same string.
SEPARATOR = "\x1f"

# Fixed order for events that share an emission time, so every replay writes identical files.
EVENT_TYPES = (
    "order_placed",
    "payment_approved",
    "handed_to_carrier",
    "delivered",
    "order_cancelled",
    "order_unavailable",
    "review_submitted",
)


def event_id(event_type: str, key_parts: list[str], version: int) -> str:
    """Hex SHA-256 of event type, logical key parts and version.

    Python's built-in hash() is randomised per process for strings, so it would give a different
    id on every run; SHA-256 gives the same id everywhere, forever.
    """
    text = SEPARATOR.join([event_type, *key_parts, str(version)])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def make_event(event_type: str, key_parts: list[str], event_time: str | None,
               emitted_at: str, payload: dict, version: int = 1,
               supersedes_event_id: str | None = None) -> dict:
    """Build one event. Key order is fixed, so serialised events are byte-for-byte stable."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event type: {event_type}")
    return {
        "event_id": event_id(event_type, key_parts, version),
        "event_type": event_type,
        "logical_key": ":".join(key_parts),
        "version": version,
        "supersedes_event_id": supersedes_event_id,
        "schema_version": SCHEMA_VERSION,
        "event_time": event_time,
        "emitted_at": emitted_at,
        "payload": payload,
    }
