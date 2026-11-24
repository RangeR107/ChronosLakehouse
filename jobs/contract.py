"""The event contract (contracts/events_v<N>.json) as Spark schemas and checks.

Silver never hard-codes a field: names, types and which fields are required all come from the
contract, so a new schema_version means a new contract file, not new parsing code.

Several versions are live at once (a late or resent event keeps the version it was sent in), so
each line is checked against its own version's contract. A field a later version renamed says
so with "renamed_from"; silver keeps the earlier name, so tables downstream never change.
"""
import json
from pathlib import Path

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"


def load_contracts() -> dict[int, dict]:
    contracts = {}
    for path in sorted(CONTRACTS_DIR.glob("events_v*.json")):
        c = json.loads(path.read_text())
        contracts[c["schema_version"]] = c
    return contracts


def payload_fields(contract: dict) -> dict[str, dict]:
    """Every payload field any event type uses, merged (a field means the same in every type)."""
    fields = {}
    for spec in contract["event_types"].values():
        fields.update(spec["payload"])
    return fields


def renames(contract: dict) -> dict[str, str]:
    """{new name: earlier name} for the top-level payload fields this version renamed."""
    return {name: spec["renamed_from"] for name, spec in payload_fields(contract).items()
            if "renamed_from" in spec}


def raw_schema(*contracts: dict) -> StructType:
    """How a line is parsed: the envelope typed as JSON delivers it, payload values as text.

    Payload values travel as Olist's CSV text, so they are read as strings here and cast field by
    field afterwards; a value that won't cast can then be named in the quarantine reason. With
    several contracts, the payload has every field any of them names.
    """
    fields = {}
    for contract in contracts:
        fields.update(payload_fields(contract))
    payload = []
    for name, spec in fields.items():
        if spec["type"] == "array":
            element = StructType([StructField(f, StringType()) for f in spec["element"]])
            payload.append(StructField(name, ArrayType(element)))
        else:
            payload.append(StructField(name, StringType()))
    return StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("logical_key", StringType()),
        StructField("version", IntegerType()),
        StructField("supersedes_event_id", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("event_time", StringType()),
        StructField("emitted_at", StringType()),
        StructField("payload", StructType(payload)),
    ])


def to_earliest_names(e: Column, contracts: dict[int, dict]) -> Column:
    """The parsed event with each renamed field copied back to its earlier name, by version."""
    for version, contract in contracts.items():
        for new, old in renames(contract).items():
            e = e.withField(f"payload.{old}", F.when(e["schema_version"] == version,
                                                     e["payload"][new])
                            .otherwise(e["payload"][old]))
    return e


def element_name(array: str):
    # One parameter only: F.transform passes the index as well to a two-parameter function.
    return lambda k: F.concat(F.lit(f"{array}[]."), k)


def unknown_fields(raw: Column, contracts: dict[int, dict]) -> Column:
    """The payload field names a line carries that its version's contract doesn't list.

    Top-level fields and the fields of array elements (items, payments) both count; an array
    element's field is named like items[].field. Empty for a line whose version is unknown: that
    line is quarantined anyway.
    """
    version = F.get_json_object(raw, "$.schema_version").cast("int")
    event_type = F.get_json_object(raw, "$.event_type")
    top = F.coalesce(F.json_object_keys(F.get_json_object(raw, "$.payload")),
                     F.array().cast("array<string>"))
    out = F.array().cast("array<string>")
    for v, contract in contracts.items():
        for t, spec in contract["event_types"].items():
            extra = F.array_except(top, F.array(*(F.lit(n) for n in spec["payload"])))
            for name, field in spec["payload"].items():
                if field["type"] != "array":
                    continue
                elements = F.from_json(F.get_json_object(raw, f"$.payload.{name}"),
                                       "array<map<string,string>>")
                keys = F.array_distinct(F.flatten(F.transform(
                    F.coalesce(elements, F.array().cast("array<map<string,string>>")),
                    F.map_keys)))
                extra = F.array_union(extra, F.transform(
                    F.array_except(keys, F.array(*(F.lit(n) for n in field["element"]))),
                    element_name(name)))
            out = F.when((version == v) & (event_type == t), extra).otherwise(out)
    return out


def cast_fails(value: Column, spark_type: str) -> Column:
    """True when a present value can't be read as its contract type."""
    return value.isNotNull() & value.cast(spark_type).isNull()


def element_check(name: str, spec: dict):
    """For F.exists: is this array element's field missing (when required) or unreadable?"""
    def bad(x: Column) -> Column:
        missing = x[name].isNull() if spec["required"] else F.lit(False)
        return missing | cast_fails(x[name], spec["type"])
    return bad


def problems(e: Column, contract: dict) -> Column:
    """An array of reasons this parsed event breaks the contract; empty when it is fine.

    Spec, DataSentinel checks: "Schema contract ... Quarantine events with an unknown version or a
    missing required field". A value that can't be read as its type is quarantined the same way.
    """
    types = list(contract["event_types"])
    checks = [
        (e["event_id"].isNull(), "missing:event_id"),
        (~e["event_type"].isin(types) | e["event_type"].isNull(), "unknown_event_type"),
        (e["version"].isNull() | (e["version"] < 1), "bad_value:version"),
        (e["emitted_at"].isNull(), "missing:emitted_at"),
        (cast_fails(e["emitted_at"], "timestamp_ntz"), "bad_value:emitted_at"),
        (cast_fails(e["event_time"], "timestamp_ntz"), "bad_value:event_time"),
    ]
    for event_type, spec in contract["event_types"].items():
        of_type = e["event_type"] == event_type
        if spec["event_time_field"] is not None:
            checks.append((of_type & e["event_time"].isNull()
                           & e["payload"][spec["event_time_field"]].isNotNull(),
                           "missing:event_time"))
        for name, field in spec["payload"].items():
            value = e["payload"][name]
            if field["type"] == "array":
                checks.append((of_type & value.isNull(), f"missing:{name}"))
                for sub, sub_spec in field["element"].items():
                    checks.append((of_type & F.exists(value, element_check(sub, sub_spec)),
                                   f"bad_value:{name}.{sub}"))
                continue
            if field["required"]:
                checks.append((of_type & value.isNull(), f"missing:{name}"))
            if field["type"] != "string":
                checks.append((of_type & cast_fails(value, field["type"]), f"bad_value:{name}"))
    return F.array_compact(F.array(*(F.when(cond, F.lit(reason)) for cond, reason in checks)))
