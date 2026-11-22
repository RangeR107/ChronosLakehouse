"""Landing to bronze: wait_for_landing, register_files and land_to_bronze (spec: chronos_daily
steps 1, 2 and 5, and Architecture, "Bronze").

Bronze keeps every event exactly as received, append-only, with where it came from (source_file,
line_number) and when it arrived (ingested_at). A file manifest records every file loaded, so a
retry never loads a file twice, while a genuinely redelivered event in a new file is kept here
and deduplicated in silver.
"""
import hashlib
import json
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from jobs.context import Context, run_id, run_time

# The producer's delivery contract (spec: "Completion marker"). The pipeline may read it; it
# says what was sent, never what is wrong. The ground-truth manifest is a different thing.
MARKER = "_complete.json"

LINES_SCHEMA = StructType([
    StructField("source_file", StringType(), False),
    StructField("line_number", IntegerType(), False),
    StructField("raw", StringType(), False),
])


class LandingNotReady(Exception):
    """The day's completion marker isn't there yet: the producer hasn't finished the day."""


class ChecksumMismatch(Exception):
    """A file's bytes differ from what the marker says was sent."""


def ensure_bronze_tables(ctx: Context) -> None:
    ctx.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ctx.namespace('bronze')}")
    ctx.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ctx.table('bronze', 'events')} (
            source_file   STRING    COMMENT 'path under landing/, e.g. date=2017-01-05/events_2017-01-05T10.jsonl',
            line_number   INT       COMMENT '1-based line in source_file',
            raw           STRING    COMMENT 'the line exactly as received',
            event_id      STRING    COMMENT 'read from raw for auditing; null if raw is not valid JSON',
            event_type    STRING,
            business_date DATE      COMMENT 'the landing folder the file came from',
            ingested_at   TIMESTAMP_NTZ COMMENT 'replay time of the run that loaded it'
        ) USING iceberg
        PARTITIONED BY (business_date)
        TBLPROPERTIES ('format-version' = '2')
    """)
    ctx.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ctx.table('bronze', 'file_manifest')} (
            file_path     STRING,
            etag          STRING    COMMENT 'SHA-256 of the file locally; the S3 ETag in Phase 4',
            size_bytes    BIGINT,
            row_count     INT,
            business_date DATE,
            run_id        STRING,
            loaded_at     TIMESTAMP_NTZ COMMENT 'replay time of the run that loaded it'
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)


def wait_for_landing(ctx: Context, business_date: date) -> dict:
    """Return the day's completion marker, or raise if the producer hasn't finished the day.

    In Airflow this becomes a sensor that retries until the marker appears.
    """
    path = ctx.landing_dir / f"date={business_date}" / MARKER
    if not path.exists():
        raise LandingNotReady(f"no completion marker at {path}")
    return json.loads(path.read_text())


def loaded_files(ctx: Context, business_date: date) -> set[str]:
    rows = ctx.spark.sql(f"""
        SELECT file_path FROM {ctx.table('bronze', 'file_manifest')}
        WHERE business_date = DATE'{business_date}'""").collect()
    return {r.file_path for r in rows}


def register_files(ctx: Context, business_date: date) -> dict:
    """Compare the landing folder with the marker and the manifest.

    Returns the files to load ("new": listed, present and never loaded) plus what the
    completeness check needs: files the marker lists that aren't there ("missing") and files
    that are there but the marker doesn't list ("unlisted"). Unlisted files are never loaded.
    """
    ensure_bronze_tables(ctx)
    marker = wait_for_landing(ctx, business_date)
    folder = ctx.landing_dir / f"date={business_date}"
    listed = {f["file"]: f for f in marker["files"]}
    on_disk = {p.name for p in folder.glob("*.jsonl")}
    done = loaded_files(ctx, business_date)

    new = [{"path": f"date={business_date}/{name}", "rows": listed[name]["rows"],
            "sha256": listed[name]["sha256"]}
           for name in sorted(listed.keys() & on_disk)
           if f"date={business_date}/{name}" not in done]
    return {"business_date": business_date.isoformat(), "new": new,
            "missing": sorted(listed.keys() - on_disk), "unlisted": sorted(on_disk - listed.keys())}


def land_to_bronze(ctx: Context, business_date: date, files: list[dict]) -> int:
    """Append the given files' lines to bronze.events, then record them in the manifest.

    Two tables mean two commits. Events go first: if the run dies between the commits, the
    retry sees the file's lines already in bronze, skips them, and only writes the missing
    manifest row. The other order could lose a file for good. Returns the lines appended.
    """
    ensure_bronze_tables(ctx)
    events = ctx.table("bronze", "events")
    paths = [f["path"] for f in files]
    already = set()
    if paths:
        already = {r.source_file for r in ctx.spark.table(events)
                   .where(F.col("business_date") == F.lit(business_date))
                   .where(F.col("source_file").isin(paths))
                   .select("source_file").distinct().collect()}

    lines, manifest = [], []
    for f in files:
        content = (ctx.landing_dir / f["path"]).read_bytes()
        etag = hashlib.sha256(content).hexdigest()
        if etag != f["sha256"]:
            # Phase 3 turns this into a Block result of the completeness check.
            raise ChecksumMismatch(f"{f['path']}: marker says {f['sha256']}, file is {etag}")
        text = content.decode("utf-8").splitlines()
        if f["path"] not in already:
            lines.extend((f["path"], n, line) for n, line in enumerate(text, start=1))
        manifest.append((f["path"], etag, len(content), len(text)))

    # Built from text as TIMESTAMP_NTZ: replay times, like Olist's, carry no time zone, and a
    # Python datetime literal would be shifted through the laptop's zone on the way in.
    ingested = F.lit(run_time(business_date).isoformat(sep=" ")).cast("timestamp_ntz")
    if lines:
        # One append for the whole day: Iceberg commits it atomically, all lines or none.
        (ctx.spark.createDataFrame(lines, LINES_SCHEMA)
         .withColumn("event_id", F.get_json_object("raw", "$.event_id"))
         .withColumn("event_type", F.get_json_object("raw", "$.event_type"))
         .withColumn("business_date", F.lit(business_date))
         .withColumn("ingested_at", ingested)
         .writeTo(events).append())
    if manifest:
        (ctx.spark.createDataFrame(manifest, "file_path string, etag string, size_bytes long,"
                                             " row_count int")
         .withColumn("business_date", F.lit(business_date))
         .withColumn("run_id", F.lit(run_id(business_date)))
         .withColumn("loaded_at", ingested)
         .writeTo(ctx.table("bronze", "file_manifest")).append())
    return len(lines)
