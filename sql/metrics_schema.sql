-- DataSentinel's metrics store and the release manifest (spec: DataSentinel, "Tooling", and
-- Publishing). Lives in the `metrics` schema of the chronos database. Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS metrics;

-- One row per release. A release exists only once status = 'published': that single update is
-- the last write of publish_release (spec: Publishing, run lifecycle step 4).
-- namespace: the lakehouse namespace the row belongs to ('' for the main lakehouse; the
-- evaluation replay and a rebuild each have their own), so they never see each other's releases.
CREATE TABLE IF NOT EXISTS metrics.releases (
    namespace      text NOT NULL DEFAULT '',
    release_id     text NOT NULL,
    business_date  date NOT NULL,
    run_id         text NOT NULL,
    status         text NOT NULL CHECK (status IN ('publishing', 'published', 'blocked')),
    warnings       jsonb NOT NULL DEFAULT '[]',
    -- false when the day is published but not complete: stale events or a listed file missing
    complete       boolean NOT NULL DEFAULT true,
    watermark      timestamp,          -- bronze ingested_at this release processed up to
    snapshots      jsonb NOT NULL DEFAULT '{}',   -- table -> Iceberg snapshot id
    published_at   timestamp,          -- replay time
    PRIMARY KEY (namespace, release_id)
);
CREATE INDEX IF NOT EXISTS releases_by_date ON metrics.releases (namespace, business_date, status);

-- Every check's decision, with the method and baseline it used, so any alert can be explained.
CREATE TABLE IF NOT EXISTS metrics.check_results (
    namespace      text NOT NULL DEFAULT '',
    run_id         text NOT NULL,
    business_date  date NOT NULL,
    table_name     text NOT NULL,
    partition_key  text NOT NULL DEFAULT '',
    check_name     text NOT NULL,
    method         text NOT NULL,
    severity       text NOT NULL CHECK (severity IN ('block', 'quarantine', 'flag', 'warn', 'alert')),
    observed       double precision,
    expected       text,
    status         text NOT NULL CHECK (status IN ('pass', 'fail')),
    detail         jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (namespace, run_id, table_name, partition_key, check_name)
);

-- Per-column profiles (null rates, distinct counts, category shares) for drift and null checks.
CREATE TABLE IF NOT EXISTS metrics.profiles (
    namespace      text NOT NULL DEFAULT '',
    run_id         text NOT NULL,
    business_date  date NOT NULL,
    table_name     text NOT NULL,
    partition_key  text NOT NULL DEFAULT '',
    column_name    text NOT NULL,
    metric         text NOT NULL,
    value          double precision,
    PRIMARY KEY (namespace, run_id, table_name, partition_key, column_name, metric)
);

-- Batches a blocked run held (jobs/batches.py). One batch = the lines one run landed, named by
-- their ingested_at. 'held' until someone decides: 'released' (the next run takes it in, then
-- 'published') or 'rejected' (never processed).
CREATE TABLE IF NOT EXISTS metrics.batch_holds (
    namespace      text NOT NULL DEFAULT '',
    ingested_at    timestamp NOT NULL,
    run_id         text NOT NULL,          -- the run that held it
    business_date  date NOT NULL,
    state          text NOT NULL CHECK (state IN ('held', 'released', 'rejected', 'published')),
    reasons        jsonb NOT NULL DEFAULT '[]',   -- the blocking checks
    decided_at     timestamp,              -- wall clock: a person's decision, not replay time
    PRIMARY KEY (namespace, ingested_at)
);

-- The quarantined lines each run judged, for the console's quarantine browser, which reads
-- PostgreSQL only. Capped per run (sentinel/gate.py): a sample to look at, not a second copy of
-- silver.quarantine, which stays the record.
CREATE TABLE IF NOT EXISTS metrics.quarantine (
    namespace      text NOT NULL DEFAULT '',
    run_id         text NOT NULL,
    business_date  date NOT NULL,
    source_file    text NOT NULL,
    line_number    int NOT NULL,
    reasons        jsonb NOT NULL,
    raw            text,
    PRIMARY KEY (namespace, run_id, source_file, line_number)
);

-- Every alert and run notice (sentinel/alerts.py), whether or not it reached Slack. The console
-- reads it; `delivered` says what happened to the post (not_configured, slack 200, failed: ...).
CREATE TABLE IF NOT EXISTS metrics.notifications (
    id             bigserial PRIMARY KEY,
    namespace      text NOT NULL DEFAULT '',
    raised_at      timestamp NOT NULL DEFAULT now(),   -- wall clock: when a person was told
    kind           text NOT NULL CHECK (kind IN ('published', 'alert', 'blocked', 'task_failed')),
    business_date  date,
    run_id         text,
    summary        text NOT NULL,
    detail         jsonb NOT NULL DEFAULT '{}',
    delivered      text NOT NULL
);
