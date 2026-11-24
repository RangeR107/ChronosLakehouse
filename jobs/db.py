"""Connection settings for the Chronos PostgreSQL, from environment variables.

The defaults are the local Docker database. The generator has its own copy of these few lines on
purpose: the pipeline must not depend on the generator's code.
"""
import os
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo

METRICS_SQL = Path(__file__).parent.parent / "sql" / "metrics_schema.sql"


def conninfo(dbname: str | None = None) -> str:
    return make_conninfo(
        host=os.environ.get("CHRONOS_DB_HOST", "localhost"),
        port=os.environ.get("CHRONOS_DB_PORT", "5433"),
        user=os.environ.get("CHRONOS_DB_USER", "chronos"),
        password=os.environ.get("CHRONOS_DB_PASSWORD", "chronos"),
        dbname=dbname or os.environ.get("CHRONOS_DB_NAME", "chronos"),
    )


def ensure_metrics_schema(conn: psycopg.Connection) -> None:
    """DataSentinel's metrics store and the release manifest (sql/metrics_schema.sql)."""
    conn.execute(METRICS_SQL.read_text())
    conn.commit()
