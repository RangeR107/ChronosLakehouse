"""Connection settings for the Chronos PostgreSQL, from environment variables.

The defaults are the local Docker database. The generator has its own copy of these few lines on
purpose: the pipeline must not depend on the generator's code.
"""
import os

from psycopg.conninfo import make_conninfo


def conninfo(dbname: str | None = None) -> str:
    return make_conninfo(
        host=os.environ.get("CHRONOS_DB_HOST", "localhost"),
        port=os.environ.get("CHRONOS_DB_PORT", "5433"),
        user=os.environ.get("CHRONOS_DB_USER", "chronos"),
        password=os.environ.get("CHRONOS_DB_PASSWORD", "chronos"),
        dbname=dbname or os.environ.get("CHRONOS_DB_NAME", "chronos"),
    )
