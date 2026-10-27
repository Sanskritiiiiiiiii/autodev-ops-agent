import os

import psycopg2


def get_connection():
    """Open a PostgreSQL connection (defaults match docker-compose.yml)."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5433")),
        user=os.environ.get("POSTGRES_USER", "autodev"),
        password=os.environ.get("POSTGRES_PASSWORD", "autodev_pw"),
        dbname=os.environ.get("POSTGRES_DB", "autodev"),
        connect_timeout=5,
    )