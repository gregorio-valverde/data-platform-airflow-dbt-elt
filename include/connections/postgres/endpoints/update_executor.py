import logging
from pathlib import Path

from sqlalchemy import text

from include.connections.postgres.client import PostgresClient


class PostgresUpdateExecutor:
    """
    Execute a PostgreSQL UPDATE, DELETE, INSERT, TRUNCATE, CREATE, ALTER, DROP,
    or WITH statement read from a SQL file.
    """

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.engine = PostgresClient(conn_id).sqlalchemy_engine()

    def run(self, sql_path: str, params: dict | None = None):
        if params is None:
            params = {}

        path = Path(sql_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file does not exist: {sql_path}")

        sql_raw = path.read_text(encoding="utf-8")
        sql_clean = sql_raw.lstrip()
        if not sql_clean:
            raise ValueError("The SQL file is empty.")

        first_word = sql_clean.split()[0].upper()
        allowed = ("UPDATE", "DELETE", "INSERT", "TRUNCATE", "CREATE", "ALTER", "DROP", "WITH")
        if first_word not in allowed:
            raise ValueError(f"This executor only allows {allowed} statements.")

        with self.engine.begin() as conn:
            result = conn.execute(text(sql_raw), params)
            logging.info("Query completed successfully. Affected rows: %s", result.rowcount)
            return result.rowcount
