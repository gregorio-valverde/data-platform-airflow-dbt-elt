import pandas as pd
from sqlalchemy import text
from pathlib import Path

from include.connections.postgres.client import PostgresClient


class PostgresSelectReader:
    """
    Execute a PostgreSQL SELECT statement read from a SQL file.

    Example:
        reader = PostgresSelectReader(conn_id="dw_postgres")
        df = reader.read("/path/query.sql", vars={"schema": "raw"})
    """

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.engine = PostgresClient(conn_id).sqlalchemy_engine()

    def read(self, sql_path: str, vars: dict | None = None) -> pd.DataFrame:
        path = Path(sql_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file does not exist: {sql_path}")

        sql_raw = path.read_text(encoding="utf-8")
        sql = sql_raw.format(**vars) if vars else sql_raw

        sql_clean = sql.lstrip()
        sql_upper = sql_clean.upper()

        if sql_upper.startswith("WITH "):
            if "SELECT" not in sql_upper:
                raise ValueError("A CTE query must contain a SELECT statement.")
        elif not sql_upper.startswith("SELECT"):
            raise ValueError("This reader only allows SELECT or WITH + SELECT queries.")

        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn)
