import os
import yaml
import logging
import pandas as pd
from sqlalchemy import text

from include.connections.postgres.client import PostgresClient


def _q(identifier: str) -> str:
    """Safely quote a PostgreSQL identifier."""
    if identifier is None or str(identifier).strip() == "":
        raise ValueError("SQL identifier cannot be empty.")
    return '"' + str(identifier).replace('"', '""') + '"'


class PostgresWriter:
    def __init__(self, yaml_path: str, conn_id: str = "dw_postgres"):
        self.yml_path = yaml_path
        self.conn_id = conn_id
        self.schema_def = self._load_yaml()
        self.table = self.schema_def["table"]
        self.schema = self._extract_schema()
        self.engine = PostgresClient(self.conn_id).sqlalchemy_engine()

    def _load_yaml(self):
        if not os.path.exists(self.yml_path):
            raise FileNotFoundError(f"YAML file not found: {self.yml_path}")
        with open(self.yml_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _extract_schema(self):
        schema = self.schema_def.get("schema")
        if not schema:
            raise ValueError("The YAML contract must contain a 'schema' key.")
        if isinstance(schema, str):
            return schema.strip()
        raise ValueError("The YAML 'schema' value must be a string.")

    def truncate(self, cascade: bool = False):
        cascade_sql = " CASCADE" if cascade else ""
        with self.engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {_q(self.schema)}.{_q(self.table)}{cascade_sql}"))
            logging.info("Truncated table %s.%s.", self.schema, self.table)

    def insert(self, df: pd.DataFrame, chunk_size: int = 1000):
        total_rows = len(df)
        if total_rows == 0:
            logging.info("The DataFrame is empty; nothing to insert.")
            return

        logging.info(
            "Inserting %s rows into %s.%s.", total_rows, self.schema, self.table
        )

        df.to_sql(
            name=self.table,
            con=self.engine,
            schema=self.schema,
            index=False,
            if_exists="append",
            method="multi",
            chunksize=chunk_size,
        )

        logging.info("Inserted %s rows into %s.%s.", total_rows, self.schema, self.table)

    def read(self) -> pd.DataFrame:
        query = f"SELECT * FROM {_q(self.schema)}.{_q(self.table)}"
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        logging.info("Read %s rows from %s.%s.", len(df), self.schema, self.table)
        return df


class PostgresLoader(PostgresWriter):
    def load(
        self,
        df: pd.DataFrame,
        load_mode: str = "full_refresh",
        merge_keys: list[str] | None = None,
        chunk_size: int = 1000,
    ):
        load_mode = (load_mode or "").lower().strip() if load_mode else "full_refresh"
        if load_mode != "full_refresh":
            raise ValueError(
                "Only load_mode='full_refresh' is supported. "
                f"Received: {load_mode}"
            )

        logging.info(f"[{self.schema}.{self.table}] FULL REFRESH (TRUNCATE + INSERT)")
        self.truncate()
        self.insert(df, chunk_size=chunk_size)


class PostgresAppender(PostgresWriter):
    def load(self, df: pd.DataFrame, chunk_size: int = 1000):
        total_rows = len(df)
        if total_rows == 0:
            logging.info("The DataFrame is empty; nothing to insert.")
            return

        logging.info(
            "[%s.%s] Appending %s rows.", self.schema, self.table, total_rows
        )
        self.insert(df, chunk_size=chunk_size)
        logging.info("[%s.%s] Append completed.", self.schema, self.table)
