import os
import yaml
import logging

from include.connections.postgres.client import PostgresClient


def _q(identifier: str) -> str:
    if identifier is None or str(identifier).strip() == "":
        raise ValueError("SQL identifier cannot be empty.")
    return '"' + str(identifier).replace('"', '""') + '"'


class PostgresSync:
    """
    Synchronize a PostgreSQL table from a YAML contract when sync_enabled is true.

    Expected YAML structure:
        schema: raw
        table: src_hr_employees
        columns:
          - name: employee_id
            type: BIGINT
            nullable: false
            primary_key: true
        indexes:
          - columns: employee_id
            unique: false
    """

    def __init__(
        self,
        yaml_path: str,
        conn_id: str = "dw_postgres",
        sync_enabled: bool = True,
    ):
        self.yml_path = yaml_path
        self.conn_id = conn_id
        self.sync_enabled = sync_enabled
        self.schema_def = self._load_yaml()
        self.schema = self.schema_def["schema"]
        self.table = self.schema_def["table"]
        self.conn = None
        self.cursor = None

    def _load_yaml(self):
        if not os.path.exists(self.yml_path):
            raise FileNotFoundError(f"YAML file not found: {self.yml_path}")
        with open(self.yml_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _connect(self):
        self.conn = PostgresClient(self.conn_id).psycopg2_connection()
        self.cursor = self.conn.cursor()

    def _close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _ensure_schema(self):
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(self.schema)};")

    def _table_exists(self):
        self.cursor.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s;
            """,
            (self.schema, self.table),
        )
        return self.cursor.fetchone() is not None

    def _get_existing_columns(self):
        self.cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s;
            """,
            (self.schema, self.table),
        )
        return [row[0] for row in self.cursor.fetchall()]

    def _create_table(self):
        columns = self.schema_def.get("columns", [])
        if not columns:
            raise ValueError("The YAML contract does not define any columns.")

        column_defs = []
        pk_columns = []

        for col in columns:
            col_name = col["name"]
            col_type = col["type"]
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"

            col_name_sql = _q(col_name)
            col_def = f"{col_name_sql} {col_type} {nullable}"
            column_defs.append(col_def)

            if col.get("primary_key", False):
                pk_columns.append(col_name_sql)

        pk_clause = f", PRIMARY KEY ({', '.join(pk_columns)})" if pk_columns else ""
        column_defs_sql = ",\n                ".join(column_defs)

        create_sql = f"""
            CREATE TABLE {_q(self.schema)}.{_q(self.table)} (
                {column_defs_sql}
                {pk_clause}
            );
        """

        logging.info("Generated CREATE TABLE statement:")
        logging.info(create_sql)
        self.cursor.execute(create_sql)

        self._create_indexes()

    def _create_indexes(self):
        for idx in self.schema_def.get("indexes", []) or []:
            cols = idx["columns"]
            if isinstance(cols, list):
                cols_sql = ", ".join([_q(c) for c in cols])
                cols_for_name = cols
            else:
                cols_sql = _q(cols)
                cols_for_name = [cols]

            unique = "UNIQUE" if idx.get("unique", False) else ""
            index_name = idx.get("name") or f"ix_{self.table}_{'_'.join(cols_for_name)}"
            sql = (
                f"CREATE {unique} INDEX IF NOT EXISTS {_q(index_name)} "
                f"ON {_q(self.schema)}.{_q(self.table)} ({cols_sql});"
            )
            logging.info("Creating index: %s", sql)
            self.cursor.execute(sql)

    def _alter_table(self):
        existing_cols = self._get_existing_columns()
        added = False

        for col in self.schema_def["columns"]:
            if col["name"] not in existing_cols:
                nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
                sql = (
                    f"ALTER TABLE {_q(self.schema)}.{_q(self.table)} "
                    f"ADD COLUMN {_q(col['name'])} {col['type']} {nullable};"
                )
                logging.info(
                    "Adding column %s to %s.%s", col["name"], self.schema, self.table
                )
                self.cursor.execute(sql)
                added = True

        if not added:
            logging.info(
                "Table %s.%s already contains every declared column.",
                self.schema,
                self.table,
            )

        self._create_indexes()

    def sync(self):
        if not self.sync_enabled:
            logging.info(
                "Synchronization disabled for %s.%s; no changes were made.",
                self.schema,
                self.table,
            )
            return

        try:
            self._connect()
            self._ensure_schema()
            if self._table_exists():
                self._alter_table()
            else:
                self._create_table()
            self.conn.commit()
        except Exception as e:
            logging.error("Could not synchronize %s.%s: %s", self.schema, self.table, e)
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self._close()
