from __future__ import annotations

from urllib.parse import quote_plus

try:
    from airflow.sdk.bases.hook import BaseHook
except Exception:  # pragma: no cover - compat Airflow 2.x
    from airflow.hooks.base import BaseHook

from sqlalchemy import create_engine
import psycopg2


class PostgresClient:
    """
    Build PostgreSQL connections for:

    - SQLAlchemy engine (postgresql+psycopg2)
    - optional direct psycopg2 connection

    Connection parameters come from an Airflow connection and its extras.

    Expected Airflow connection:
        conn_id: dw_postgres
        conn_type: postgres
        host: dw_postgres
        schema: dw_hr
        login: dw_user
        password: dw_password
        port: 5432

    Optional extras:
        {
          "sslmode": "require",
          "application_name": "airflow"
        }
    """

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self._engine = None

    def _airflow_conn(self):
        return BaseHook.get_connection(self.conn_id)

    def build_sqlalchemy_url(self) -> str:
        conn = self._airflow_conn()
        extra = conn.extra_dejson or {}

        user = quote_plus(conn.login or "")
        password = quote_plus(conn.password or "")
        host = conn.host or "localhost"
        port = conn.port or 5432
        database = conn.schema or extra.get("database") or "postgres"

        auth = f"{user}:{password}@" if user or password else ""
        url = f"postgresql+psycopg2://{auth}{host}:{port}/{database}"

        query_params = []
        for key in ("sslmode", "application_name", "connect_timeout", "options"):
            value = extra.get(key)
            if value is not None and str(value).strip() != "":
                query_params.append(f"{quote_plus(str(key))}={quote_plus(str(value))}")

        if query_params:
            url += "?" + "&".join(query_params)

        return url

    def sqlalchemy_engine(self, **engine_kwargs):
        if self._engine is None:
            kwargs = {
                "pool_pre_ping": True,
                "future": True,
            }
            kwargs.update(engine_kwargs or {})
            self._engine = create_engine(self.build_sqlalchemy_url(), **kwargs)
        return self._engine

    def psycopg2_connection(self):
        conn = self._airflow_conn()
        extra = conn.extra_dejson or {}

        params = {
            "host": conn.host or "localhost",
            "port": conn.port or 5432,
            "dbname": conn.schema or extra.get("database") or "postgres",
            "user": conn.login,
            "password": conn.password,
        }

        for key in ("sslmode", "application_name", "connect_timeout", "options"):
            value = extra.get(key)
            if value is not None and str(value).strip() != "":
                params[key] = value

        return psycopg2.connect(**params)
