from __future__ import annotations
from datetime import datetime
from include.elt.factory.dag_elt_generator import DagEltGenerator
from include.elt.config.pipelines.pipelines_hr import pipelines


elt_dag = DagEltGenerator(
    dag_id="elt_hr",
    pipelines=pipelines,
    dbt_tag="hr",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    default_args={
        "owner": "gregorio.valverde",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    tags=["elt", "hr", "postgres", "raw"],
    default_conn_id="dw_postgres",
    dbt_project_dir="/usr/local/airflow/dbt",
    dbt_profiles_dir="/usr/local/airflow/dbt",
    dbt_target="dev",
    max_active_runs=1,
    max_active_tasks=3,
).build()
