from __future__ import annotations

from datetime import datetime
from typing import Callable

import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import TaskGroup

from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.bash import BashOperator

from include.connections.postgres.endpoints.sync import PostgresSync
from include.connections.postgres.endpoints.writer import PostgresLoader

from include.helpers.parquets.df_parquet_save import df_parquet_save
from include.helpers.parquets.df_parquet_read_delete import df_parquet_read_delete
from include.helpers.transformations.read_columns_yaml_list import read_columns_yaml_list
from include.helpers.transformations.df_types_from_yaml import df_types_from_yaml


def _call_extractor(
    extractor: Callable,
    elt_vars: dict,
    context: dict,
) -> pd.DataFrame:
    """
    Call an extractor while supporting multiple function signatures.

    Supported signatures:
      - extractor(**elt_vars, **context)
      - extractor(elt_vars=elt_vars, **context)
      - extractor(etl_vars=elt_vars, **context)
      - extractor(**context)
    """
    try:
        return extractor(**elt_vars, **context)
    except TypeError:
        try:
            return extractor(elt_vars=elt_vars, **context)
        except TypeError:
            try:
                return extractor(etl_vars=elt_vars, **context)
            except TypeError:
                return extractor(**context)


def _apply_yaml_contract(df: pd.DataFrame, yaml_path: str) -> pd.DataFrame:
    """
    Apply the contract defined in YAML:

      - retain only columns declared in YAML
      - preserve the YAML column order
      - cast values to the declared YAML types

    Business cleaning and normalization belong in the downstream dbt models.
    """
    valid_columns = read_columns_yaml_list(yaml_path)
    selected_columns = [column for column in valid_columns if column in df.columns]

    if not selected_columns:
        raise ValueError(
            "No columns remained after applying the YAML contract. "
            f"yaml_path={yaml_path}. "
            f"yaml_columns={valid_columns}. "
            f"df_columns={list(df.columns)}"
        )

    df = df[selected_columns].copy()
    df = df_types_from_yaml(df, yaml_path)

    return df


def _build_dbt_command(
    dbt_project_dir: str,
    dbt_profiles_dir: str,
    dbt_target: str,
    dbt_tag: str | None,
    vars_task_id: str = "start_elt",
) -> str:
    command = (
        f"cd {dbt_project_dir} && "
        "dbt run "
        f"--project-dir {dbt_project_dir} "
        f"--profiles-dir {dbt_profiles_dir} "
        f"--target {dbt_target} "
    )

    if dbt_tag:
        command += f"--select tag:{dbt_tag} "

    command += f"--vars '{{{{ ti.xcom_pull(task_ids=\"{vars_task_id}\") | tojson }}}}'"

    return command


class DagEltGenerator:
    """
    Generate Airflow ELT DAGs backed by PostgreSQL and dbt.

    Per-pipeline flow:

      1. gate_source
         Decide whether the pipeline should run.

      2. sync_target
         Create or synchronize the PostgreSQL raw table from YAML.

      3. extract_source
         Run the extractor and return a DataFrame.

      4. load_raw
         Load the DataFrame into PostgreSQL.

    Optional final step:

      5. transform_dbt
         Run dbt when dbt_tag is not None.

    Example pipeline:

    pipelines = [
        {
            "pipeline_id": "employees",
            "yaml_path": "include/elt/config/src/src_hr_employees.yml",
            "extractor": extract_src_hr_employees,
            "conn_id": "dw_postgres",
            "load_mode_default": "full_refresh",
            "enabled": True,
            "sync_enabled": True,
        }
    ]
    """

    def __init__(
        self,
        dag_id: str,
        pipelines: list[dict],
        dbt_tag: str | None = None,
        schedule: str | None = None,
        start_date: datetime = datetime(2025, 1, 1),
        default_args: dict | None = None,
        tags: list[str] | None = None,
        dbt_vars_callable: Callable[..., dict] | None = None,
        default_conn_id: str = "dw_postgres",
        dbt_project_dir: str = "/usr/local/airflow/dbt",
        dbt_profiles_dir: str = "/usr/local/airflow/dbt",
        dbt_target: str = "dev",
        max_active_tasks: int | None = None,
        max_active_runs: int = 1,
        pool: str | None = None,
        chunk_size: int = 1000,
    ):
        self.dag_id = dag_id
        self.pipelines = pipelines
        self.dbt_tag = dbt_tag
        self.schedule = schedule

        self.start_date = start_date
        self.default_args = default_args or {"owner": "data"}
        self.tags = tags or []

        self.dbt_vars_callable = dbt_vars_callable
        self.default_conn_id = default_conn_id

        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.dbt_target = dbt_target

        self.max_active_tasks = max_active_tasks
        self.max_active_runs = max_active_runs
        self.pool = pool
        self.chunk_size = chunk_size

    def build(self) -> DAG:
        dag_schedule = (
            CronTriggerTimetable(self.schedule, timezone="Europe/Madrid")
            if self.schedule
            else None
        )

        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            start_date=self.start_date,
            schedule=dag_schedule,
            catchup=False,
            tags=self.tags,
            max_active_tasks=self.max_active_tasks,
            max_active_runs=self.max_active_runs,
        ) as dag:

            def _start_elt(**context):
                if not self.dbt_vars_callable:
                    return {}

                vars_dict = self.dbt_vars_callable(**context)

                if vars_dict is None:
                    return {}

                if not isinstance(vars_dict, dict):
                    raise ValueError("dbt_vars_callable must return a dictionary")

                return vars_dict

            start_elt = PythonOperator(
                task_id="start_elt",
                python_callable=_start_elt,
                email_on_failure=False,
                pool=self.pool,
            )

            raw_load_tasks = []

            for index, pipeline in enumerate(self.pipelines, start=1):
                pipeline_id = str(pipeline.get("pipeline_id", index))
                yaml_path = pipeline["yaml_path"]
                extractor = pipeline["extractor"]

                enabled = bool(pipeline.get("enabled", True))
                sync_enabled = bool(pipeline.get("sync_enabled", True))

                conn_id = pipeline.get("conn_id", self.default_conn_id)
                sync_conn_id = pipeline.get("sync_conn_id", conn_id)
                load_conn_id = pipeline.get("load_conn_id", conn_id)

                load_mode_default = pipeline.get("load_mode_default", "full_refresh")
                chunk_size = int(pipeline.get("chunk_size", self.chunk_size))

                group_id = f"elt_raw_{pipeline_id}"

                with TaskGroup(
                    group_id=group_id,
                    tooltip=f"ELT raw load: {pipeline_id}",
                ) as task_group:

                    def _make_gate(
                        pipeline_id=pipeline_id,
                        enabled=enabled,
                    ):
                        def _gate(**context):
                            if not enabled:
                                return False

                            conf = context.get("dag_run").conf or {}
                            include = conf.get("include_pipelines")
                            exclude = conf.get("exclude_pipelines")

                            def _to_str_set(value):
                                if value is None:
                                    return None

                                if not isinstance(value, (list, tuple, set)):
                                    raise ValueError(
                                        "include_pipelines/exclude_pipelines must be a list"
                                    )

                                return {str(item) for item in value}

                            include_set = _to_str_set(include)
                            exclude_set = _to_str_set(exclude)

                            pid = str(pipeline_id)

                            if include_set is not None:
                                return pid in include_set

                            if exclude_set is not None:
                                return pid not in exclude_set

                            return True

                        return _gate

                    gate_source = ShortCircuitOperator(
                        task_id="gate_source",
                        python_callable=_make_gate(),
                        email_on_failure=False,
                        pool=self.pool,
                    )

                    def _make_sync(
                        yaml_path=yaml_path,
                        sync_conn_id=sync_conn_id,
                    ):
                        def _sync(**context):
                            PostgresSync(
                                yaml_path=yaml_path,
                                conn_id=sync_conn_id,
                            ).sync()

                        return _sync

                    sync_target = None
                    if sync_enabled:
                        sync_target = PythonOperator(
                            task_id="sync_target",
                            python_callable=_make_sync(),
                            email_on_failure=False,
                            pool=self.pool,
                        )

                    def _make_extract(
                        extractor=extractor,
                        yaml_path=yaml_path,
                        start_task_id="start_elt",
                    ):
                        def _extract(**context):
                            elt_vars = context["ti"].xcom_pull(
                                task_ids=start_task_id,
                            ) or {}

                            df = _call_extractor(
                                extractor=extractor,
                                elt_vars=elt_vars,
                                context=context,
                            )

                            if df is None or df.empty:
                                raise AirflowSkipException("No data available to load")

                            df = _apply_yaml_contract(
                                df=df,
                                yaml_path=yaml_path,
                            )

                            if df.empty:
                                raise AirflowSkipException(
                                    "The DataFrame is empty after applying the YAML contract"
                                )

                            return df_parquet_save(df)

                        return _extract

                    extract_source = PythonOperator(
                        task_id="extract_source",
                        python_callable=_make_extract(),
                        email_on_failure=False,
                        pool=self.pool,
                    )

                    def _make_load(
                        yaml_path=yaml_path,
                        load_conn_id=load_conn_id,
                        load_mode_default=load_mode_default,
                        upstream_task_id=f"{group_id}.extract_source",
                        chunk_size=chunk_size,
                    ):
                        def _load(**context):
                            dag_run = context.get("dag_run")
                            mode = (dag_run.conf or {}).get("mode", load_mode_default)

                            parquet_path = context["ti"].xcom_pull(
                                task_ids=upstream_task_id,
                            )

                            df = df_parquet_read_delete(parquet_path)

                            if df is None or df.empty:
                                raise AirflowSkipException("No data available to load")

                            loader = PostgresLoader(
                                yaml_path=yaml_path,
                                conn_id=load_conn_id,
                            )

                            loader.load(
                                df=df,
                                load_mode=mode,
                                chunk_size=chunk_size,
                            )

                            return {
                                "rows_loaded": int(len(df)),
                                "load_mode": str(mode),
                                "conn_id": load_conn_id,
                            }

                        return _load

                    load_raw = PythonOperator(
                        task_id="load_raw",
                        python_callable=_make_load(),
                        email_on_failure=False,
                        pool=self.pool,
                    )

                    if sync_enabled:
                        gate_source >> sync_target >> extract_source >> load_raw
                    else:
                        gate_source >> extract_source >> load_raw

                    start_elt >> gate_source

                raw_load_tasks.append(task_group)

            if self.dbt_tag:
                dbt_command = _build_dbt_command(
                    dbt_project_dir=self.dbt_project_dir,
                    dbt_profiles_dir=self.dbt_profiles_dir,
                    dbt_target=self.dbt_target,
                    dbt_tag=self.dbt_tag,
                    vars_task_id="start_elt",
                )

                transform_dbt = BashOperator(
                    task_id="transform_dbt",
                    bash_command=dbt_command,
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                    email_on_failure=False,
                    pool=self.pool,
                )

                for task_group in raw_load_tasks:
                    task_group >> transform_dbt

        return dag


dag_elt_generator = DagEltGenerator
