from __future__ import annotations
from include.elt.config.extractors.extract import (
    extract_src_hr_consulting_firms,
    extract_src_hr_departments,
    extract_src_hr_employees,
    extract_src_hr_payroll,
    extract_src_hr_performance_reviews,
)

pipelines = [
    {
        "pipeline_id": "payroll",
        "yaml_path": "/usr/local/airflow/include/elt/config/src/src_hr_payroll.yml",
        "extractor": extract_src_hr_payroll,
        "sync_enabled": True,
        "load_mode_default": "full_refresh",
        "enabled": True,
    },
    {
        "pipeline_id": "consulting_firms",
        "yaml_path": "/usr/local/airflow/include/elt/config/src/src_hr_consulting_firms.yml",
        "extractor": extract_src_hr_consulting_firms,
        "sync_enabled": True,
        "load_mode_default": "full_refresh",
        "enabled": True,
    },
    {
        "pipeline_id": "departments",
        "yaml_path": "/usr/local/airflow/include/elt/config/src/src_hr_departments.yml",
        "extractor": extract_src_hr_departments,
        "sync_enabled": True,
        "load_mode_default": "full_refresh",
        "enabled": True,
    },
    {
        "pipeline_id": "performance_reviews",
        "yaml_path": "/usr/local/airflow/include/elt/config/src/src_hr_performance_reviews.yml",
        "extractor": extract_src_hr_performance_reviews,
        "sync_enabled": True,
        "load_mode_default": "full_refresh",
        "enabled": True,
    },
    {
        "pipeline_id": "employees",
        "yaml_path": "/usr/local/airflow/include/elt/config/src/src_hr_employees.yml",
        "extractor": extract_src_hr_employees,
        "sync_enabled": True,
        "load_mode_default": "full_refresh",
        "enabled": True,
    },
]
