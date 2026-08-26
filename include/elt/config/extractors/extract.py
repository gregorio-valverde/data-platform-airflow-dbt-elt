from pathlib import Path

import pandas as pd


def _get_data_dir() -> Path:
    """
    Locate include/data from the current extract.py location.

    This keeps the extractor independent from absolute paths.
    """
    current_path = Path(__file__).resolve()

    for parent in current_path.parents:
        candidate = parent / "include" / "data"
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Could not locate include/data from the extract.py location"
    )


def _read_csv(filename: str) -> pd.DataFrame:
    """
    Read a CSV file from include/data and return it unchanged as a DataFrame.
    """
    data_dir = _get_data_dir()
    file_path = data_dir / filename

    if not file_path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")

    return pd.read_csv(file_path)


def extract_src_hr_employees(dag=None, etl_vars=None, **kwargs) -> pd.DataFrame:
    """
    Extract the raw employee source for raw.src_hr_employees.
    """
    return _read_csv("employees.csv")


def extract_src_hr_departments(dag=None, etl_vars=None, **kwargs) -> pd.DataFrame:
    """
    Extract the raw department source for raw.src_hr_departments.
    """
    return _read_csv("departments.csv")


def extract_src_hr_consulting_firms(dag=None, etl_vars=None, **kwargs) -> pd.DataFrame:
    """
    Extract the raw consulting-firm source for raw.src_hr_consulting_firms.
    """
    return _read_csv("consulting_firms.csv")


def extract_src_hr_performance_reviews(dag=None, etl_vars=None, **kwargs) -> pd.DataFrame:
    """
    Extract the raw performance-review source for raw.src_hr_performance_reviews.
    """
    return _read_csv("performance_reviews.csv")


def extract_src_hr_payroll(dag=None, etl_vars=None, **kwargs) -> pd.DataFrame:
    """
    Extract the raw 2025 payroll source for raw.src_hr_payroll.
    """
    return _read_csv("payroll.csv")
