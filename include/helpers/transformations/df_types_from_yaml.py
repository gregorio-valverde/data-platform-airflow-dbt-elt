import logging


import pandas as pd
import yaml


def df_types_from_yaml(df, path_yaml):
    with open(path_yaml, "r", encoding="utf-8") as f:
        spec_yaml = yaml.safe_load(f)

    for column in spec_yaml["columns"]:
        column_name = column["name"]
        column_type = column["type"].upper()
        if column_name not in df.columns:
            continue

        if column_type.startswith(("VARCHAR", "CHAR", "TEXT")):
            df[column_name] = df[column_name].astype(object).where(
                pd.notnull(df[column_name]), None
            )
        elif column_type in {"INT", "INTEGER"}:
            column_values = pd.to_numeric(df[column_name], errors="coerce")
            try:
                df[column_name] = column_values.astype("Int64")
            except (TypeError, ValueError) as e:
                logging.warning(
                    "[df_types_from_yaml] Could not convert '%s' to Int64: %s. "
                    "Falling back to float64.",
                    column_name,
                    e,
                )
                df[column_name] = column_values
        elif column_type.startswith(("NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "REAL")):
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
        elif column_type in {"DATETIME", "DATE"}:
            df[column_name] = pd.to_datetime(df[column_name], errors="coerce")

    return df
