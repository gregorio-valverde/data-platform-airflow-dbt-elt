import os
import logging
import pandas as pd


def df_parquet_read_delete(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    try:
        os.remove(path)
    except Exception as e:
        logging.warning(f"Could not delete temporary file {path}: {e}")
    return df
