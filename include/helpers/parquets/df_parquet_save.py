import tempfile
import os
import uuid
import pandas as pd


def df_parquet_save(df: pd.DataFrame) -> str:
    temp_dir = tempfile.gettempdir()
    filename = f"df_{uuid.uuid4().hex}.parquet"
    path = os.path.join(temp_dir, filename)
    df.to_parquet(path, index=False)
    return path
