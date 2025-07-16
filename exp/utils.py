from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
import pyarrow.dataset as ds


CST = timezone(timedelta(hours=8))

def utc_to_cst(utc: datetime) -> datetime:
    """
    Convert UTC datetime to CST (China Standard Time).
    """
    # print(utc)
    if utc.tzinfo is None:
        utc = utc.replace(tzinfo=timezone.utc)
    return utc.astimezone(CST)

def daterange(start: datetime, end: datetime):
    start = utc_to_cst(start)
    end = utc_to_cst(end)
    for n in range((end.date() - start.date()).days + 1):
        yield (start.date() + timedelta(n)).strftime("%Y-%m-%d")

# @lru_cache(maxsize=64)
def read_parquet_with_filters(file: Path, columns: Optional[List[str]] = None, filter: Optional[Tuple] = None) -> pd.DataFrame:
    try:
        dataset = ds.dataset(file, format="parquet")
        # print(f"Reading {file}")
        table = dataset.to_table(
            columns=columns if columns else [field.name for field in dataset.schema],
            filter=filter,
            use_threads=True
        )
        return table.to_pandas()
    except Exception as e:
        print(f"Failed to read {file} with filter: {e}")
        return pd.DataFrame()

