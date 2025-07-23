from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import pandas as pd
import pyarrow.dataset as ds
import logging

from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, List, Optional
from exp.utils.time import utc_to_cst

logger = logging.getLogger(__name__)

def load_parquet(file: Path, columns: Optional[List[str]] = None, filter: Optional[ds.Expression] = None) -> pd.DataFrame:
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

def load_parquet_by_hour(start: datetime, end: datetime, dataset: str, file_pattern: str, fields: Optional[List[str]] = None, filter: Optional[ds.Expression] = None, callback: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None, max_workers: int = 4) -> pd.DataFrame:
    start = utc_to_cst(start).replace(minute=0, second=0, microsecond=0)
    end = utc_to_cst(end).replace(minute=0, second=0, microsecond=0)
    
    files = []
    while start <= end:
        logger.info(f"Searching logs for {start.strftime('%Y-%m-%d %H:%M:%S')}")
        day = start.strftime("%Y-%m-%d")
        hour = start.strftime("%H")
        file_pattern = file_pattern.format(dataset=dataset, day=day, hour=hour)
        files.extend(glob.glob(file_pattern))
        start += timedelta(hours=1)

    records = []
    pool = ThreadPoolExecutor(max_workers=max_workers)
    futures = {pool.submit(load_parquet, Path(f), fields, filter): f for f in files}
    for future in as_completed(futures):
        record = future.result()
        if not record.empty:
            if callback:
                record = callback(record)
            records.append(record[fields])
    pool.shutdown(wait=True)

    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()