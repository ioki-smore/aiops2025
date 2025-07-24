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


def load_parquet(file: Path, columns: Optional[List[str]] = None,
                 filter: Optional[ds.Expression] = None) -> pd.DataFrame:
    try:
        dataset = ds.dataset(file, format="parquet")
        # print(f"Reading {file}")
        table = dataset.to_table(
            columns=columns if columns else [field.name for field in dataset.schema],
            filter=filter,
        )
        return table.to_pandas()
    except Exception as e:
        print(f"Failed to read {file} with filter: {e}")
        return pd.DataFrame()


def load_parquet_by_hour(
        start: datetime,
        end: datetime,
        dataset: str,
        file_pattern: str,
        load_fields: Optional[List[str]] = None,
        return_fields: Optional[List[str]] = None,
        filter: Optional[ds.Expression] = None,
        callback: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        max_workers: int = 4
) -> pd.DataFrame:
    start = utc_to_cst(start).replace(minute=0, second=0, microsecond=0)
    end = utc_to_cst(end).replace(minute=0, second=0, microsecond=0)

    files = []
    current = start
    while current <= end:
        logger.info(f"Searching records for {current.strftime('%Y-%m-%d %H:%M:%S')}")
        day = current.strftime("%Y-%m-%d")
        hour = current.strftime("%H")
        pattern = file_pattern.format(dataset=dataset, day=day, hour=hour)
        matched_files = glob.glob(pattern)
        if matched_files:
            files.extend(matched_files)
        else:
            logger.warning(f"No files matched for pattern: {pattern}")
        current += timedelta(hours=1)

    if not files:
        logger.warning(f"No files found for the {start} to {end}.")
        return pd.DataFrame()

    records = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(load_parquet, Path(f), load_fields, filter): f for f in files}
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                df = future.result()
                if df.empty:
                    logger.info(f"No data in file: {file_path}")
                    continue
                if callback:
                    df = callback(df)
                if return_fields:
                    missing_fields = set(return_fields) - set(df.columns)
                    if missing_fields:
                        logger.warning(f"File {file_path} missing fields {missing_fields} in return_fields")
                    df = df.loc[:, [f for f in return_fields if f in df.columns]]
                records.append(df)
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {e}", exc_info=True)

    if records:
        return pd.concat(records, ignore_index=True)
    else:
        return pd.DataFrame()
