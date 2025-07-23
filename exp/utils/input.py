from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow.dataset as ds

def read_parquet_with_filters(file: Path, columns: Optional[List[str]] = None, filter: Optional[ds.Expression] = None) -> pd.DataFrame:
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

