import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

ROOT_DIR = Path("phaseone")

START_DATE = datetime.strptime("2025-06-06", "%Y-%m-%d")
END_DATE = datetime.strptime("2025-06-14", "%Y-%m-%d")

def daterange(start_date: datetime, end_date: datetime):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def preprocess_parquet(file_path: Path) -> None:
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        logging.error(f"Read file error: {file_path}: {e}")
        return
    column = "@timestamp" if file_path.name.startswith("log_") else "time"
    if column not in df.columns:
        logging.warning(f"Skip {file_path.name}, less {column} column")
        return

    ts = pd.to_datetime(df[column], errors="coerce", utc=True)
    if ts.isnull().all():
        ts = pd.to_datetime(df[column], errors="coerce", unit="s", utc=True)

    if ts.isnull().any():
        logging.warning(f"{file_path.name} contains invalid timestamps, dropping rows")

    df[column] = ts.dt.tz_localize(None)

    try:
        df.to_parquet(file_path, engine="pyarrow", allow_truncated_timestamps=True)
        logging.info(f"✅ processed {file_path.name} successfully")
    except Exception as e:
        logging.error(f"Failed to write {file_path.name}: {e}")

def process_directory_for_date(date: datetime, subdirs: list[str]) -> None:
    day_str = date.strftime("%Y-%m-%d")
    for subdir in subdirs:
        dir_path = ROOT_DIR / day_str / subdir
        if not dir_path.exists():
            logging.warning(f"Skip: {dir_path}")
            continue

        parquet_files = list(dir_path.glob("*.parquet"))
        if not parquet_files:
            logging.info(f"No parquet files found in {dir_path}")
            continue

        for file_path in parquet_files:
            preprocess_parquet(file_path)

def main():
    service_dirs = ["metric-parquet/apm/service"]
    infra_dirs = ["metric-parquet/infra/infra_pod"]
    other_dirs = ["metric-parquet/other"]
    log_dirs = ["log-parquet"]

    for date in daterange(START_DATE, END_DATE):
        process_directory_for_date(date, service_dirs + infra_dirs + other_dirs + log_dirs)
        # process_directory_for_date(date, log_dirs)

if __name__ == "__main__":
    main()