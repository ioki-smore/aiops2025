import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# 根目录，例如 "phaseone"
ROOT_DIR = Path("phasetwo")

# 起止日期
START_DATE = datetime.strptime("2025-06-17", "%Y-%m-%d")
END_DATE = datetime.strptime("2025-06-29", "%Y-%m-%d")

# 遍历日期范围
def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

# 处理单个文件
def preprocess_parquet(file_path: Path):
    try:
        df = pd.read_parquet(file_path)
        if "time" not in df.columns:
            print(f"Skipping {file_path.name}: no 'time' column")
            return

        # 转换 @timestamp 字段为 datetime
        df["time"] = pd.to_datetime(df["time"], unit="s", errors="coerce", utc=True).dt.tz_localize(None)

        # 可选：移除无效行（非必需）
        # df = df[df["time"].notnull()]

        # 覆盖写回原文件
        df.to_parquet(file_path, engine="pyarrow", allow_truncated_timestamps=True)
        print(f"✅ Processed {file_path} with {len(df)} records")
    except Exception as e:
        print(f"❌ Failed to process {file_path}: {e}")

# 主程序
for date in daterange(START_DATE, END_DATE):
    day_str = date.strftime("%Y-%m-%d")
    service_dir = ROOT_DIR / day_str / "metric-parquet" / "apm" / "service"
    if not service_dir.exists():
        print(f"Skipping missing directory: {service_dir}")
        continue

    for file_path in service_dir.glob("*.parquet"):
        preprocess_parquet(file_path)

    infra_dir = ROOT_DIR / day_str / "metric-parquet" / "infra" / "infra_pod"
    if not infra_dir.exists():
        print(f"Skipping missing directory: {infra_dir}")
        continue

    for file_path in infra_dir.glob("*.parquet"):
        preprocess_parquet(file_path)
    
    other_dir = ROOT_DIR / day_str / "metric-parquet" / "other"
    if not other_dir.exists():
        print(f"Skipping missing directory: {other_dir}")
        continue 
    for file_path in other_dir.glob("*.parquet"):
        preprocess_parquet(file_path)

    # log_dir = ROOT_DIR / day_str / "log-parquet"
    # if not log_dir.exists():
    #     print(f"Skipping missing directory: {log_dir}")
    #     continue

    # for file_path in log_dir.glob("*.parquet"):
    #     preprocess_parquet(file_path)

    
