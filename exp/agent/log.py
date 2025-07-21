from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
from pathlib import Path
from typing import Counter
import pandas as pd
from datetime import datetime, timedelta
import pyarrow.dataset as ds

from exp.utils import read_parquet_with_filters, utc_to_cst

class LogAgent:
    """
    Enhanced LogAgent with structured log filtering and keyword clustering.
    """
    ERROR_KEYWORDS = ['warning', 'error', 'exception', 'fail', 'timeout', 'critical', 'panic']
    def __init__(self, root_path: str):
        print(f"Initializing LogAgent with root path: {root_path}")
        self.root_path = Path(root_path)
        self.fields = ["k8_namespace", "k8_pod", "k8_node_name", "agent_name", "message", "@timestamp"]
        
    def load_logs(self, start: datetime, end: datetime, max_workers=8):
        start_cst = utc_to_cst(start) - timedelta(hours=1)
        end_cst = utc_to_cst(end) + timedelta(hours=1)

        files = []
        current = start_cst.replace(minute=0, second=0, microsecond=0)
        end_hour = end_cst.replace(minute=0, second=0, microsecond=0)

        while current <= end_hour:
            day = current.strftime("%Y-%m-%d")
            hour = current.strftime("%H")
            file_pattern = f"{self.root_path}/{day}/log-parquet/log_filebeat-server_{day}_{hour}-00-00.parquet"
            files.extend(glob.glob(file_pattern))
            current += timedelta(hours=1)

        results = []
        filter = (ds.field("@timestamp") >= start) & (ds.field("@timestamp") <= end)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(read_parquet_with_filters, Path(f), self.fields, filter): f for f in files}
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    df["k8_pod"] = df["k8_pod"].astype(str).str.replace(r"-\d+$", "", regex=True)
                    df = df[df['message'].notna() & df['message'].str.startswith("{")]
                    results.append(df[self.fields])

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    
    def score(self, start_time: datetime, end_time: datetime):
        """
        Inspect logs between start_time and end_time for error events.
        Returns a dict with an observation and details of log events.
        """
        log = self.load_logs(start_time, end_time)
        if log.empty:
            observation = "No log data available for analysis."
            return {"observation": observation, "details": {}}
        pod_groups = log.groupby(['k8_namespace', 'k8_pod'])
        scores = []
        total_errors = 0
        for (_, pod), group in pod_groups:
            pod_errors = len(group)
            total_errors += pod_errors
            
            # sample_logs = group.head(5)[["k8_node_name", "agent_name", "message", "@timestamp"]].to_dict('records')
            
            keyword_counts = Counter()
            for msg in group['message'].astype(str):
                for kw in self.ERROR_KEYWORDS:
                    if kw in msg.lower():
                        keyword_counts[kw] += 1

            scores.append({
                'service': pod,
                'error_count': pod_errors,
                # 'sample_logs': sample_logs,
                # 'top_keywords': dict(keyword_counts.most_common(3))
            })
        return scores