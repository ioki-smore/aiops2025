from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

from utils import read_parquet_with_filters, utc_to_cst


def parse_tags_array(tags_array):
    tag_dict = {}
    if isinstance(tags_array, np.ndarray):
        for tag in tags_array:
            if isinstance(tag, dict) and "key" in tag and "value" in tag:
                tag_dict[tag["key"]] = tag["value"]
    return tag_dict


def detect_error_spans(spans: pd.DataFrame):
    error_spans = []
    for _, row in spans.iterrows():
        tag_dict = parse_tags_array(row.get("tags"))
        if tag_dict.get("status.code", "0") != "0" or str(tag_dict.get("http.status_code", "0")).startswith("5"):
            error_spans.append(row.to_dict())
    return error_spans


def detect_unbalanced_logs(spans: pd.DataFrame) -> List[Dict]:
    unbalanced = []
    for _, row in spans.iterrows():
        logs = row.get("logs")
        if logs.size == 0 or not isinstance(logs, np.ndarray):
            continue
        types = set()
        for log in logs:
            fields = log.get("fields")
            if isinstance(fields, np.ndarray):
                for f in fields:
                    if f.get("key") == "message.type":
                        types.add(f.get("value"))
        if not {"SENT", "RECEIVED"}.issubset(types):
            unbalanced.append(row.to_dict())
    return unbalanced


def detect_large_message(spans: pd.DataFrame, threshold=10*1024) -> List[Dict]:
    large_msgs = []
    for _, row in spans.iterrows():
        logs = row.get("logs")
        if logs.size == 0 or not isinstance(logs, np.ndarray):
            continue
        for log in logs:
            fields = log.get("fields")
            if isinstance(fields, np.ndarray):
                for f in fields:
                    if f.get("key") == "message.uncompressed_size":
                        size = int(f.get("value", "0"))
                        if size > threshold:
                            large_msgs.append(row.to_dict())
    return large_msgs


def build_trace(spans: pd.DataFrame) -> Dict[str, Dict]:
    traces = defaultdict(lambda: {'spans': {}, 'children': defaultdict(list), 'roots': []})
    for _, row in spans.iterrows():
        trace_id = row['traceID']
        span_id = row['spanID']
        span = row.to_dict()
        traces[trace_id]['spans'][span_id] = span
        refs = span.get('references', "[]")
        if isinstance(refs, list):
            parents = [r['spanID'] for r in refs if r.get('refType') == 'CHILD_OF']
            if parents:
                for p in parents:
                    traces[trace_id]['children'][p].append(span_id)
            else:
                traces[trace_id]['roots'].append(span_id)
        else:
            traces[trace_id]['roots'].append(span_id)
    return traces


def detect_trace_structure_signature(trace: Dict) -> str:
    def dfs(node, children):
        if node not in children:
            return node
        return f"{node}({','.join(sorted([dfs(c, children) for c in children[node]]))})"

    roots = trace['roots'] or list(trace['spans'].keys())
    children = trace['children']
    signatures = [dfs(r, children) for r in roots]
    return '|'.join(sorted(signatures))


def group_by_structure(traces: Dict[str, Dict]) -> Dict[str, List[str]]:
    groups = defaultdict(list)
    for tid, trace in traces.items():
        signature = detect_trace_structure_signature(trace)
        groups[signature].append(tid)
    return groups


def analyze_trace_group_durations(traces: Dict[str, Dict], trace_ids: List[str], threshold_sigma=3):
    durations = [sum([s['duration'] for s in traces[tid]['spans'].values()]) for tid in trace_ids]
    mean, std = np.mean(durations), np.std(durations)
    if std == 0:
        return []
    return [tid for tid, dur in zip(trace_ids, durations) if (dur - mean) > threshold_sigma * std]


def get_all_children(children_map: Dict) -> set:
    all_children = set()
    for children in children_map.values():
        all_children.update(children)
    return all_children


def detect_self_loops(span_map: Dict, children_map: Dict) -> List[List[str]]:
    visited = set()
    stack = []
    loops = []

    def dfs(span_id):
        if span_id in stack:
            loop_start = stack.index(span_id)
            loops.append(stack[loop_start:] + [span_id])
            return
        if span_id in visited:
            return
        visited.add(span_id)
        stack.append(span_id)
        for child in children_map.get(span_id, []):
            dfs(child)
        stack.pop()

    for root in [s for s in span_map if s not in get_all_children(children_map)]:
        dfs(root)
    return loops


def get_service_name(span_map: Dict, span_id: str) -> Optional[str]:
    span = span_map.get(span_id)
    if not span:
        return None
    process = span.get('process')
    if not process:
        return None
    return process.get('serviceName')


def detect_service_self_calls(span_map: Dict, children_map: Dict) -> List[str]:
    self_calls = []
    for parent_id, children in children_map.items():
        parent_service = get_service_name(span_map, parent_id)
        for child_id in children:
            child_service = get_service_name(span_map, child_id)
            if parent_service and child_service and parent_service == child_service:
                self_calls.append((parent_id, child_id))
    return self_calls


class TraceAgent:
    def __init__(self, root_path: str):
        self.root_path = Path(root_path)
        self.fields = ["traceID", "spanID", "operationName", "references", "startTimeMillis", "duration", "tags", "logs", "process"]
        self.spans_fields = ["traceID", "spanID", "operationName", "references", "start", "end", "duration", "tags", "logs", "process", "pod"]

    def load_spans(self, start: datetime, end: datetime, max_workers=8):
        start_cst = utc_to_cst(start) - timedelta(hours=1)
        end_cst = utc_to_cst(end) + timedelta(hours=1)

        files = []
        current = start_cst.replace(minute=0, second=0, microsecond=0)
        end_hour = end_cst.replace(minute=0, second=0, microsecond=0)

        while current <= end_hour:
            day = current.strftime("%Y-%m-%d")
            hour = current.strftime("%H")
            file_pattern = f"{self.root_path}/{day}/trace-parquet/trace_jaeger-span_{day}_{hour}-00-00.parquet"
            files.extend(glob.glob(file_pattern))
            current += timedelta(hours=1)

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(read_parquet_with_filters, Path(f), self.fields): f for f in files}
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    df['start'] = pd.to_datetime(df["startTimeMillis"], unit="ms")
                    df['end'] = df['start'] + pd.to_timedelta(df['duration'], unit='ms')
                    df['pod'] = df['process'].apply(lambda x: x.get('serviceName', 'unknown') if isinstance(x, dict) else 'unknown')
                    results.append(df[self.spans_fields])

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    def analyze_spans(self, start_time: datetime, end_time: datetime):
        spans = self.load_spans(start_time, end_time)
        grouped = spans.groupby('traceID')

        valid_trace_ids = []
        for trace_id, group in grouped:
            trace_start = group['start'].min()
            trace_end = group['end'].max()
            if trace_start <= end_time and trace_end >= start_time:
                valid_trace_ids.append(trace_id)

        spans = spans[spans['traceID'].isin(valid_trace_ids)]
        logging.info(f"Analyzing {len(valid_trace_ids)} valid traces from {start_time} to {end_time}")
        traces = build_trace(spans)
        structure_groups = group_by_structure(traces)

        issues = []
        details = {}

        long_traces = []
        for _, tids in structure_groups.items():
            outliers = analyze_trace_group_durations(traces, tids)
            if outliers:
                long_traces.extend(outliers)

        if long_traces:
            issues.append("long_duration_traces")
            details['long_duration_traces'] = long_traces

        error_spans = detect_error_spans(spans)
        if error_spans:
            issues.append("error_spans")
            details['error_spans'] = error_spans

        unbalanced_logs = detect_unbalanced_logs(spans)
        if unbalanced_logs:
            issues.append("unbalanced_logs")
            details['unbalanced_logs'] = unbalanced_logs

        large_messages = detect_large_message(spans)
        if large_messages:
            issues.append("large_messages")
            details['large_messages'] = large_messages

        for tid in valid_trace_ids:
            trace = traces[tid]
            loops = detect_self_loops(trace['spans'], trace['children'])
            if loops:
                issues.append("self_loops")
                details.setdefault('self_loops', []).extend(loops)

            self_calls = detect_service_self_calls(trace['spans'], trace['children'])
            if self_calls:
                issues.append("service_self_calls")
                details.setdefault('service_self_calls', []).extend(self_calls)

        observation = f"Detected issues: {', '.join(set(issues))}." if issues else "No significant anomalies detected in traces."
        # logging.info({
        #     "observation": observation,
        #     "details": details,
        # })
        return {
            "observation": observation,
            "details": details
        }