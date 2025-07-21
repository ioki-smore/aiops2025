from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
from collections import defaultdict
from exp.utils import read_parquet_with_filters, utc_to_cst


def parse_tags_array(tags_array) -> Dict:
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


def detect_large_message(spans: pd.DataFrame, threshold=1024*1024) -> List[Dict]:
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


def analyze_trace_group_durations(traces: Dict[str, Dict[str, Dict]], trace_ids: List[str], threshold_sigma=3):
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


def get_service_name(span_map: Dict[str, Dict[str, Dict]], span_id: str) -> Optional[str]:
    span = span_map.get(span_id)
    if not span:
        return None
    process = span.get('process')
    if not process:
        return None
    return process.get('serviceName')


def detect_service_self_calls(span_map: Dict[str, Dict[str, Dict]], children_map: Dict[str, List[str]]) -> List[str]:
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

    def load_spans(self, start: datetime, end: datetime, max_workers=4):
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
                    df['end'] = df['start'] + pd.to_timedelta(df['duration'], unit='us')
                    df['pod'] = df['process'].apply(lambda x: x.get('serviceName', 'unknown') if isinstance(x, dict) else 'unknown')
                    results.append(df[self.spans_fields])

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    def score(self, start_time: datetime, end_time: datetime):
        spans = self.load_spans(start_time, end_time)
        spans.dropna(subset=self.spans_fields, inplace=True)
        grouped = spans.groupby('traceID')

        valid_trace_ids = []
        for trace_id, group in grouped:
            trace_start = group['start'].min()
            trace_end = group['end'].max()
            if trace_start <= end_time and trace_end >= start_time:
                valid_trace_ids.append(trace_id)

        spans = spans[spans['traceID'].isin(valid_trace_ids)]
        print(f"Analyzing {len(valid_trace_ids)} valid traces from {start_time} to {end_time}")
        # traces = build_trace(spans)
        # structure_groups = group_by_structure(traces)

        scores = []
        operations = spans["operationName"].unique().tolist()
        operation_threshold = {o: spans[spans["operationName"] == o]["duration"].mean() + 3 * spans[spans["operationName"] == o]["duration"].std() for o in operations}
        # spans["rpc.service"] = spans["tags"].apply(lambda tags: parse_tags_array(tags)['rpc.service'])
        # spans["rpc.method"] = spans["tags"].apply(lambda tags: parse_tags_array(tags)['rpc.method'])
        # spans = spans.dropna(subset=["rpc.service", "rpc.method", "duration"])
        traces = spans.groupby('traceID')
        candidate_service = []
        for trace_id, spans in traces:

            for _, span in spans.iterrows():
                tags = parse_tags_array(span.get("tags", []))
                if tags.get("span.kind", "") != "client":
                    continue
                service = span["pod"]
                duration = span["duration"]
                status_code = tags.get("status.code", "0")
                http_status_code = tags.get("http.status_code", "200")
                operation = span["operationName"]
                logs = span.get("logs", [])

                score = 0
                reason = []

                # Check for anomalies
                if status_code != "0" and http_status_code != "200":
                    score += 10
                    reason.append("status_code != 0")

                if duration > operation_threshold.get(operation, 0):
                    score += 7
                    reason.append(f"duration {duration} exceeds threshold {operation_threshold.get(operation)}")

                for log in logs:
                    fields = log.get("fields", [])
                    for field in fields:
                        if "error" in str(field.get("value", "")).lower():
                            score += 10
                            reason.append("log contains error")
                            break
                        # if field.get("key") == "message.uncompressed_size":
                        #     size = int(field.get("value", "0"))
                        #     if size > 1024:
                        #         score += 3
                        #         reason.append(f"large message size {size} bytes")

                if score > 0 and (service, operation) not in candidate_service:
                    scores.append({
                        # "traceID": span["traceID"],
                        # "spanID": span["spanID"],
                        "service": service,
                        "operation": operation,
                        "score": score,
                        "reason": reason,
                        # "startTimeMillis": span["start"],
                        # "duration": duration
                    })
                    candidate_service.append((service, operation))
            trace = build_trace(spans)
            loops = detect_self_loops(trace['spans'], trace['children'])
            if loops:
                scores.append({
                    "traceID": trace_id,
                    "issue": "self_loops",
                    "details": loops
                })

            self_calls = detect_service_self_calls(trace['spans'], trace['children'])
            if self_calls:
                scores.append({
                    "traceID": trace_id,
                    "issue": "service_self_calls",
                    "details": self_calls
                })

        grouped = defaultdict(list)
        
        for s in scores:
            if "service" in s and s["score"] >= 5:
                threshold_info = next((r for r in s["reason"] if "duration" in r and "threshold" in r), "")
                if threshold_info:
                    try:
                        parts = threshold_info.split()
                        duration = float(parts[1])
                        threshold = float(parts[-1])
                        exceed_ratio = round(duration / threshold, 2)
                    except:
                        exceed_ratio = 1.0
                else:
                    exceed_ratio = 1.0

                grouped[s["service"]].append({
                    "operation": s["operation"],
                    "score": s["score"],
                    "exceed_ratio": exceed_ratio,
                    "reason": s["reason"]
                })

        compressed = []
        for service, ops in grouped.items():
            ops = sorted(ops, key=lambda x: (-x["score"], -x["exceed_ratio"]))
            top_ops = ops[:2]
            compressed.append({
                "service": service,
                "top_operations": top_ops
            })

        compressed = sorted(compressed, key=lambda x: -x["top_operations"][0]["score"])[:5]
        return compressed