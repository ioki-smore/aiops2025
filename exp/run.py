from concurrent.futures import ThreadPoolExecutor
from logging import debug
import os
import re
import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

class MetricAgent:
    def __init__(self, root_path: Path):
        self.metric_path = root_path
        self.infra_pod_metrics = ["pod_cpu_usage", "pod_memory_working_set_bytes", "pod_network_transmit_bytes", "pod_fs_reads_bytes", "pod_network_receive_bytes", "pod_network_transmit_packets", "pod_fs_writes_bytes", "pod_network_receive_packets", "pod_processes"]
        self.infra_pod_metrics_thresholds = {
            "pod_cpu_usage": 0.8,
            # "pod_memory_working_set_bytes": 80,
            # "pod_network_transmit_bytes": 80,
            # "pod_fs_reads_bytes": 80,
            # "pod_network_receive_bytes": 80,
            # "pod_network_transmit_packets": 80,
            # "pod_fs_writes_bytes": 80,
            # "pod_network_receive_packets": 80,
            # "pod_processes": 10
        }
        self.other_metrics = [
            "pd_abnormal_region_count", "pd_cpu_usage", "pd_leader_count", "pd_leader_primary",
            "pd_learner_count", "pd_memory_usage", "pd_region_count", "pd_region_health",
            "pd_storage_capacity", "pd_storage_size", "pd_storage_used_ratio",
            "pd_store_down_count", "pd_store_low_space_count", "pd_store_slow_count",
            "pd_store_unhealth_count", "pd_store_up_count", "pd_witness_count",
            "tikv_available_size", "tikv_capacity_size", "tikv_cpu_usage", "tikv_grpc_qps",
            "tikv_io_util", "tikv_memory_usage", "tikv_qps", "tikv_raft_apply_wait",
            "tikv_raft_propose_wait", "tikv_read_mbps", "tikv_region_pending",
            "tikv_rocksdb_write_stall", "tikv_server_is_up", "tikv_snapshot_apply_count",
            "tikv_store_size", "tikv_threadpool_readpool_cpu", "tikv_write_wal_mbps"
        ]

    def analyze(self, start_time: str, end_time: str) -> List[Dict]:
        utc_start = pd.to_datetime(start_time)
        cst_start = utc_start.tz_convert(pytz.timezone('Asia/Shanghai'))
        utc_end = pd.to_datetime(end_time)

        results = []
        # apm pod metrics
        for file in sorted(self.metric_path.joinpath(f"{cst_start.date()}/metric-parquet/apm/pod").glob("*.parquet")):
            if "(deleted)" in file.name:
                # print(f"Skipping deleted file: {file.name}")
                continue
            try:
                df = pd.read_parquet(file)
                df["time"] = pd.to_datetime(df["time"], utc=True)
                df = df[(df["time"] >= utc_start) & (df["time"] <= utc_end)]
                if df.empty:
                    continue

                monitor_fields = [
                    "client_error", "server_error", "error_ratio", "client_error_ratio", "server_error_ratio", "timeout", "rrt", "rrt_max"
                ]

                for field in monitor_fields:
                    if field not in df.columns:
                        continue
                    
                    mean = df[field].mean()
                    std = df[field].std()
                    threshold = mean + 3 * std

                    spikes = df[df[field] > threshold]

                    for _, row in spikes.iterrows():
                        results.append({
                            "time": row['time'].strftime("%Y-%m-%d %H:%M:%S"),
                            "pod": row['object_id'][:row['object_id'].find("-")],
                            "field": field,
                            "value": row[field],
                            # "mean": round(mean, 2),
                            # "threshold": round(threshold, 2)
                        })

            except Exception as e:
                print(f"Error reading {file.name}: {e}")
                continue
        
        # infra metrics
        for metric_type in self.infra_pod_metrics:
            filename = f"infra_pod_{metric_type}_{cst_start.date()}.parquet"
            file_path = self.metric_path / f"{cst_start.date()}" / "metric-parquet" / "infra" / "infra_pod" / filename
            if not file_path.exists():
                print(f"File {file_path} does not exist, skipping.")
                continue
            try:
                df = pd.read_parquet(file_path)
                if 'time' not in df.columns:
                    continue

                df['time'] = pd.to_datetime(df['time'], utc=True)
                df = df[(df['time'] >= utc_start) & (df['time'] <= utc_end)]

                value_column = self._detect_value_column(df)
                if not value_column:
                    continue
                if self.infra_pod_metrics_thresholds.get(metric_type):
                    threshold = self.infra_pod_metrics_thresholds[metric_type]
                else:
                    mean = df[value_column].mean()
                    std = df[value_column].std()
                    threshold = mean + 3 * std
                abnormal_df = df[df[value_column] > threshold]

                for _, row in abnormal_df.iterrows():
                    results.append({
                        "time": row['time'].strftime("%Y-%m-%d %H:%M:%S"),
                        "pod": row['pod'][:row['pod'].find("-")],
                        # "metric": metric_type,
                        "field": value_column,
                        "value": row[value_column],
                        # "mean": round(mean, 2),
                        # "threshold": round(threshold, 2)
                    })
            except Exception as e:
                print(f"Error processing {file_path.name}: {e}")
                continue
        # other metrics
        for metric_type in self.other_metrics:
            filename = f"infra_{metric_type}_{cst_start.date()}.parquet"
            file_path = self.metric_path / f"{cst_start.date()}" / "metric-parquet" / "other" / filename
            if not file_path.exists():
                print(f"File {file_path} does not exist, skipping.")
                continue
            try:
                df = pd.read_parquet(file_path)
                if 'time' not in df.columns:
                    continue
                df['time'] = pd.to_datetime(df['time'], utc=True)
                df = df[(df['time'] >= utc_start) & (df['time'] <= utc_end)]

                value_column = self._detect_value_column(df)
                if not value_column:
                    continue
                threshold = df[value_column].mean() + 3 * df[value_column].std()
                abnormal_df = df[df[value_column] > threshold]

                for _, row in abnormal_df.iterrows():
                    results.append({
                        "time": row['time'].strftime("%Y-%m-%d %H:%M:%S"),
                        "pod": "pd" if metric_type.startswith("pd") else "tikv",
                        # "metric": metric_type,
                        "field": value_column,
                        "value": row[value_column]
                    })
            except Exception as e:
                print(f"Error processing {file_path.name}: {e}")
                continue
        results = [dict(t) for t in {tuple(sorted(d.items())) for d in results}]
        results = [d for d in results if "monitoring" not in d.get("pod") and "redis" not in d.get("pod")]
        # print(f"Found {len(results)} abnormal metrics between {start_time} and {end_time}")
        results = results[:100]
        return results

    def _detect_value_column(self, df: pd.DataFrame) -> str:
        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        exclude = {'time'}
        candidates = [col for col in numeric_cols if col not in exclude]
        return candidates[0] if candidates else None


class TraceAgent:
    def __init__(self, root_path: Path):
        self.trace_path = root_path

    def analyze(self, component: str | None, start_time: str, end_time: str) -> List[str]:
        utc_start = pd.to_datetime(start_time)
        utc_end = pd.to_datetime(end_time)
        cst_start = utc_start.tz_convert(pytz.timezone('Asia/Shanghai'))
        file_name = f"trace_jaeger-span_{cst_start.date()}_{cst_start.strftime('%H')}-00-00.parquet"
        path = self.trace_path / f"{cst_start.date()}" / "trace-parquet" / file_name
        if not path.exists():
            return []
        df = pd.read_parquet(path)
        df["startTime"] = pd.to_datetime(df["startTimeMillis"], unit="ms", utc=True)
        df = df[(df["startTime"] >= utc_start) & (df["startTime"] <= utc_end)]
        if component is None:
            debug(f"No component specified, returning all traces between {start_time} and {end_time}")
        else:
            df = df[df["operationName"].str.contains(component, case=False, na=False)]
        if df.empty:
            debug(f"No traces found for component '{component}' between {start_time} and {end_time}")
            return []
        debug(f"Found {len(df)} traces for component '{component}' between {start_time} and {end_time}")
        
        results = []
        duration_mean = df["duration"].mean()
        duration_std = df["duration"].std()
        duration_threshold = duration_mean + 3 * duration_std
        duration_spikes = df[df["duration"] > duration_threshold]

        for _, row in duration_spikes.iterrows():
            results.append({
                "type": "high_latency",
                "traceID": row["traceID"],
                "spanID": row["spanID"],
                "operation": row["operationName"],
                "startTime": row["startTime"].strftime("%Y-%m-%d %H:%M:%S"),
                "duration": row["duration"],
                "mean_duration": round(duration_mean, 2),
                "threshold": round(duration_threshold, 2)
            })

        def is_error(tags, logs):
            for tag in tags:
                if tag.get("key") == "error" and tag.get("value") in [True, "true", "True"]:
                    return True
                if tag.get("key") == "http.response.status_code" and str(tag.get("value")).startswith("5"):
                    return True
            for log in logs:
                for field in log.get("fields", []):
                    if "exception" in field.get("key", "") or "error" in field.get("key", ""):
                        return True
            return False

        for _, row in df.iterrows():
            if is_error(row["tags"], row["logs"]):
                results.append({
                    "type": "error_tag",
                    "traceID": row["traceID"],
                    "spanID": row["spanID"],
                    "operation": row["operationName"],
                    "startTime": row["startTime"].strftime("%Y-%m-%d %H:%M:%S"),
                    "duration": row["duration"]
                })

        trace_counts = df["traceID"].value_counts()
        trace_mean = trace_counts.mean()
        trace_std = trace_counts.std()
        trace_threshold = trace_mean + 3 * trace_std

        for trace_id, count in trace_counts.items():
            if count > trace_threshold:
                results.append({
                    "type": "trace_span_count_high",
                    "traceID": trace_id,
                    "span_count": int(count),
                    "mean_span_count": round(trace_mean, 2),
                    "threshold": round(trace_threshold, 2)
                })
        results = [dict(t) for t in {tuple(sorted(d.items())) for d in results}]
        return results
        # grouped_traces = {}
        # for _, row in df.iterrows():
        #     if row['traceID'] not in grouped_traces:
        #         grouped_traces[row['traceID']] = []
        #     grouped_traces[row['traceID']].append({
        #         "spanID": row['spanID'],
        #         "operationName": row['operationName'],
        #         "startTime": datetime.fromtimestamp(row['startTimeMillis'] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        #         "duration": row['duration'],
        #         "tags": row['tags'],
        #         "logs": row['logs'],
        #         "process": row['process']
        #     })
        # for trace_id, spans in grouped_traces.items():
            # print(f"Trace ID: {trace_id}, Spans: {spans}")
        # loop_spans = related[related["operationName"].str.contains(component)]
        # return loop_spans["operationName"].tolist()
        return []


class LogAgent:
    def __init__(self, root_path: Path):
        self.log_path = root_path

    def analyze(self, component: str, start_time: str, end_time: str) -> List[Dict]:
        utc_start = pd.to_datetime(start_time)
        utc_end = pd.to_datetime(end_time)
        cst = utc_start.tz_convert(pytz.timezone('Asia/Shanghai'))
        file_name = f"log_filebeat-server_{cst.date()}_{cst.strftime('%H')}-00-00.parquet"
        path = self.log_path / f"{cst.date()}" / "log-parquet" / file_name
        if not path.exists():
            return []
        debug(f"Reading logs from: {path}")
        df = pd.read_parquet(path)
        error_logs = df[df['message'].notna() & df['message'].str.startswith("{") & df['message'].str.contains("warning|error|exception", case=False, na=False)]
        timestamp_series = pd.to_datetime(error_logs['@timestamp'], utc=True)
        slots = error_logs[timestamp_series.between(utc_start, utc_end)]
        result = []
        for _, row in slots.iterrows():
            try:
                message = json.loads(row["message"])
            except json.JSONDecodeError:
                continue  # skip invalid JSON

            combined = {
                "k8_namespace": row.get("k8_namespace"),
                "timestamp": row.get("@timestamp"),
                "agent_name": row.get("agent_name"),
                "k8_pod": row.get("k8_pod"),
                "k8_node_name": row.get("k8_node_name"),
                "error": message.get("error", "Unknown error"),
                "id": message.get("http.req.id", "Unknown ID"),
                "path": message.get("http.req.path", "Unknown path"),
                "method": message.get("http.req.method", "Unknown method"),
                "message": message.get("message", "No message"),
                "session": message.get("session", "Unknown session"),
            }
            if component in combined['k8_pod']:
                result.append(combined)
        debug(f"Found {len(result)} error logs between {start_time} and {end_time}")
        return result

class JudgeAgent:
    def query_metrics(self, metrics) -> Tuple[str, str]:
        prompt = f"""
        Given the following observations from metric:
        Metric: {metrics}
        What is the root cause? Note that the component is only one. Please answer in JSON format and provide me with concise and concise evidence, similar to `latency spikes detected`. Don't give me any explanations or details.:
        {{"component":..., "reason":..., "evidence":..., "confidence":0.0-1.0}}
        """
        response = call_llm(prompt)
        try:
            response = re.sub(r'^```json\s*|\s*```$', '', response.strip())
            res = json.loads(response)
            return res.get("component", ""), res.get("reason", "")
        except:
            print("LLM failed to parse response")
            return "", "LLM failed to parse"
        
    def reason(self, trace_obs, log_obs) -> Tuple[str, str, str]:
        prompt = f"""
        Given the following observations from trace, and log:
        Trace: {trace_obs[:20]}
        Log: {log_obs[:20]}
        What is the root cause? Please answer in JSON format, Note that the component is only one. Please answer in JSON format and provide me with concise and concise evidence, similar to `latency spikes detected`. Don't give me any explanations or details.:
        {{"component":..., "reason":..., "trace_reason":..., "log_reason":..., "evidence":..., "confidence":0.0-1.0}}
        """
        response = call_llm(prompt)
        try:
            response = re.sub(r'^```json\s*|\s*```$', '', response.strip())
            res = json.loads(response)
            return (
                res.get("reason", ""),
                res.get("trace_reason", ""),
                res.get("log_reason", "")
            )
        except Exception as e:
            print("LLM failed to parse response")
            print(f"LLM Response: {response}")
            return "error", "LLM failed to parse", "LLM failed to parse"

def call_llm(prompt: str) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("DEEPSEEK_API_KEY"), base_url=os.getenv("DEEPSEEK_API_URL"))
    response = client.chat.completions.create(
        model="deepseek-r1:671b-0528",
        messages=[
            {"role": "system", "content": "You are a root cause analysis expert for distributed systems"},
            {"role": "user", "content": prompt}

        ],
        response_format={"type": "json_object"},  # 强制JSON输出
        stream=False,
        temperature=0,       # 控制随机性 (0-2)
        top_p=0.95,             # 多样性控制
    )
    return response.choices[0].message.content

class Controller:
    def __init__(self, root_path: str):
        self.root_path = Path(root_path)
        self.metric_agent = MetricAgent(self.root_path)
        self.trace_agent = TraceAgent(self.root_path)
        self.log_agent = LogAgent(self.root_path)
        self.judge = JudgeAgent()

    def analyze_one(self, uuid: str, start: str, end: str) -> Dict:
        metric_obs = self.metric_agent.analyze(start, end)
        component, metrics_reason = self.judge.query_metrics(metric_obs)
        trace_obs = self.trace_agent.analyze(component, start, end)
        log_obs = self.log_agent.analyze(component, start, end)
        reason, trace_reason, log_reason = self.judge.reason(trace_obs, log_obs)
        return {
            "uuid": uuid,
            "component": component,
            "reason": reason,
            "time": start,
            "reasoning_trace": [
                {"step": 1, "action": "QueryMetrics", "observation": str(metrics_reason)},
                {"step": 2, "action": "TraceCheck", "observation": str(trace_reason)},
                {"step": 3, "action": "LogInspection", "observation": str(log_reason)}
            ],
        }

    def batch_analyze(self, tasks: List[Dict]) -> List[Dict]:
        results = []
        all = len(tasks)
        def analyze_task(task):
            uuid = task["uuid"]
            desc = task["Anomaly Description"]
            pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'
            utc_times = re.findall(pattern, desc)

            if len(utc_times) == 2:
                return self.analyze_one(uuid, utc_times[0], utc_times[1])
            else:
                print(f"Failed to parse time range in description: {desc}")
                return {"uuid": uuid, "error": "Failed to parse time range."}
        completed = 0
        with ThreadPoolExecutor(max_workers=64) as executor:
            future_to_task = {executor.submit(analyze_task, task): task for task in tasks}
            for future in as_completed(future_to_task):
                try:
                    results.append(future.result())
                    completed += 1
                    print(f"Task {future_to_task[future]['uuid']} completed successfully.")
                    print(f"Progress: {completed}/{all} ({(completed / all) * 100:.2f}%)")
                except Exception as e:
                    print(f"Error in analyzing task: {e}")
                    results.append({"uuid": future_to_task[future]["uuid"], "component": "", "reason": "", "reasoning_trace": []})
        return results


input = json.load(open("phaseone/input.json", "r", encoding="utf-8"))
if __name__ == "__main__":
    ctrl = Controller("phaseone/")
    
    print("Starting batch analysis...")
    print(f"Total tasks: {len(input)}")

    result = ctrl.batch_analyze(input)
    print("Batch analysis completed.")
    with open("submission/output.jsonl", "w", encoding="utf-8") as f:
        for item in result:
            json_line = json.dumps(item, ensure_ascii=False)
            f.write(json_line + "\n")
        