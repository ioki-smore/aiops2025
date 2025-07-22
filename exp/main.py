import logging
import os
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

from exp.agent.metric import MetricAgent
from exp.agent.trace import TraceAgent
from exp.agent.log import LogAgent
from exp.agent.judge import JudgeAgent

DATA_PATH = 'phasetwo/'
INPUT_JSON = 'phasetwo/input.json'
OUTPUT_JSONL = 'submission/output.jsonl'

def parse_time_range(description: str):
    """
    Parse start and end UTC times from the anomaly description.
    Returns (start_time, end_time) as datetime objects.
    """
    # Regular expression to find datetime strings (e.g., 2023-07-10T12:00:00Z)
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'
    matches = re.findall(pattern, description)
    if len(matches) >= 2:
        start_str, end_str = str(matches[0]), str(matches[1])
    elif len(matches) == 1:
        start_str = end_str = str(matches[0])
    else:
        return None, None

    try:
        start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
    except ValueError:
        start_time = datetime.fromisoformat(start_str)
    try:
        end_time = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
    except ValueError:
        end_time = datetime.fromisoformat(end_str)

    # Remove timezone info for compatibility with naive timestamps
    if start_time.tzinfo:
        start_time = start_time.replace(tzinfo=None)
    if end_time.tzinfo:
        end_time = end_time.replace(tzinfo=None)
    return start_time, end_time

def process_anomaly(item: Dict, metric_agent: MetricAgent, trace_agent: TraceAgent, log_agent: LogAgent, judge_agent: JudgeAgent):
    """
    Process a single anomaly item.
    """
    uuid = item.get("uuid")
    description = item.get("Anomaly Description", "")
    start_time, end_time = parse_time_range(description)
    if not start_time or not end_time:
        print(f"Warning: Could not parse time range from description: {description}")
        return {
            "uuid": uuid,
            "component": "Unknown",
            "reason": "Time range parsing failed.",
            "reasoning_trace": []
        }
    analysis = {}
    # Query each agent
    metric_result = metric_agent.score(start_time, end_time)
    trace_result = trace_agent.score(start_time, end_time)
    log_result = log_agent.score(start_time, end_time)
    print(metric_result), print(trace_result), print(log_result)

    # metric_obs = metric_result.get("observation", "")
    # trace_obs = trace_result.get("observation", "")
    # log_obs = log_result.get("observation", "")

    # Use JudgeAgent to produce final analysis
    analysis = judge_agent.analyze(uuid, description, metric_result, trace_result, log_result)
    return analysis

# async def main():
def main():
    # Ensure output directory exists
    logger = logging.getLogger(__name__)
    logger.info("Starting analysis of anomalies...")
    os.makedirs(os.path.dirname(OUTPUT_JSONL), exist_ok=True)

    # Initialize agents with data paths
    metric_agent = MetricAgent(DATA_PATH)
    trace_agent = TraceAgent(DATA_PATH)
    log_agent = LogAgent(DATA_PATH)
    judge_agent = JudgeAgent(None, None)

    # Load anomalies from input.json
    try:
        with open(INPUT_JSON, 'r') as f:
            anomalies = json.load(f)
    except Exception as e:
        print(f"Failed to read {INPUT_JSON}: {e}")
        anomalies = []

    # If no anomalies provided, create a dummy example for testing
    if not anomalies:
        print("No anomalies provided in input.json; using example anomaly for testing.")
        exit(-1)

    # Process anomalies concurrently
    results = []
    # 批量处理（推荐用分页分批发送）
    # batch_inputs = []
    # for item in anomalies[:1]:
    #     uuid = item.get("uuid")
    #     desc = item.get("Anomaly Description", "")
    #     start_time, end_time = parse_time_range(desc)
    #     if not start_time or not end_time:
    #         continue

    #     metric = metric_agent.query_metrics(start_time, end_time)
    #     trace = trace_agent.analyze_spans(start_time, end_time)
    #     log = log_agent.inspect_logs(start_time, end_time)

    #     batch_inputs.append({
    #         "uuid": uuid,
    #         "description": desc,
    #         "metric_obs": metric.get("observation", ""),
    #         "trace_obs": trace.get("observation", ""),
    #         "log_obs": log.get("observation", "")
    #     })

    # results = await judge_agent.async_analyze_batch(batch_inputs)

    completed = 0
    all_tasks = len(anomalies)
    output = open(OUTPUT_JSONL, 'w', encoding='utf-8')
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(process_anomaly, item, metric_agent, trace_agent, log_agent, judge_agent)
                   for item in anomalies]
        for future in futures:
            res = future.result()
            if res:
                results.append(res)
                completed += 1
                print(f"Processed {completed}/{all_tasks} anomalies.", end='\r')
                output.write(json.dumps(res, ensure_ascii=False) + "\n")
            else:
                print("Warning: Received None result from processing an anomaly.")
            
    output.close()
    print(f"Analysis complete. Results written to {OUTPUT_JSONL}.")

if __name__ == "__main__":
    # import asyncio
    # asyncio.run(main())
    main()