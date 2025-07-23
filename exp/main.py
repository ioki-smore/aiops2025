import logging
import json
import argparse

from concurrent.futures import ThreadPoolExecutor
import os
from typing import Dict
from datetime import datetime

from exp.agent.metric import MetricAgent
from exp.agent.trace import TraceAgent
from exp.agent.log import LogAgent
from exp.agent.judge import JudgeAgent
from exp.utils.log import setup_logger
from exp.utils.time import parse_time_range

logger = logging.getLogger(__name__)

def process_anomaly(item: Dict, metric_agent: MetricAgent, trace_agent: TraceAgent, log_agent: LogAgent, judge_agent: JudgeAgent):
    uuid = str(item.get("uuid", ""))
    description = str(item.get("Anomaly Description", ""))
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
    print(metric_result)
    print(trace_result)
    print(log_result)

    # metric_obs = metric_result.get("observation", "")
    # trace_obs = trace_result.get("observation", "")
    # log_obs = log_result.get("observation", "")

    # Use JudgeAgent to produce final analysis
    analysis = judge_agent.analyze(uuid, description, metric_result, trace_result, log_result)
    return analysis

def main(args: argparse.Namespace, uuid: str):
    dataset = str(args.dataset)
    log_file = f"results/{dataset}/logs/{uuid}.log"
    log_level = str(args.log_level).upper()
    input = f"{dataset}/input.json"
    output = f"results/{dataset}/answer/{uuid}-output.jsonl"
    max_workers = int(args.max_workers)

    # Setup logging
    setup_logger(log_file, log_level)
    logger.info(f"Logger initialized. Log file: {log_file}, Log level: {log_level}")
    logger.info(f"Dataset: {dataset}, UUID: {uuid}, Max Workers: {max_workers}")
    logger.info(f"Input file: {input}, Output file: {output}")

    logger.info("Starting analysis of anomalies...")

    # Initialize agents with data paths
    metric_agent = MetricAgent(dataset)
    trace_agent = TraceAgent(dataset)
    log_agent = LogAgent(dataset)
    judge_agent = JudgeAgent(None, None)

    # Load anomalies from input.json
    try:
        with open(input, 'r') as f:
            anomalies = json.load(f)
    except Exception as e:
        print(f"Failed to read {input}: {e}")
        anomalies = []

    # If no anomalies provided, create a dummy example for testing
    if not anomalies:
        print("No anomalies provided in input.json; using example anomaly for testing.")
        exit(-1)

    # Process anomalies concurrently
    results = []

    completed = 0
    all_tasks = len(anomalies)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    o = open(output, 'w', encoding='utf-8')
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_anomaly, item, metric_agent, trace_agent, log_agent, judge_agent) for item in anomalies[:1]]
        for future in futures:
            res = future.result()
            if res:
                results.append(res)
                completed += 1
                print(f"Processed {completed}/{all_tasks} anomalies.", end='\r')
                o.write(json.dumps(res, ensure_ascii=False) + "\n")
            else:
                print("Warning: Received None result from processing an anomaly.")
    o.flush() 
    o.close()
    print(f"Analysis complete. Results written to {output}.")

if __name__ == "__main__":

    uuid = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Argument parser for command line options
    parser = argparse.ArgumentParser(description="Analyze anomalies using various agents.")
    parser.add_argument('--dataset', type=str, default="phasetwo", help='Path to the data directory.')
    parser.add_argument('--max_workers', type=int, default=2, help='Number of worker threads for processing anomalies.')
    parser.add_argument('--log_level', type=str, default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).')
    args = parser.parse_args()
    main(args, uuid)