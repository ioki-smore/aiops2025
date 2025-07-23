import logging
import json
import pandas as pd
import pyarrow.dataset as ds

from datetime import datetime
from exp.utils.input import load_parquet_by_hour

logger = logging.getLogger(__name__)

def has_error_key(message: str) -> bool:
    try:
        message = json.loads(message)
        return 'error' in message
    except json.JSONDecodeError:
        return False

"""
k8_namespace	hipstershop
timestamp	2025-06-05T16:00:27.724Z
agent_name	filebeat-filebeat-nx7q2
k8_pod	frontend-2
message	{"http.req.id":"f86f5b1e-cd6d-40b2-bc62-21d870517fbb","http.req.method":"GET","http.req.path":"/product/2ZYFJ3GM2N","http.resp.bytes":8014,"http.resp.status":200,"http.resp.took_ms":119,"message":"request complete","session":"bc7eadb2-b959-42f7-ab22-24a95c25f3b5","severity":"debug","timestamp":"2025-06-05T16:00:27.724357829Z"}
k8_node_name	aiops-k8s-04
"""
class LogAgent:
    """
    Enhanced LogAgent with structured log filtering and keyword clustering.
    """
    ERROR_KEYWORDS = ['warning', 'error', 'exception', 'fail', 'timeout', 'critical', 'panic']
    def __init__(self, dataset: str):
        print(f"Initializing LogAgent with root path: {dataset}")
        self.root_path = dataset
        self.fields = [
            "k8_namespace", "@timestamp", 
            # "agent_name", 
            "k8_pod", "message", "k8_node_name"
        ]

    def load_logs(self, start: datetime, end: datetime, max_workers=4) -> pd.DataFrame:
        return load_parquet_by_hour(start, end, self.root_path,
                             file_pattern="{dataset}/{day}/log-parquet/log_filebeat-server_{day}_{hour}-00-00.parquet",
                             fields=self.fields,
                             filter=(ds.field("@timestamp") >= start) & (ds.field("@timestamp") <= end),
                                        callback=lambda logs: logs[logs['message'].notna() & logs['message'].str.startswith("{")],
                                max_workers=max_workers)
        # current = utc_to_cst(start).replace(minute=0, second=0, microsecond=0)
        # end_cst = utc_to_cst(end).replace(minute=0, second=0, microsecond=0)
       
        # files = []
        # while current <= end_cst:
        #     logger.info(f"Searching logs for {current.strftime('%Y-%m-%d %H:%M:%S')}")
        #     day = current.strftime("%Y-%m-%d")
        #     hour = current.strftime("%H")
        #     file_pattern = f"{self.root_path}/{day}/log-parquet/log_filebeat-server_{day}_{hour}-00-00.parquet"
        #     files.extend(glob.glob(file_pattern))
        #     current += timedelta(hours=1)

        # logs = []
        # filter = (ds.field("@timestamp") >= start) & (ds.field("@timestamp") <= end)
        # with ThreadPoolExecutor(max_workers=max_workers) as pool:
        #     futures = {pool.submit(read_parquet_with_filters, Path(f), self.fields, filter): f for f in files}
        #     for future in as_completed(futures):
        #         log = future.result()
        #         if not log.empty:
        #             log = log[log['message'].notna() & log['message'].str.startswith("{")]

        #             logs.append(log[self.fields])

        # return pd.concat(logs, ignore_index=True) if logs else pd.DataFrame()

    
    def score(self, start_time: datetime, end_time: datetime):
        """
        Inspect logs between start_time and end_time for error events.
        Returns a dict with an observation and details of log events.
        """
        log = self.load_logs(start_time, end_time)
        if log.empty:
            return []
        pod_groups = log.groupby(['k8_namespace', 'k8_node_name', 'k8_pod'])
        scores = []
# message keys: ['severity', 'time', 'message', 'pid', 'hostname', 'name', 
# 'v', 'logging.googleapis.com/trace', 'logging.googleapis.com/spanId', 'logging.googleapis.com/traceSampled', 
# 'http.req.id', 'http.req.method', 'http.req.path', 'session', 'timestamp', 'currency', 'id', 'http.resp.bytes', 'http.resp.status', 'http.resp.took_ms', 'curr.new', 'curr.old', 'order', 'logEvent', 'product', 'quantity', 'error']
        for (ns, node, pod), group in pod_groups:
            error = int(group['message'].apply(has_error_key).sum())
            if error == 0:
                continue

            # TODO: add more detailed scoring logic based on error types or counts
# example = {
#     'error': 'failed to get ads: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp 10.233.8.174:9555: connect: connection refused"', 
#     'http.req.id': 'cff2e4a5-473b-45fa-a2d7-2dbd7735c410', ''
#     'http.req.method': 'GET', 
#     'http.req.path': '/product/0PUK6V6EV0', 
#     'message': 'failed to retrieve ads', 
#     'session': '03bbc20f-700a-4cbb-8ba3-93e6698feec8', 
#     'severity': 'warning', 
#     'timestamp': '2025-06-17T08:14:31.470555511Z'
#     }

            logger.info(f"Pod {pod} in namespace {ns} on node {node} has {error} error messages.")
            scores.append({
                'service': pod,
                'error_count': error,
            })
        return scores