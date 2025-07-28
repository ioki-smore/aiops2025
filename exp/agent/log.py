import logging
import re
import pandas as pd
import pyarrow.dataset as ds

from datetime import datetime
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

from exp.utils.input import load_parquet_by_hour

logger = logging.getLogger(__name__)

error_pattern = re.compile(r'(?P<prefix>.*?)(?P<segment>rpc error: code = [^:]+? desc = (?:(?!rpc error:).)+)',
                           re.IGNORECASE | re.DOTALL)
code_desc_pattern = re.compile(r'code\s*=\s*(\w+)\s*desc\s*=\s*(.+)', re.IGNORECASE | re.DOTALL)

config = TemplateMinerConfig()
config.load("exp/template/drain3_log.ini")
persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence, config)


def aggregate_errors(log: pd.DataFrame) -> list:
    grouped = log.groupby(['code', 'desc', 'http.req.path', 'http.req.method'])
    aggregates = []
    for (code, desc, path, method), group in grouped:
        count = len(group)
        start = group['@timestamp'].min().to_pydatetime().replace(microsecond=0)
        end = group['@timestamp'].max().to_pydatetime().replace(microsecond=0)

        aggregates.append({
            'http.req.path': path,
            'http.req.method': method,
            'count': count,
            'code': code,
            'desc': desc,
            'timestamp': f"{start} -> {end}",
        })

    return aggregates


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
        self.analysis_fields = [
            "k8_namespace", "@timestamp", "k8_pod", "message", "k8_node_name", "code", "desc", "http.req.path",
            "http.req.method"]

    def load_logs(self, start: datetime, end: datetime, max_workers=4) -> pd.DataFrame:
        def callback(logs: pd.DataFrame) -> pd.DataFrame:
            def clean_path(path: str | None) -> str | None:
                if path is None:
                    return None
                if path.startswith('/product/'):
                    return '/product'
                else:
                    return path

            def try_parse(message: str) -> pd.Series:
                import json
                import ast
                # example = {
                #     'error': 'failed to get ads: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp 10.233.8.174:9555: connect: connection refused"',
                #     'http.req.id': 'cff2e4a5-473b-45fa-a2d7-2dbd7735c410', ''
                #     'http.req.method': 'GET',
                #     'http.req.path': '/product/0PUK6V6EV0',
                #     'message': 'failed to retrieve ads', # only two type, seem like it is valueless
                #     'session': '03bbc20f-700a-4cbb-8ba3-93e6698feec8',
                #     'severity': 'warning',
                #     'timestamp': '2025-06-17T08:14:31.470555511Z'
                #     }

                try:
                    log_msg = dict(json.loads(message))
                except json.JSONDecodeError:
                    try:
                        log_msg = dict(ast.literal_eval(message))
                    except (ValueError, SyntaxError) as e:
                        logging.debug(f"Literal eval failed: {e}")
                        return pd.Series([None, None, None, None, None])
                    except Exception as e:
                        logging.warning(f"Unexpected parsing error: {e}")
                        return pd.Series([None, None, None, None, None])

                error = log_msg.get('error')

                if not error:
                    return pd.Series([None, None, log_msg.get('message'),
                                      clean_path(log_msg.get('http.req.path')),
                                      log_msg.get('http.req.method')])
                matches = list(error_pattern.finditer(error))
                prefixes = []
                segments = []
                codes = []
                desc_ = []
                for i, m in enumerate(matches):
                    prefix = m.group("prefix").rstrip(": ") if i == 0 else ""
                    segment = m.group("segment").rstrip(":")
                    if prefix:
                        prefix = template_miner.add_log_message(prefix)
                        prefixes.append(prefix["template_mined"])
                    segment = template_miner.add_log_message(segment)
                    match = re.search(code_desc_pattern, segment["template_mined"])
                    if match:
                        code, desc = match.group(1), match.group(2)
                        codes.append(code)
                        desc_.append(desc)
                    segments.append(segment["template_mined"])
                code = " -> ".join(codes[::-1]) if len(codes) > 1 else codes[0] if codes else ""
                desc = " -> ".join(desc_[::-1]) if len(desc_) > 1 else desc_[0] if desc_ else ""
                prefix = " -> ".join(prefixes[::-1]) if len(prefixes) > 1 else prefixes[0] if prefixes else ""
                segment = " -> ".join(segments[::-1]) if len(segments) > 1 else segments[0] if segments else ""

                return pd.Series([
                    code,
                    desc,
                    f"{prefix}: {segment}",
                    clean_path(log_msg.get('http.req.path')),
                    log_msg.get('http.req.method'),
                ])

            logs = logs[logs['message'].notna() & logs['message'].str.startswith("{")].reset_index(drop=True)
            logs[['code', 'desc', 'parsed_message', 'http.req.path', 'http.req.method']] = logs['message'].apply(
                try_parse)

            mask = logs[['code', 'desc', 'parsed_message', 'http.req.path', 'http.req.method']].notna().all(axis=1)
            logs = logs.loc[mask].reset_index(drop=True)

            if 'message' in logs.columns:
                logs = logs.drop(columns=['message'])
            logs.rename(columns={'parsed_message': 'message'}, inplace=True)
            return logs

        return load_parquet_by_hour(
            start, end, self.root_path,
            file_pattern="{dataset}/{day}/log-parquet/log_filebeat-server_{day}_{hour}-00-00.parquet",
            load_fields=self.fields,
            return_fields=self.analysis_fields,
            filter_=(ds.field("@timestamp") >= start) & (ds.field("@timestamp") <= end),  # type: ignore
            callback=callback,
            max_workers=max_workers)

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
        # message keys: [
        # 'severity', 'time', 'message', 'pid', 'hostname', 'name', 'http.req.method', 'http.req.path',
        # 'v', 'logging.googleapis.com/trace', 'logging.googleapis.com/spanId', 'logging.googleapis.com/traceSampled',
        # 'http.req.id', 'session', 'timestamp', 'currency', 'id', 'http.resp.bytes', 'http.resp.status', 'http.resp.took_ms', 'curr.new', 'curr.old', 'order', 'logEvent', 'product', 'quantity', 'error']
        for (ns, node, pod), group in pod_groups:
            error = len(group)
            if error == 0:
                continue

            aggregates = aggregate_errors(group)

            scores.append({
                'namespace': ns,
                'node': node,
                'pod': pod,
                'error_count': error,
                'error_details': aggregates,
            })
            logger.info(f"Pod {pod} in namespace {ns} on node {node} has {error} error messages.")
        logger.info(scores)
        return scores

# {"failed to complete the order: rpc error: code = Internal desc = cart failure: failed to get user cart during checkout: rpc error: code = FailedPrecondition desc = Can't access cart storage. StackExchange.Redis.RedisTimeoutException: Timeout awaiting response (outbound=0KiB, inbound=0KiB, 5450ms elapsed, timeout is 5000ms), command=HGET, next: INFO, inst: 0, qu: 0, qs: 3, aw: False, bw: SpinningDown, rs: ReadAsync, ws: Idle, in: 0, in-pipe: 0, out-pipe: 0, last-in: 2, cur-in: 0, sync-ops: 2, async-ops: 27312, serverEndpoint: redis-cart:6379, conn-sec: 118978.57, aoc: 1, mc: 1/1/0, mgr: 10 of 10 available, clientName: cartservice-0(SE.Redis-v2.6.122.38350), IOCP: (Busy=0,Free=1000,Min=1,Max=1000), WORKER: (Busy=1,Free=32766,Min=1,Max=32767), POOL: (Threads=3,QueuedItems=0,CompletedItems=1109352,Timers=2), v: 2.6.122.38350 (Please take a look at this article for some common client-side issues that can cause timeouts: https://stackexchange.github.io/StackExchange.Redis/Timeouts)\n   at cartservice.cartstore.RedisCartStore.GetCartAsync(String userId) in /app/cartstore/RedisCartStore.cs:line 248",}
# TODO: 1. error only occurs in requests
# TODO: 2. add error message chain from current to downstream ✅
# TODO: 3. desc need to be handle (ignore number...) ✅
