# import json
# response = """{'content': '\n{\n  "component": "pd",\n  "reason": "Unbalanced logs causing uneven region distribution and resource contention",\n  "reasoning_trace": {\n    "QueryMetrics": "Anomalies detected in pd metrics: reduced available_size indicates storage pressure; elevated read_mbps and write_wal_mbps suggest abnormal I/O patterns; increased region_pending implies scheduling backlog.",\n    "TraceCheck": "Detected unbalanced_logs issue, indicating uneven log distribution across regions, leading to hotspots and resource contention.",\n    "LogInspection": "No log data available for further validation."\n  }\n}', 'role': 'assistant', 'tool_calls': [], 'reasoning_content': '\nWe are given an anomaly time window and several observations from metrics, traces, and logs.\n The metrics show anomalies in the \'pd\' component for: available_size, read_mbps, region_pending, write_wal_mbps.\n The trace observation shows an issue: unbalanced_logs.\n The log observation states: no log data available.\n\n Steps:\n 1. QueryMetrics: We look at the metrics that are anomalous. The component is \'pd\' (which typically stands for Placement Driver in distributed systems like TiDB). The anomalous metrics are:\n    - available_size: This might indicate a storage capacity issue.\n    - read_mbps: This is the read throughput. An anomaly here could indicate a spike in read operations or a problem in handling reads.\n    - region_pending: This typically refers to the number of regions that are waiting to be processed (like in a queue). A high value suggests that the system is falling behind in processing region-related tasks (such as leader transfers, splits, etc.).\n    - write_wal_mbps: This is the write throughput to the Write-Ahead Log (WAL). An anomaly here could indicate a spike in writes or a problem in writing to the WAL.\n\n 2. TraceCheck: The trace observation indicates "unbalanced_logs". This suggests that there is an imbalance in the log distribution or processing. In distributed systems, logs (like Raft logs) being unbalanced could mean that some nodes have more logs to process than others, leading to hotspots.\n\n 3. LogInspection: There is no log data available, so we cannot get additional information from logs.\n\n Putting it together:\n - The metrics from the PD component show issues with storage capacity (available_size), read and write throughput, and a backlog in region processing (region_pending).\n - The trace of unbalanced_logs suggests that the system might be experiencing an uneven load in terms of log processing. This could be due to regions (or partitions) being unevenly distributed across nodes, causing some nodes to be overloaded with write requests (which would also affect the WAL writes and read operations if there are read hotspots as well).\n\n Root cause reasoning:\n - The unbalanced_logs (from traces) could be the root cause. When logs are unbalanced, some nodes may be overwhelmed with write requests, leading to:\n   - Increased write_wal_mbps on those nodes (and possibly the entire system if the imbalance is severe).\n   - The region_pending metric being high because the PD (which manages regions) might be trying to rebalance regions but is overwhelmed or because the uneven load causes some regions to have pending tasks.\n   - The available_size might be affected if the writes are causing more storage to be used (or if the system is struggling to compact logs, leading to increased storage usage).\n   - The read_mbps anomaly could be a side effect: if the system is busy with writes and rebalancing, read performance might degrade.\n\n Therefore, the root cause is likely the unbalanced logs leading to an uneven load and causing the observed metric anomalies in the PD.\n\n Component: Since the metrics are from \'pd\', we attribute the root cause to the PD component? However, note that the unbalanced_logs might be originating from the storage nodes (like TiKV in TiDB). But the PD is the component that manages the regions and is responsible for balancing. So the issue might be that the PD is failing to balance the logs appropriately, or the imbalance is beyond what the PD can handle in a timely manner.\n\n However, note that the trace observation does not specify a component. But the metrics are from PD. So we can say the root cause is related to PD because it is responsible for region scheduling and balancing.\n\n Alternatively, the unbalanced_logs might be a symptom of a deeper issue in the storage layer that the PD is supposed to manage. But without more information, we can only go by the provided data.\n\n Since the question asks for the root cause and the trace shows unbalanced_logs, and the metrics from PD show problems that are consistent with unbalanced logs, we can conclude:\n\n Component: "pd"\n Reason: "Unbalanced logs leading to uneven load and resource exhaustion"\n\n However, note that the trace observation of "unbalanced_logs" might be the root cause. So we can say:\n\n Reason: "Unbalanced logs causing uneven load on regions, leading to high pending regions, increased I/O, and storage pressure"\n\n Let\'s structure the JSON:\n\n {\n   "component": "pd",\n   "reason": "Unbalanced logs causing uneven load on regions, leading to high pending regions, increased I/O, and storage pressure",\n   "reasoning_trace": {\n     "QueryMetrics": "Anomalies detected in pd: available_size (storage capacity), read_mbps (read throughput), region_pending (backlog in region processing), write_wal_mbps (write throughput to WAL).",\n     "TraceCheck": "Detected issue: unbalanced_logs, indicating uneven distribution of log processing.",\n     "LogInspection": "No log data available."\n   }\n }\n\n But note: the component in the reason should be the one where the root cause lies. The unbalanced_logs might be a system-wide issue, but the metrics are from PD and PD is responsible for balancing. So we attribute to PD.\n\n Alternatively, if the unbalanced_logs is a symptom of a storage node failure or network partition, then the root cause might be elsewhere. However, the data does not provide that. We must stick to the given observations.\n\n Therefore, we output the JSON as below.\n'}"""
# response = json.loads(response)
# print(response)

import json
import re
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

# 日志样例列表
logs = [
    'failed to get ads: rpc error: code = DeadlineExceeded desc = context deadline exceeded',
    'failed to get product recommendations: rpc error: code = Unknown desc = Exception calling application: <_InactiveRpcError of RPC that terminated with:\n\tstatus = StatusCode.UNAVAILABLE\n\tdetails = "Connection reset by peer"\n\tdebug_error_string = "{"created":"@1750145456.221303383","description":"Error received from peer ipv4:10.233.54.177:3550","file":"src/core/lib/surface/call.cc","file_line":1055,"grpc_message":"Connection reset by peer","grpc_status":14}"\n>',
    'failed to complete the order: rpc error: code = Canceled desc = context canceled',
    'failed to complete the order: rpc error: code = Unavailable desc = transport is closing',
    "failed to add to cart: rpc error: code = FailedPrecondition desc = Can't access cart storage.",
    'failed to complete the order: rpc error: code = Internal desc = failed to prepare order: failed to get product #"6E92ZMYYFZ"',
    'failed to get product recommendations: rpc error: code = Canceled desc = context canceled',
    'could not retrieve currencies: rpc error: code = Canceled desc = context canceled',
    'failed to do currency conversion for product L9ECAV7KIM: rpc error: code = Canceled desc = context canceled',
    'failed to get shipping quote: failed to convert currency for shipping cost: rpc error: code = Canceled desc = context canceled',
    'could not convert currency for product #1YMWWN1N4O: rpc error: code = Canceled desc = context canceled',
    'failed to complete the order: rpc error: code = Internal desc = failed to prepare order: failed to convert price of "2ZYFJ3GM2N" to EUR',
    'failed to get ads: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp 10.233.8.174:9555: connect: connection refused"',
    'failed to get ads: rpc error: code = DeadlineExceeded desc = context deadline exceeded',
    "failed to add to cart: rpc error: code = FailedPrecondition desc = Can't access cart storage.",
    'could not retrieve product: rpc error: code = Unknown desc = Error 9005 (HY000): Region is unavailable',
    'failed to get product recommendations: failed to get recommended product info (#1YMWWN1N4O): rpc error: code = Unknown desc = Error 9005 (HY000): Region is unavailable',
    "failed to add to cart: rpc error: code = FailedPrecondition desc = Can't access cart storage.",
    'failed to complete the order: rpc error: code = Internal desc = failed to prepare order: failed to get product #"OLJCESPC7Z"',
    'could not retrieve product: rpc error: code = Unknown desc = Error 9001 (HY000): PD server timeout: start timestamp may fall behind safe point',
    'failed to get product recommendations: failed to get recommended product info (#7JSJCESPC9H): rpc error: code = Unknown desc = Error 9001 (HY000): PD server timeout: start timestamp may fall behind safe point',
    'failed to complete the order: rpc error: code = Internal desc = cart failure: failed to get user cart during checkout: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp: lookup cartservice on 10.233.33.139:53: server misbehaving"',
    "could not retrieve cart: rpc error: code = FailedPrecondition desc = Can't access cart storage. StackExchange.Redis.RedisTimeoutException: Timeout awaiting response (outbound=0KiB, inbound=0KiB, 5575ms elapsed, timeout is 5000ms), command=HGET, next: HGET 1dc31007-9289-45cb-a5f2-5953d34d1543, inst: 0, qu: 0, qs: 2, aw: False, bw: Inactive, rs: ReadAsync, ws: Idle, in: 0, in-pipe: 0, out-pipe: 0, last-in: 349, cur-in: 0, sync-ops: 2, async-ops: 1620, serverEndpoint: redis-cart:6379, conn-sec: 2785.32, aoc: 1, mc: 1/1/0, mgr: 10 of 10 available, clientName: cartservice-2(SE.Redis-v2.6.122.38350), IOCP: (Busy=0,Free=1000,Min=1,Max=1000), WORKER: (Busy=1,Free=32766,Min=1,Max=32767), POOL: (Threads=4,QueuedItems=0,CompletedItems=43675,Timers=4), v: 2.6.122.38350"
]

config = TemplateMinerConfig()
config.load("exp/template/drain3_log.ini")
persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence, config)
split_pattern = re.compile(
    r'(?=(?:failed to|could not)\s+\w+)'        # RPC 头
    , re.IGNORECASE
)

templates = {}
for log_line in logs:
    # result = template_miner.add_log_message(log_line)
    # template = result["template_mined"]
    # if template:
    #     templates[template] = templates.get(template, 0) + 1
    parts = [p.strip() for p in split_pattern.split(log_line) if p.strip()]
    print("Original log:")
    print(log_line)
    print("Split into:")
    for part in parts:
        print("  •", part)
    print()

# # 打印排序后的模板
# for i, (tmpl, count) in enumerate(sorted(templates.items(), key=lambda x: -x[1])):
#     print(f"[{count} occurrences] {tmpl}")