import asyncio
import json
import logging
import os
from openai import OpenAI
import re
import json
from typing import Dict, Any
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def extract_json_from_response(content: str) -> Dict[str, Any]:
    match = re.search(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:
        match = re.search(r"(\{.*\})", content, re.DOTALL)
        if match:
            json_str = match.group(1)
        else:
            print("❌ No JSON found in the response.")
            return {}

    json_str = json_str.strip()

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        print("❌ Json parsing failed:", e)
        return {}


class JudgeAgent:
    """
    Integrates signals from MetricAgent, TraceAgent, LogAgent and uses the DeepSeek-LLM to infer root cause.
    """

    def __init__(self, api_key: str | None, api_url: str | None):
        print("JudgeAgent: Initializing JudgeAgent")
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.api_url = api_url or os.getenv("DEEPSEEK_API_URL")
        # if not self.api_key or not self.api_url:
            # print("JudgeAgent: Warning - DeepSeek-LLM API key or URL not provided. LLM calls will be skipped.")

    def analyze(self, uuid: str, description: str, metric_obs: str, trace_obs: str, log_obs: str):
        """
        Combine observations and call LLM to determine root cause.
        Returns a structured result with fields: uuid, component, reason, reasoning_trace.
        """
        print(f"JudgeAgent: Analyzing anomaly {uuid} with description: {description}")
        # Construct the payload for LLM
        prompt = (
            f"Description: {description}\\n"
            f"Metric Observation: {metric_obs}\\n"
            f"Trace Observation: {trace_obs}\\n"
            f"Log Observation: {log_obs}\\n"
            "Based on the above, identify the root cause of the anomaly. "
            "Output a JSON with keys: component, reason, reasoning_trace. "
            "The reasoning_trace should list steps: "
            "(1) QueryMetrics, (2) TraceCheck, (3) LogInspection, "
            "each with the corresponding observation."
            "Please respond **only** with a JSON object, without markdown formatting or extra commentary."
        )
        print(f"JudgeAgent: Sending prompt to LLM: {prompt}")
        client = OpenAI(api_key=self.api_key, base_url=self.api_url)
        response = client.chat.completions.create(
            model="deepseek-r1:671b-0528",
            # model="deepseek-r1:32b",
            messages=[
                {"role": "system", "content": "You are a root cause analysis expert for distributed systems"},
                {"role": "user", "content": prompt}

            ],
            response_format={"type": "json_object"},  # 强制JSON输出
            stream=False,
            temperature=0,       # 控制随机性 (0-2)
            top_p=0.95,             # 多样性控制
            parallel_tool_calls=True,  # 并行调用工具
        )
        # print(response.choices[0].message.model_dump_json(exclude_none=True, exclude_unset=True))
        response = json.loads(json.loads(response.choices[0].message.model_dump_json(exclude_none=True, exclude_unset=True))['content'])
        print(f"JudgeAgent: LLM response: {response}")
        # response = extract_json_from_response(response)
        print(response.get("component", ""))
        output = {
            "uuid": uuid,
            "component": response.get("component", ""),
            "reason": response.get("reason", ""),
            "reasoning_trace": response.get("reasoning_trace", [])
        }
        return output
    
    async def async_analyze_batch(self, inputs: list[Dict[str, str]]) -> list[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(None, self.analyze, item["uuid"], item["description"], item["metric_obs"], item["trace_obs"], item["log_obs"])
            for item in inputs
        ]
        return await asyncio.gather(*tasks)

        # try:
        #     response = re.sub(r'^```json\s*|\s*```$', '', response.strip())
        #     res = json.loads(response)
        #     return (
        #         res.get("reason", ""),
        #         res.get("trace_reason", ""),
        #         res.get("log_reason", "")
        #     )
        # except Exception as e:
        #     print("LLM failed to parse response")
        #     print(f"LLM Response: {response}")
        #     return "error", "LLM failed to parse", "LLM failed to parse"
        
        # headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
        # payload = {"prompt": prompt}

        # # Call the DeepSeek-LLM API
        # if self.api_key and self.api_url:
        #     try:
        #         response = requests.post(self.api_url, headers=headers, json=payload, timeout=10)
        #         response.raise_for_status()
        #         result = response.json()
        #         # Include uuid in the final output
        #         output = {
        #             "uuid": uuid,
        #             "component": result.get("component", ""),
        #             "reason": result.get("reason", ""),
        #             "reasoning_trace": result.get("reasoning_trace", [])
        #         }
        #         return output
        #     except Exception as e:
        #         print(f"JudgeAgent: LLM request failed: {e}")

        # # Fallback if LLM is not used or fails: use the given observations
        # fallback = {
        #     "uuid": uuid,
        #     "component": "Unknown",  # Could attempt heuristics or defaults here
        #     "reason": "Unable to determine root cause automatically.",
        #     "reasoning_trace": [
        #         {"step": 1, "action": "QueryMetrics", "observation": metric_obs},
        #         {"step": 2, "action": "TraceCheck", "observation": trace_obs},
        #         {"step": 3, "action": "LogInspection", "observation": log_obs}
        #     ]
        # }
        # return fallback
