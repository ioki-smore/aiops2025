import json
import logging
import os
from openai import OpenAI
import re
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)

format = """{
    "analysis": (Your analysis of the code execution result from Executor in the last step, with detailed reasoning of 'what have been done' and 'what can be derived'. Respond 'None' if it is the first step.),
    "completed": ("True" if you believe the issue is resolved, and an answer can be derived in the 'instruction' field. Otherwise "False"),
    "instruction": (Your instruction for the Executor to perform via code execution in the next step. Do not involve complex multi-step instruction. Keep your instruction atomic, with clear request of 'what to do' and 'how to do'. Respond a summary by yourself if you believe the issue is resolved. Respond a summary by yourself if you believe the issue is resolved. Respond a summary by yourself if you believe the issue is resolved.)
}
(DO NOT contain "```json" and "```" tags. DO contain the JSON object with the brackets "{}" only. Use '\\n' instead of an actual newline character to ensure JSON compatibility when you want to insert a line break within a string.)"""


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
        logger.info("JudgeAgent: Initializing JudgeAgent")
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.api_url = api_url or os.getenv("DEEPSEEK_API_URL")
        if not self.api_key or not self.api_url:
            logger.warning("JudgeAgent: DeepSeek-LLM API key or URL not provided. LLM calls will be skipped.")

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
        logger.info(f"JudgeAgent: LLM response: {response}")
        # response = extract_json_from_response(response)
        print(response.get("component", ""))
        output = {
            "uuid": uuid,
            "component": response.get("component", ""),
            "reason": response.get("reason", ""),
            "reasoning_trace": response.get("reasoning_trace", [])
        }
        return output