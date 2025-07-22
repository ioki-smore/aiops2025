import json

def transform_reasoning_trace(data):
    if isinstance(data, dict):
        reasoning_trace = []
        step_num = 1
        
        for key, value in data.items():
            action = f"{key}"
            
            if isinstance(value, list):
                # 处理列表中可能包含字典的情况
                obs_items = []
                for item in value:
                    if isinstance(item, dict):
                        obs_items.append(str(item))
                    else:
                        obs_items.append(str(item))
                obs_text = '; '.join(obs_items)[:20]
            else:
                obs_text = str(value)[:20]
            
            reasoning_trace.append({
                "step": step_num,
                "action": action,
                "observation": obs_text
            })
            step_num += 1
            
        return reasoning_trace
    
    elif isinstance(data, list):
        reasoning_trace = []
        step_num = 1
        
        for item in data:
            if isinstance(item, dict):
                action = item.get('step', 'Unknown')
                
                # 处理observation可能是字典的情况
                observation = item.get('observation', '')
                if isinstance(observation, (dict, list)):
                    obs_text = str(observation)[:20]
                else:
                    obs_text = str(observation)[:20]
                
                reasoning_trace.append({
                    "step": step_num,
                    "action": str(action),
                    "observation": obs_text
                })
                step_num += 1
            else:
                # 处理列表元素不是字典的情况
                reasoning_trace.append({
                    "step": step_num,
                    "action": "Unknown",
                    "observation": str(item)[:20]
                })
                step_num += 1
                
        return reasoning_trace
    
    return []

# 读取输入文件
with open('submission/output.jsonl', 'r') as input_file:
    input_data = [json.loads(line) for line in input_file]

# 转换数据
for item in input_data:
    if "reasoning_trace" in item:
        item["reasoning_trace"] = transform_reasoning_trace(item["reasoning_trace"])

# 写入输出文件
with open('submission/output_transformed.jsonl', 'w') as output_file:
    for item in input_data:
        output_file.write(json.dumps(item) + '\n')