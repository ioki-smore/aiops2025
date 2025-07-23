from datetime import datetime, timedelta, timezone
import re


def parse_time_range(description: str):
    """
    Parse start and end UTC times from the anomaly description.
    Returns (start_time, end_time) as datetime objects.
    """
    # Regular expression to find datetime strings (e.g., 2023-07-10T12:00:00Z)
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'
    matches = re.findall(pattern, description)
    if len(matches) == 2:
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

CST = timezone(timedelta(hours=8))

def utc_to_cst(utc: datetime) -> datetime:
    """
    Convert UTC datetime to CST (China Standard Time).
    """
    # print(utc)
    if utc.tzinfo is None:
        utc = utc.replace(tzinfo=timezone.utc)
    return utc.astimezone(CST)

def daterange(start: datetime, end: datetime):
    start = utc_to_cst(start)
    end = utc_to_cst(end)
    for n in range((end.date() - start.date()).days + 1):
        yield (start.date() + timedelta(n)).strftime("%Y-%m-%d")
