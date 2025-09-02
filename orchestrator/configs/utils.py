import pandas as pd
import calendar
from datetime import datetime, timedelta
from typing import Optional, List, Dict

def unnest_json(location_data,nested_column: str):
        df = pd.DataFrame(location_data[nested_column])
        for key, value in location_data.items():
            if key != nested_column:
                df[key] = value
        return df

def chunk_date_range_in_months(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, str]]:
    start_date = start_date.strip() if start_date and start_date.strip() else None
    end_date = end_date.strip() if end_date and end_date.strip() else None

    if not any([start_date, end_date]):
        raise ValueError("At least one of start_date or end_date must be provided")

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if start_date is None:
        start_date = today.strftime('%Y-%m-%d')
    if end_date is None:
        end_date = today.strftime('%Y-%m-%d')

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_dt > end_dt:
        raise ValueError("Start date must be before end date")

    chunks = []
    current_dt = start_dt

    while current_dt <= end_dt:
        last_day = calendar.monthrange(current_dt.year, current_dt.month)[1]
        month_end_dt = datetime(current_dt.year, current_dt.month, last_day)
        chunk_end = min(month_end_dt, end_dt)

        chunks.append({
            'start_date': current_dt.strftime('%Y-%m-%d'),
            'end_date': chunk_end.strftime('%Y-%m-%d'),
            'year': current_dt.strftime("%Y"),
            'month': current_dt.strftime("%m")
        })

        # Move to the first day of the next month.
        current_dt = chunk_end + timedelta(days=1)

    return chunks