from pendulum import parse

def coerce_numeric_value(record, fields):
    for key, value in record.items():
        if key in fields:
            record[key] = float(value)
    return record

def format_date(date):
    if isinstance(date, str):
        date = parse(date)
        return date.strftime("%Y-%m-%d")
    raise ValueError(f"Not able to parse date, invalid date: {date}")