from pendulum import parse
import datetime

def coerce_numeric_value(record, fields):
    for key, value in record.items():
        if key in fields:
            record[key] = float(value)
    return record

def format_ns_account_for_header(ns_account):
    """Normalize account id for OAuth realm / SOAP token passport (e.g. 123-sb1 -> 123_SB1)."""
    return ns_account.replace("-", "_").upper()

def format_date(date):
    if isinstance(date, str):
        date = parse(date)
    
    if isinstance(date, datetime.datetime):
        return date.strftime("%Y-%m-%d")
    
    raise ValueError(f"Not able to parse date, invalid date: {date}")


def format_date_as_naive_datetime(date):
    """Return a timezone-naive datetime at midnight for the source calendar date."""
    if isinstance(date, str):
        date = parse(date)

    if isinstance(date, datetime.datetime):
        return datetime.datetime(date.year, date.month, date.day)

    raise ValueError(f"Not able to parse date, invalid date: {date}")
