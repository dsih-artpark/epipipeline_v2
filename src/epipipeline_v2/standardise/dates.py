import pandas as pd
from dateutil.parser import parse, ParserError
import datetime


def parse_date(value, dayfirst=True):
    if isinstance(value, pd.Timestamp):
        return value.date()
    elif isinstance(value, datetime.date):
        return value
    elif isinstance(value, str):
        try:
            temp_date = parse(value, dayfirst=dayfirst).date()
            return temp_date
        except ParserError:
            return pd.NaT
    else:
        return pd.NaT
