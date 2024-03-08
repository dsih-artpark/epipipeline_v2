import pandas as pd
from dateutil.parser import parse, ParserError
from datetime import date, datetime, timedelta


def parse_date(value, dayfirst=True):
    if isinstance(value, pd.Timestamp):
        return value.date()
    elif isinstance(value, date):
        return value
    elif isinstance(value, str):
        try:
            temp_date = parse(value, dayfirst=dayfirst).date()
            return temp_date
        except ParserError:
            return pd.NaT
    else:
        return pd.NaT


def validate_dates(df, year_of_data, date_columns):
    # Function to convert Excel dates to datetime objects
    def excel_to_datetime(date):
        if isinstance(date, int):
            return datetime(1899, 12, 30) + timedelta(days=date)
        return date

    # Function to handle various date formats and coerce invalid formats to NaT
    def parse_date_v2(date):
        try:
            date = pd.to_datetime(date, errors='coerce')
            date = excel_to_datetime(date)
            if date.year not in [year_of_data, year_of_data - 1, year_of_data + 1] or date > datetime.now():
                return pd.NaT
            return date
        except ParserError:
            return pd.NaT

    # Function to validate the order of dates
    # def validate_order(row):
        # return row[date_columns[0]] <= row[date_columns[1]] <= row[date_columns[2]]

    # Iterate over each row in the DataFrame
    for column in date_columns:
        df[column] = df[column].apply(excel_to_datetime)
        df[column] = df[column].apply(parse_date_v2)

    # Validate the order of dates
    # df['order_valid'] = df.apply(validate_order, axis=1)

    return df
