import datetime
import logging
import re
from typing import Optional

import pandas as pd

# Set up logging
logger = logging.getLogger("epipipeline.standardise.dengue.karnataka")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)


def fix_symptom_date(*, symptomDate: str, resultDate: str) -> datetime.datetime:
    """If symptom date is in number of days, extracts number and converts to date as result date - number

    Args:
        symptomDate (str): symptom date as date string/integer
        resultDate (str): result date as date string
    """

    if isinstance(symptomDate, str) and (isinstance(resultDate, str) or isinstance(resultDate, datetime.datetime)):
        match = re.search(r".*(\d+)\s?(days?)", symptomDate, re.IGNORECASE)
        if match:
            if match.group(1):
                try:
                    resultDate = pd.to_datetime(resultDate)
                    symptomDate = resultDate - \
                        pd.to_timedelta(int(match.group(1)), unit='d')
                except ValueError:
                    return (pd.NA, pd.NA)
            else:
                try:
                    resultDate = pd.to_datetime(resultDate)
                    return (pd.NA, resultDate)
                except ValueError:
                    return (pd.NA, pd.NA)
        else:
            return (symptomDate, resultDate)

    return (symptomDate, resultDate)


def string_clean_dates(*, Date) -> datetime:
    """Nullifies dates with no number, cleans extraneous elements in dates, and converts to datetime format

    Args:
        Date (str or datetime or NaT): date in dataset

    Returns:
        datetime: date in datetime format
    """

    if not re.search(r"\d", str(Date)):
        return pd.NA
    else:
        Date = re.sub(r"\-\-", "-", str(Date))
    try:
        Date = pd.to_datetime(Date, format="mixed")
        return Date
    except ValueError:
        return pd.NA


def fix_year(*, Date: datetime.datetime, tagDate: Optional[datetime.datetime] = None) -> datetime.datetime:
    """Fixes year to current year/previous year where year is not equal to the current year. 

    Args:
        Date (datetime.datetime): date variable in datetime format
        tagDate (datetime.datetime), optional:  max date of cases - set to current date by default

    Returns:
        tuple: clean date with year = current/next/previous
    """

    if pd.isna(Date):
        return pd.NaT
    
    assert (isinstance(Date, datetime.datetime)), "Format the dates before applying this function"  # noqa: E501
    
    if tagDate:
        tagDate = pd.to_datetime(tagDate)
        current_year = tagDate.year
    else:
        current_year = datetime.datetime.today().year

    # if first date is not null, and year is not current year
    if Date.year != current_year:
        # set year to current year if month is not Dec
        if Date.month != 12:
            Date = datetime.datetime(day=Date.day, month=Date.month, year=current_year)
        else: # december entries can be current/previous year
            # year can be previous year
            year_diff = (current_year - Date.year)
            # if post-dated, set to current year
            if year_diff < 0:
                Date = datetime.datetime(day=Date.day, month=Date.month, year=current_year)
            # if year is beyond 1 year prior, set to current year
            elif year_diff >1:
                Date = datetime.datetime(day=Date.day, month=Date.month, year=current_year)
            # remaining dates are from dec of previous year, so we retain them
            else:
                pass

    return (Date)


def fix_two_dates(*, earlyDate: datetime.datetime, lateDate: datetime.datetime, tagDate: Optional[datetime.datetime] = None) -> tuple:
    """Fixes invalid year entries, and attempts to fix logical check on symptom date>=sample date>=result date through date swapping

    Args:
        earlyDate (datetime): First date in sequence (symptom date or sample date)
        lateDate (datetime): Second date in sequence (sample date or result date)
        tagDate (datetime), optional: Only swap if date < current date

    Returns:
        tuple: If logical errors can be fixed, returns updated date(s). Else, returns original dates.
    """

    assert (isinstance(lateDate, datetime.datetime) or pd.isna(lateDate)) and (isinstance(earlyDate, datetime.datetime) or pd.isna(earlyDate)), "Format the dates before applying this function"  # noqa: E501

    # Fix dates
    # if any of the dates is na, return dates as is
    if pd.isna(earlyDate) or pd.isna(lateDate):
        return (earlyDate, lateDate)

    # Check tag date format if provided
    if tagDate:
        if not isinstance(tagDate, datetime.datetime):
            tagDate = pd.to_datetime(tagDate)
    else:
        tagDate = datetime.datetime.today()

    delta = lateDate - earlyDate

    # if diff between second and first date is >60 or <0, attempt to fix dates
    if (pd.Timedelta(60, "d") < delta) | (delta < pd.Timedelta(0, "d")):
        # if day of second date=month of first date and day is in month-range, try swapping it's day and month
        # e.g. 2023-02-05, 2023-06-02
        if (lateDate.day == earlyDate.month) & (lateDate.day in range(1, 13)):
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            if (newLateDate <= tagDate) & (pd.Timedelta(0, "d") <= newLateDate - earlyDate <= pd.Timedelta(60, "d")):
                return (earlyDate, newLateDate)
            else:
                pass
        # if day of first date=month of second date and day is in month-range, try swapping it's day and month
        # e.g. 2023-06-02, 2023-02-05
        if (earlyDate.day == lateDate.month) & (earlyDate.day in range(1, 13)):
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            if (newEarlyDate <= tagDate) & (pd.Timedelta(0, "d") <= lateDate - newEarlyDate <= pd.Timedelta(60, "d")):
                return (newEarlyDate, lateDate)
            else:
                pass
        # if both dates have the same day and different month, try swapping day and month for both dates
        # e.g. 2023-08-02, 2023-11-02
        if (earlyDate.day == lateDate.day) & (earlyDate.day in range(1, 13)):
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            if (newEarlyDate <= tagDate) & (newLateDate <= tagDate) & (pd.Timedelta(0, "d") <= newLateDate - newEarlyDate <= pd.Timedelta(60, "d")):  # noqa: E501
                return (newEarlyDate, newLateDate)
            else:
                pass
        # if difference between day of second date and month of first date is 1, try swapping day and month for second date
        # e.g. 2023-08-27, 2023-06-09
        if (lateDate.day-earlyDate.month == 1) & (lateDate.day in range(1, 13)):
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            if (newLateDate <= tagDate) & (pd.Timedelta(0, "d") <= newLateDate-earlyDate <= pd.Timedelta(60, "d")):
                return (earlyDate, newLateDate)
            else:
                pass
        # if difference between day of first date and month of second date is -1, try swapping day and month for first date
        # e.g., 2023-10-07, 2023-08-09
        if (earlyDate.day-lateDate.month == -1):  # standalone fix to sample date
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            if (newEarlyDate <= tagDate) & (pd.Timedelta(0, "d") <= lateDate-newEarlyDate <= pd.Timedelta(60, "d")):
                return (newEarlyDate, lateDate)
            else:
                pass
    else:
        # returns original dates if dates meet logical conditions
        return (earlyDate, lateDate)

    # returns original dates if dates cannot be fixed
    return (earlyDate, lateDate)


def check_date_to_today(*, Date: datetime.datetime, tagDate: Optional[datetime.datetime] = None, districtName: Optional[str] = None,
                        districtID: Optional[str] = None) -> datetime:
    """Nullifies dates that are greater than current date

    Args:
        Date (datetime): Date variable (symptom date, sample date or result date)
        tagDate (datetime), optional: Date of file. Defaults to None and uses current date if not specified.
        districtName, optional: District Name to print for logger
        districtID, optional: District ID to print for logger

    Returns:
        datetime: pd.NaT if date is > current date or file date, else returns original date
    """

    if pd.isna(Date):
        return pd.NaT
    
    if tagDate:
        tagDate=pd.to_datetime(tagDate)
    else:
        tagDate = datetime.datetime.today()

    if Date > tagDate:
        if districtName and districtID:
            logger.warning(f"Found a date greater than today in {districtName} ({districtID}). Removing...")
        return pd.NaT
    else:
        return Date
