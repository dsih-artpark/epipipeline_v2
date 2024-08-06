import datetime
import logging
import re
from typing import Optional, Tuple, Union
import pandas as pd
from dateutil.parser import parse

# Set up logging
logger = logging.getLogger("epipipeline.standardise.dengue.karnataka")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)


def extract_symptom_date(*, symptomDate: str, sampleDate: str) -> Tuple[datetime.datetime,datetime.datetime]:
    """If symptom date is in number of days, extracts number and converts to date as sample date - number

    Args:
        symptomDate (str): symptom date as date string/integer
        sampleDate (str): sample/result date as date string
    """

    # make sampleDate into datetime if not already datetime object
    if not isinstance(sampleDate, datetime.datetime):
        try:
            sampleDate = pd.to_datetime(sampleDate)
        except ValueError:
            return (pd.NaT, pd.NaT)

    # if symptom date is not datetime object, looks for int days - nullify symptom date if pattern not found
    if not isinstance(symptomDate, datetime.datetime):
        match = re.search(r".*(\d+)\s?(day)?", str(symptomDate), re.IGNORECASE)
        if match:
            if match.group(1):
                try:
                    symptomDate = sampleDate - pd.to_timedelta(int(match.group(1)), unit='d')
                except ValueError:
                    return (pd.NaT, sampleDate)
            else:
                return (pd.NaT, sampleDate)
        else: # if pattern not found, try to convert it to datetime object
            try:
                symptomDate = pd.to_datetime(symptomDate)
                return (symptomDate, sampleDate)
            except:
                return (pd.NaT, sampleDate)
    return (symptomDate, sampleDate)


def string_clean_dates(*, Date) -> datetime.datetime:
    """Nullifies dates with no number, cleans extraneous elements in dates, and converts to datetime format

    Args:
        Date (str or datetime or NaT): date in dataset

    Returns:
        datetime: date in datetime format
    """

    if not re.search(r"[0-9]", str(Date)):
        return pd.NaT
    
    Date = re.sub(r"\-\-", "-", str(Date))

    formats = ["%d/%m/%y", "%m/%d/%y", "%d/%m/%Y", "%m/%d/%Y"]

    # parse with multiple formats
    for fmt in formats:
        try:
            parsed_date = pd.to_datetime(str(Date), format=fmt)
            return parsed_date
        except ValueError:
            continue
            
    # If formats above don't work, try parsing with mixed formats - if value error, nullify
    try:
        return pd.to_datetime(Date, format="mixed")
    except ValueError:
        return pd.NaT


def fix_year_for_ll(*, Date: datetime.datetime, Year: Optional[Union[datetime.datetime, str]] = None, limitYear: bool = None) -> datetime.datetime:
    """Fixes year to current year/previous year where year is not equal to the current year. 

    Args:
        Date (datetime.datetime): date variable in datetime format
        Year (datetime.datetime, int, str), optional:  year of case
        limitYear (bool): whether to limit to Year inputted or to consider previous year entries

    Returns:
        datetime.datetime: clean date with year fix
    """

    if pd.isna(Date):
        return pd.NaT
    
    try:
        Date = pd.to_datetime(Date).to_pydatetime()
    except AttributeError as e:
        raise(f"{e}. Date entered is invalid")

    if Year:
        Year=str(Year)
        try:
            current_year = pd.to_datetime(Year).year
            assert current_year <= datetime.datetime.today().year, "A post-dated year has been entered. Re-enter a valid year."
        except Exception as e:
            raise(f"{e}. Year entered is invalid.")
    else:
        current_year = datetime.datetime.today().year

    # if year is not current year
    if Date.year!=current_year:
        # set year to current year if limitYear = True or month is not Dec
        if (Date.month != 12) or (limitYear):
            newDate = datetime.datetime(day=Date.day, month=Date.month, year=current_year)
            return newDate
        else: # december entries can be current/previous year
            # year can be previous year
            year_diff = (current_year - Date.year)
            # if post-dated or beyond 1 year prior to the current year, force year to current year
            if (year_diff < 0) or (year_diff > 1):
                newDate = datetime.datetime(day=Date.day, month=Date.month, year=current_year)
                return newDate
            else: # leave previous year's case as is
                return Date
    else:
        return Date


def fix_two_dates(*, earlyDate: datetime.datetime, lateDate: datetime.datetime, minDate: Optional[datetime.datetime] = None, tagDate: Optional[datetime.datetime] = None, days_diff: int = 30) -> Tuple[datetime.datetime, datetime.datetime]:
    """Fixes invalid year entries, and attempts to fix logical check on symptom date>=sample date>=result date through date swapping if delta is >=30 or days_diff specified

    Args:
        earlyDate (datetime): First date in sequence (symptom date or sample date)
        lateDate (datetime): Second date in sequence (sample date or result date)
        tagDate (datetime), optional: Ceiling date defaults to current date, only swap if date <=tagDate
        minDate (datetime), optional: Floor date defaults to None, only swap if date >= minDate

    Returns:
        tuple: If logical errors can be fixed, returns updated date(s). Else, returns original dates.
    """

    # if any of the dates is na, return dates as is
    if pd.isna(earlyDate) or pd.isna(lateDate):
        return (earlyDate, lateDate)
    
    try:
        lateDate = pd.to_datetime(lateDate).to_pydatetime()
        earlyDate = pd.to_datetime(earlyDate).to_pydatetime()
    except AttributeError as e:
        raise (f"{e}. Date entered is invalid.")

    # Fix dates
    # Check tag date format if provided
    if tagDate:
        try:
            tagDate = pd.to_datetime(tagDate)
        except Exception as e:
            (f"{e}. Date entered is invalid.")

    else:
        tagDate = datetime.datetime.today()

    # Check min date
    if minDate:
        try:
            minDate = pd.to_datetime(minDate)
        except Exception as e:
            (f"{e}. Date entered is invalid.")

    delta = lateDate - earlyDate

    # if diff between second and first date is >15 or <0, attempt to fix dates
    if (pd.Timedelta(days_diff, "d") < delta) | (delta < pd.Timedelta(0, "d")):
        # if day of second date=month of first date and day is in month-range, try swapping it's day and month
        # e.g. 2023-02-05, 2023-06-02
        if (lateDate.day == earlyDate.month) & (lateDate.day in range(1, 13)):
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            if (newLateDate <= tagDate) & (pd.Timedelta(0, "d") <= newLateDate - earlyDate <= pd.Timedelta(60, "d")):
                if minDate:
                    if newLateDate>=minDate:
                        return (earlyDate, newLateDate)
                    else:
                        pass # skip to next condition
                return (earlyDate, newLateDate)
            else:
                pass # skip to next condition
        # if day of first date=month of second date and day is in month-range, try swapping it's day and month
        # e.g. 2023-06-02, 2023-02-05
        if (earlyDate.day == lateDate.month) & (earlyDate.day in range(1, 13)):
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            if (newEarlyDate <= tagDate) & (pd.Timedelta(0, "d") <= lateDate - newEarlyDate <= pd.Timedelta(60, "d")):
                if minDate:
                    if newEarlyDate>=minDate:
                        return(newEarlyDate, lateDate)
                    else:
                        pass # skip to next condition
                return (newEarlyDate, lateDate)
            else:
                pass # skip to next condition
        # if both dates have the same day and different month, try swapping day and month for both dates
        # e.g. 2023-08-02, 2023-11-02
        if (earlyDate.day == lateDate.day) & (earlyDate.day in range(1, 13)):
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            if (newEarlyDate <= tagDate) & (newLateDate <= tagDate) & (pd.Timedelta(0, "d") <= newLateDate - newEarlyDate <= pd.Timedelta(60, "d")):  # noqa: E501
                if minDate:
                    if (newEarlyDate >= minDate) & (newLateDate >= minDate):
                        return (newEarlyDate, newLateDate)
                    else:
                        pass # skip to next condition
                return (newEarlyDate, newLateDate)
            else:
                pass # skip to next condition
        # if difference between day of second date and month of first date is 1, try swapping day and month for second date
        # e.g. 2023-08-27, 2023-06-09
        if (lateDate.day-earlyDate.month == 1) & (lateDate.day in range(1, 13)):
            newLateDate = datetime.datetime(
                day=lateDate.month, month=lateDate.day, year=lateDate.year)
            if (newLateDate <= tagDate) & (pd.Timedelta(0, "d") <= newLateDate-earlyDate <= pd.Timedelta(60, "d")):
                if minDate:
                    if newLateDate >= minDate:
                        return (earlyDate, newLateDate)
                    else:
                        pass # skip to next condition
                return (earlyDate, newLateDate)
            else:
                pass # skip to next condition
        # if difference between day of first date and month of second date is -1, try swapping day and month for first date
        # e.g., 2023-10-07, 2023-08-09
        if (earlyDate.day-lateDate.month == -1):  # standalone fix to sample date
            newEarlyDate = datetime.datetime(
                day=earlyDate.month, month=earlyDate.day, year=earlyDate.year)
            if (newEarlyDate <= tagDate) & (pd.Timedelta(0, "d") <= lateDate-newEarlyDate <= pd.Timedelta(60, "d")):
                if minDate:
                    if newEarlyDate >= minDate:
                        return (newEarlyDate, lateDate)
                    else:
                        pass # skip to next condition
                return (newEarlyDate, lateDate)
            else:
                pass # skip to next condition
    else:
        # returns original dates if dates meet logical conditions
        return (earlyDate, lateDate)

    # returns original dates if dates cannot be fixed
    return (earlyDate, lateDate)


def check_date_bounds(*, Date: datetime.datetime, tagDate: Optional[datetime.datetime] = None, minDate: Optional[datetime.datetime] = None, districtName: Optional[str] = None) -> datetime.datetime:
    """Nullifies dates that are less than min date provided and greater than max date provided/current date

    Args:
        Date (datetime): Date variable (symptom date, sample date or result date)
        tagDate (datetime), optional: Date of file. Defaults to None and uses current date if not specified.
        minDate (datetime), optional: Min date. Defaults to none and does not check for min bound if none
        districtName, optional: District Name to print for logger
        districtID, optional: District ID to print for logger

    Returns:
        datetime: pd.NaT if date is > current date or file date or if date < min date, else returns original date
    """

    if pd.isna(Date):
        return pd.NaT
    
    try:
        Date = pd.to_datetime(Date).to_pydatetime()
    except AttributeError as e:
        raise(f"{e}. Date entered is invalid")

    if tagDate:
        try:
            tagDate = pd.to_datetime(tagDate)
        except Exception as e:
            raise(f"{e}. Date entered is invalid")
    else:
        tagDate = datetime.datetime.today()
    
    if minDate: # check lower bound
        try:
            minDate = pd.to_datetime(minDate)
        except Exception as e:
            raise(f"{e}. Date entered is invalid")

        if Date < minDate:
            if districtName:
                logger.warning(f"Found a date {Date} lesser than minDate in {districtName}. Removing...")
            else:
                logger.warning(f"Found a date {Date} lesser than today. Removing...")
            return pd.NaT

    if Date > tagDate: # check upper bound
        if districtName:
            logger.warning(f"Found a date {Date} greater than today in {districtName}. Removing...")
        else:
            logger.warning(f"Found a date {Date} greater than today. Removing...")
        return pd.NaT
    
    return Date
