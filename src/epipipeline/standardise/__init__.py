import re
import pandas as pd
from typing import Union, Tuple
import logging

# Set up logging
logger = logging.getLogger("epipipeline.standardise")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)


def clean_strings(*, s:str, case: bool = True, case_type: str = "title") -> str:
    """Standardises string entries

    Args:
        s (str): string entries in the raw dataset

    Returns:
        str: null for entries without  a single alphabet, no extraspaces/whitespaces, upper case
    """
    if pd.isna(s):
        return s
    
    s = str(s)
    s = s.replace("\n"," ")
    s = re.sub(r'[^a-zA-Z0-9]', ' ', s)
    s = re.sub(r' {2,}', ' ', s)
    s = s.strip()

    if case:
        if re.search(r"lower", str(case_type), re.IGNORECASE):
            s = s.lower()
        elif re.search(r"upper", str(case_type), re.IGNORECASE):
            s = s.upper()
        elif re.search(r"title|proper", str(case_type), re.IGNORECASE):
            s = s.title()
        else:
            logger.warning("Case type must be lower, upper or title. Setting to default: title case")
            s = s.title()
    
    if s == '' or s == ' ' or s == 'Nan':
        return pd.NA
    else:
        return s

def standardise_age(*, age:str) -> float:
    """Extracts year and month from string age entries

    Args:
        age (str): age string in the raw data

    Returns:
        float: age rounded to 2 decimal places
    """
    if isinstance(age, str):
        pattern = r'^(\d+\.?\d*) *([ym]?[ |.|,|-]?.*)?$'
        match = re.search(pattern, age)
        if match:
            if match.group(1):
                if re.match(r'^\d{1,3}', match.group(1)):
                    age = float(match.group(1))
                else:
                    return pd.NA
            else:
                return pd.NA
            if match.group(2):
                if re.match('^[m|M].*', match.group(2)):
                    if age<13:
                        return round(age / 12, 2)
                    else:
                        return age
                elif re.match(r'^[y|Y]\D*\d{1,2}[m|M]', match.group(2)):
                    month_match=re.match(r'^[y|Y]\D*(\d{1,2})[m|M]', match.group(2))
                    if month_match.group(1):
                        month=round(float(month_match.group(1))/ 12, 2)
                        age+=month
                        return age
                else:
                    return age
            return age
        else:
            return pd.NA
    elif isinstance(age, int):
        return float(age)
    elif isinstance(age,float):
        return age
    else:
        return pd.NA

def validate_age(*, age: float, upper_limit: float =105) -> float:
    """Validates age range

    Args:
        age: Age (as float/NaT)
        upper_limit(int): Upper limit for age

    Returns:
        float/NaT: <0 Age <106
    """
    if isinstance(age, float):
        if age>0 and age<=upper_limit:
            return age
        elif age>0:
            return age//10
        else:
            return pd.NA
    else:
        return age


def standardise_gender(*, gender:str) -> str:
    """Standardises gender

    Args:
        gender (str): gender entries in the raw dataset

    Returns:
        str: Female, Male, Unknown
    """
    if pd.isna(gender):
        return "Unknown"
    
    if re.search(r'[fwg]', str(gender), re.IGNORECASE):
        return "Female"
    elif re.search(r'^[mb]|hm',str(gender), re.IGNORECASE):
        return "Male"
    else:
        return "Unknown"


def standardise_test_result(*, result:str) -> str:
    """Standardises results to positive or negative

    Args:
        result (str): Result in the raw dataset

    Returns:
        str: Negative, Positive or Unknown
    """
    if pd.isna(result):
        return "Unknown"
    if re.search(r"-ve|Neg|No|\bN\b|0", str(result), re.IGNORECASE):
        return "Negative"
    elif re.search(r"NS1|IgM|\bD\b|Yes|\bY\b|\+ve|Pos|1|Dengue", str(result), re.IGNORECASE):
        return "Positive"
    elif re.search(r"Inconclusive", str(result), re.IGNORECASE):
        return "Inconclusive"
    else:
        return "Unknown"


def generate_test_count(*, test_results: list) -> int:
    """Generates test count from test result variables

    Args:
        test_results: list of results

    Returns:
        int: number of test results known
    """

    if test_results==[]:
        return 0

    count = 0
    for test in test_results:
        if test!="Unknown":
            count+=1
    return count


def opd_ipd(*, s:str) -> str:
    """Standardises entries for IPD or OPD

    Args:
        s (str): IPD/OPD field in the dataset

    Returns:
        str: standardised value for IPD or OPD
    """

    if pd.isna(s):
        return "Unknown"
    
    if re.search(r"IPD?", str(s), re.IGNORECASE):
        return "IPD"
    elif re.search(r"OPD?", str(s), re.IGNORECASE):
            return "OPD"
    else:
        return "Unknown"


def public_private(*, s:str) -> str:
    """Standardises entries for private or public

    Args:
        s (str): Private/Public field in the dataset

    Returns:
        str: standardised value for Private or Public
    """
    if pd.isna(s):
        return "Unknown"
    
    if re.search(r"Private|Pvt", str(s), re.IGNORECASE):
        return "Private"
    elif re.search(r"Pub|Govt|Government", str(s), re.IGNORECASE):
        return "Public"
    else:
        return "Unknown"


def active_passive(*, s:str) -> str:
    """Standardises entries for active or passive

    Args:
        s (str): Active/Passive field in the dataset

    Returns:
        str: standardised value for Active or Passive
    """

    if pd.isna(s):
        return "Unknown"
    
    if re.search(r"Acti?v?e?|\bA\b", str(s), re.IGNORECASE):
        return "Active"
    elif re.search(r"Pas?s?i?v?e?|\bP\b", str(s), re.IGNORECASE):
        return "Passive"
    else:
        return "Unknown"


def rural_urban(*, s:str) -> str:
    """Standardises entries for rural or urban

    Args:
        s (str): Rural/Urban field in the dataset

    Returns:
        str: standardised value for Rural or Urban
    """

    if pd.isna(s):
        return "Unknown"
   
    if re.search(r"Rura?l?|\bR\b", str(s), re.IGNORECASE):
        return "Rural"
    elif re.search(r"Urba?n?|\bU\b", str(s), re.IGNORECASE):
        return "Urban"
    else:
        return "Unknown"

def event_death(*, s:str) -> str:
    """Standardises event death to boolean

    Args:
        s (str): Whether death occured

    Returns:
        bool: True/False or pd.NA
    """
    if pd.isna(s):
        return s
    
    if re.search(r"\bNo\b|\bN\b|0", str(s), re.IGNORECASE) and not re.search(r"travel|history", str(s), re.IGNORECASE):
        return False
    elif re.search(r"Death|\bD\b|Yes|\bY\b|1", str(s), re.IGNORECASE):
        return True
    else:
        return pd.NA

def extract_gender_age(*, gender: str, age: Union[str, float]) -> Tuple[str, str]:
    """Returns gender and age values that are swapped

    Args:
        gender (str): gender field
        age (Union[str, float]): age field

    Returns:
        Tuple[str, str]: gender, age
    """
    if re.search(r"[0-9]", str(gender)) and re.search(r"[^0-9]", str(age)):
        return (age, gender)
    
    return (gender, age)
