import re

import pandas as pd


def clean_strings(*, s:str)->str:
    """Standardises string entries

    Args:
        s (str): string entries in the raw dataset

    Returns:
        str: null for entries without  a single alphabet, no extraspaces/whitespaces, upper case
    """

    if isinstance(s, str):
        if re.search(r'[A-Za-z]', s):
            s=re.sub(r'[\.\,\-\)\(]',' ', s)
            s=re.sub(r'[^a-zA-Z0-9\s]+', '', s)
            s=re.sub(r'\s+', ' ', s).strip()
            return s.lstrip().rstrip().upper()
    return pd.NA


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


def standardise_gender(*, gender:str)->str:
    """Standardises gender

    Args:
        gender (str): gender entries in the raw dataset

    Returns:
        str: FEMALE, MALE, UNKNOWN
    """

    gender = str(gender).upper().lstrip().rstrip()

    if re.search(r'[fwgFWG]', gender):
        return "FEMALE"
    elif re.search(r'^[mbMB]', gender):
        return 'MALE'
    else:
        return 'UNKNOWN'


def standardise_test_result(*, result:str) -> str:
    """Standardises results to positive or negative

    Args:
        result (str): Result in the raw dataset

    Returns:
        str: Negative, Positive or Unknown
    """
    if isinstance(result, str) or isinstance(result, int):
        if re.search(r"-ve|Neg|Negative|No|0", str(result), re.IGNORECASE):
            return "NEGATIVE"
        elif re.search(r"NS1|IgM|D|Yes|\+ve|Pos|Positive|1", str(result), re.IGNORECASE):
            return "POSITIVE"
    return "UNKNOWN"


def generate_test_count(*, test1:str, test2:str)->int:
    """Generates test count from test result variables

    Args:
        test1 (str): result from test 1 - positive, negative or unknown
        test2 (str): result from test 2 - positive, negative or unknown

    Returns:
        int: number of test results known - 0, 1 or 2
    """

    if test1!="UNKNOWN" and test2!="UNKNOWN":
        return 2
    elif test1!="UNKNOWN" or test2!="UNKNOWN":
        return 1
    else:
        return 0


def opd_ipd(*, s:str) -> str:
    """Standardises entries for IPD or OPD

    Args:
        s (str): IPD/OPD field in the dataset

    Returns:
        str: standardised value for IPD or OPD
    """

    if isinstance(s, str):
        if re.search(r"IPD?", s, re.IGNORECASE):
            return "IPD"
        elif re.search(r"OPD?", s, re.IGNORECASE):
            return "OPD"
        else:
            return pd.NA

def public_private(*, s:str) -> str:
    """Standardises entries for private or public

    Args:
        s (str): Private/Public field in the dataset

    Returns:
        str: standardised value for Private or Public
    """

    if isinstance(s, str):
        if re.search(r"Private|Pvt", s, re.IGNORECASE):
            return "PRIVATE"
        elif re.search(r"Public|Pub|Govt|Government", s, re.IGNORECASE):
            return "PUBLIC"
        else:
            return pd.NA


def active_passive(*, s:str) -> str:
    """Standardises entries for active or passive

    Args:
        s (str): Active/Passive field in the dataset

    Returns:
        str: standardised value for Active or Passive
    """

    if isinstance(s, str):
        if re.search(r"Acti?v?e?|A", s, re.IGNORECASE):
            return "ACTIVE"
        elif re.search(r"Pas?s?i?v?e?|P", s, re.IGNORECASE):
            return "PASSIVE"
        else:
            return pd.NA


def rural_urban(*, s:str) -> str:
    """Standardises entries for rural or urban

    Args:
        s (str): Rural/Urban field in the dataset

    Returns:
        str: standardised value for Rural or Urban
    """

    if isinstance(s, str):
        if re.search(r"Rura?l?|R", s, re.IGNORECASE):
            return "RURAL"
        elif re.search(r"Urba?n?|U", s, re.IGNORECASE):
            return "URBAN"
        else:
            return pd.NA
