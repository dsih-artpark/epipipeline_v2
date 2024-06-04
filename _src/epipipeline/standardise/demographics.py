import numpy as np
import re
from fuzzywuzzy import fuzz, process


def standardise_age(age):
    if isinstance(age, str):
        pattern = r'^(\d+\.?\d*) *([ym]?[ |.|,|-]?.*)?$'
        match = re.search(pattern, age)
        if match:
            if match.group(1):
                if re.match(r'^\d{1,3}', match.group(1)):
                    age = match.group(1)
                else:
                    return np.nan
            else:
                return np.nan
            if match.group(2):
                if re.match('^[m|M].*', match.group(2)):
                    return round(float(int(float(age)) / 12), 1)
                else:
                    return float(age)
            return float(age)
        else:
            return np.nan
    elif isinstance(age, int):
        return float(age)
    else:
        return np.nan

def standardise_age2(age) -> float:
    """Converts mixed age entries to a standard float 

    Args:
        age (str/float/int): age specified in the raw dataset

    Returns:
        float: standardised age 
    """
    if isinstance(age, str):
        pattern = r'^(\d+\.?\d*) *([ym]?[ |.|,|-]?.*)?$'
        match = re.search(pattern, age)
        if match:
            if match.group(1):
                if re.match(r'^\d{1,3}', match.group(1)):
                    age = float(match.group(1))
                else:
                    return np.nan
            else:
                return np.nan
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
            return np.nan
    elif isinstance(age, int):
        return float(age)
    elif isinstance(age,float):
        return age
    else:
        return np.nan

def standardise_gender(gender):
    standard_genders = ['MALE', 'FEMALE']
    gender = str(gender).upper()

    if re.search(r'[mfMF]', gender):
        gender = gender
    else:
        return 'UNKNOWN'

    matches = process.extract(gender, standard_genders, scorer=fuzz.token_sort_ratio)  # Fuzzywuzzy
    best_match = max(matches, key=lambda x: x[1])

    return best_match[0]


def standardise_gender2(gender:str)->str:
    """Converts mixed gender entries to a standard format

    Args:
        gender (str): gender entries in the raw dataset

    Returns:
        str: standardised gender (FEMALE, MALE, UNKNOWN)
    """
    gender = str(gender).upper().lstrip().rstrip()

    if re.search(r'[fwgFWG]', gender):
        gender="FEMALE"
    elif re.search(r'^[mbMB]', gender):
        gender='MALE'
    else:
        return 'UNKNOWN'

    return gender