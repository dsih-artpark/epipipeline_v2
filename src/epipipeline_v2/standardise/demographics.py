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
