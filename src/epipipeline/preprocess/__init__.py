import re
import pandas as pd
from typing import Tuple


def clean_colname(*, colname: str) -> str:
    """String clean column names

    Args:
        colname (str): Column name

    Returns:
        str: Clean column name after removing special characters, double spaces and replacing single spaces with _
    """

    colname = str(colname).lower()
    colname = colname.replace("\n", " ")
    colname = re.sub(r'[^a-z0-9]', ' ', colname)
    colname = re.sub(r' {2,}', ' ', colname)
    colname = colname.strip()
    colname = re.sub(r' ', '_', colname)

    return colname


def map_column(*, map_dict: dict) -> dict:
    """Reverses the key-value item pair in a dictionary containing preprocessed headers and their corrresponding
        standardised headers.

    Args:
        map (dict): Dictionary with key = standardised col name, value = preprocessed col names

    Returns:
        dict: Dictionary with key = preprocessed col name, value = standardised col name
    """
    assert isinstance(
        map_dict, dict), "Invalid input type for column name or dictionary"

    col_mapper = {}
    for standard_name, name_options in map_dict.items():
        for option in name_options:
            col_mapper[option] = standard_name

    return col_mapper


def extract_test_method_with_result(*, test_method: str, result: str) -> Tuple[str, str]:
    """Creates separate NS1 and IgM columns with corresponding result if test_method and result variables provided

    Args:
        test_method (str): test method - IgM, NS1 or both
        result (str): whether positive or negative

    Returns:
        tuple: (NS1 result, IgM result)
    """

    if pd.isna(test_method):
        return (pd.NA, pd.NA)

    else:
        test1, test2 = ("", "")

        if re.search(r"NS1", str(test_method), re.IGNORECASE):
            test1 = result
        if re.search(r"IgM", str(test_method), re.IGNORECASE):
            test2 = result
        return (test1, test2)


def extract_test_method_without_result(*, test_method: str) -> Tuple[str, str]:
    """Creates separate NS1 and IgM columns with positive as default result if only test_method is provided

    Args:
        test_method (str): test method - IgM, NS1 or both

    Returns:
        tuple: (NS1 result, IgM result)
    """
    if pd.isna(test_method):
        return (pd.NA, pd.NA)

    else:
        test1, test2 = ("", "")

        if re.search(r"NS1", str(test_method), re.IGNORECASE):
            test1 = "Positive"
        if re.search(r"IgM", str(test_method), re.IGNORECASE):
            test2 = "Positive"
        return (test1, test2)

def extract_contact(*, address: str) -> tuple:
    """Extracts mobile number from the address/name fields and strips the name/address from the mobile number field

    Args:
        address (str): Name & Address or Address field

    Returns:
        tuple: DataFrame series of address & mobile number
    """
    if pd.isna(address):
        return address
    
    mobile_present = re.search(r"(9?1?\d{10})", str(address))

    if (mobile_present):
        mobile_number = mobile_present.group(1)
        address = re.sub(r"9?1?\d{10}", "", str(address))
        return (address, mobile_number)
    else:
        return (address, pd.NA)


def separate_age_gender(*, agegender: str) -> tuple:
    """Extracts age and gender from a slash-separated string

    Args:
        agegender (str): age/gender field

    Returns:
        tuple: age, gender as strings
    """

    if not pd.isna(agegender):
        match = re.search(
            r"([0-9]+[YyMm]?[A-Za-z]*)\/([MmFfGgBbWw]?[A-Za-z]*)", str(agegender))
        if match:
            if match.group(1) and match.group(2):
                return (match.group(1), match.group(2))
            elif match.group(1):
                return (match.group(1), pd.NA)
            else:
                return (match.group(2), pd.NA)
        else:
            return (pd.NA, pd.NA)
    else:
        return (pd.NA, pd.NA)