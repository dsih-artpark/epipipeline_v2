import re
import pandas as pd


def clean_colname(*, colname: str) -> str:
    """string clean column names

    Args:
        colname (str): column name

    Returns:
        str: clean column name after removing special characters, double spaces and replacing single spaces with _
    """

    assert isinstance(colname, str), "Column name must be a string"
    colname = re.sub(r"[^\w\s]", "", colname.strip().lower())
    colname = re.sub(r"(\s+)", " ", colname)
    colname = re.sub(r"\s", "_", colname)

    return colname


def map_column(*, columnname: str, map_dict: dict) -> str:
    """Maps column name to standard column name using mapper provided

    Args:
        colname (str): Current column in DataFrame
        map (dict): Dictionary mapping of preprocessed col names to standardised col names

    Returns:
        str: Standardised column name
    """
    assert isinstance(columnname, str) and isinstance(
        map_dict, dict), "Invalid input type for column name or dictionary"

    for standard_name, name_options in map_dict.items():
        if colname in name_options:
            return standard_name
    return pd.NA

def extract_test_method_with_result(*, test_method: str, result: str) -> tuple:
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


def extract_test_method_without_result(*, test_method: str) -> tuple:
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

