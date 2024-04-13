import re

def map_columns(colname:str, map_dict: dict) -> str: 
    """This function standardises column names using mapping in config file

    Args:
        colname (str): Current column in DataFrame
        map (dict): Dictionary mapping of preprocessed col names to standardised col names

    Returns:
        str: Standardised column name
    """

    colname=re.sub(r"[^\w\s]","", colname.lstrip().rstrip().lower())
    colname=re.sub(r"(\s+)"," ", colname)
    colname=re.sub(r"\s","_", colname)

    for key, values in map_dict.items():
        if colname in values:
            return key
    return colname
