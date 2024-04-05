def MapColumns(colname:str, map_dict: dict) -> str: 
    """_summary_

    Args:
        colname (str): Current column in DataFrame
        map (dict): Dictionary mapping of preprocessed col names to standardised col names

    Returns:
        str: Standardised column name
    """

    for key, values in map_dict.items():
        if colname in values:
            return key
    return colname