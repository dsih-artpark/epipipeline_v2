import pandas as pd
import re


def search_header(L: list, pivot_col_name) -> bool:
    """This function identifies the header row in a dataframe

    Args:
        L (list): Current list of Dataframe headers
        pivot_col_name (str): Column name) is that a stable header

    Returns:
        bool: Whether header was identified
    """
    assert isinstance(L,list) and isinstance(pivot_col_name,str), "Invalid input"

    import re

    header_search=True
    pivot_col_name=pivot_col_name.lstrip().rstrip()
    for column in L:
        if re.search(pivot_col_name, str(column), re.IGNORECASE):
            header_search=False
            break
    return header_search

def set_headers(df: pd.DataFrame, pivot_column: str, col_start_index: int, col_start_value):
    """_summary_

    Args:
        df (pd.DataFrame): DataFrame
        pivot_column (str): Name of the Stable Column used to identify the header
        col_start_index (int): Index of the column used to identify the dataframe start row (start at 0)
        col_start_value (_type_): Str/Int value of col_start_index column to indicate start of dataframe
    """
    assert isinstance(df, pd.DataFrame) and isinstance(pivot_column, str) and isinstance(col_start_index, int) and col_start_index in range(0,len(df)+1), "Invalid input"

# sets the dataframe's row start
    i=0
    while search_header(list(df.columns), pivot_column) and (i<6):  # change pivot column here, if needed
        df.columns=df.iloc[i,:]
        i+=1
    df.drop(axis=0, index=[n for n in range(i)], inplace=True)

# forward fills for nan columns after the correct columns are identified
    for i in range(1,len(df.columns)):
        if (re.search("Unnamed", str(df.columns[i]), re.IGNORECASE)) or (re.search("NaN", str(df.columns[i]), re.IGNORECASE)):
            df.columns.values[i]=df.columns.values[i-1]

# identify where data starts based on a column and value input - e.g., S.No. is a digit
    start_index=df[df.iloc[:,col_start_index]==col_start_value].index[0] - 1 

# upward fills merged columns after the correct columns are identified
    for row in range(start_index):
        row_data=df.iloc[i].to_list()
        for i in range(len(row_data)):
            if not re.search("nan",str(row_data[i]), re.IGNORECASE):
                 merge_col=re.sub(r"[\,\.\-\d\(\)\s\*\-\_]+", "", str(row_data[i])).lower()
                 df.columns.values[i]=df.columns.values[i]+merge_col
    
# drops headers
    df.drop(axis=0, index=[i for i in range(1, start_index+1)], inplace=True)
            
    return (df)

