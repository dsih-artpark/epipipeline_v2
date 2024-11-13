
import logging
import re
import pandas as pd

from epipipeline.preprocess import (clean_colname, map_column, extract_test_method_with_result)
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
pd.set_option('future.no_silent_downcasting', True)


# Set up logging
logger = logging.getLogger("epipipeline.preprocess.dengue.ihip")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)

def fetch_ihip_v2(*, 
                   json_cred: str, 
                   gsheet: str, 
                   raw_sheets: list) -> dict:
    """Fetches IHIP data (for GoK and BBMP) from Google Sheets

    Args:
        json_cred (str): link to the json file with API credentials
        gsheet (str): name of the Google Sheet
        raw_sheets (list): expected list of sheet names

    Raises:
        NameError: Incorrect Google Sheet name

    Returns:
        dict: Dictionary with keys = District Name and Values = raw dataframe 
    """
    
    # Setting up access to google sheet config
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_cred, scope)
    client = gspread.authorize(creds)

    # Load workbook
    try:
        wb = client.open(gsheet)
    except gspread.exceptions.SpreadsheetNotFound as e:
        logger.warning(f"Unable to locate workbook {gsheet}")
        raise NameError(f"Unable to locate workbook {gsheet}")

    # Extract sheets and read into a dict of dataframes
    raw_data_dict = dict()

    for expected_sheet in raw_sheets:
        try:
            ws = wb.worksheet(expected_sheet)
        except gspread.exceptions.WorksheetNotFound as e:
            logger.warning(f"Unable to locate sheet: {expected_sheet}")
            continue
        try:
            raw_data_dict[expected_sheet] = pd.DataFrame(ws.get_all_records())
        except Exception as e:
            logger.warning(f"Unable to read sheet: {expected_sheet}. Error {e}")

    missing_sheets = set(raw_sheets) - set(raw_data_dict.keys())

    if len(missing_sheets) != 0:
        logger.warning(f"Districts not present in provided directory {missing_sheets}")
    else:
        logger.info("All district sheets present")

    return raw_data_dict

def preprocess_ihip_v2(*, 
                       raw_data_dict: dict,
                       standard_mapper: dict,
                       minimum_columns: list
                       ) -> dict:
    """Preprocesses IHIP data for KA (including BBMP):
    1) Checks for data submitted by each district, and flags districts with no case data
    2) Maps raw data headers to standardised headers
    3) Checks for minimum expected columns and flags missing columns
    4) Separates NS1 and IgM results 
    5) Re-assigns cases to BBMP based on "remarks" provided
    6) Assigns BBMP to Bengaluru Urban district and ULB to BBMP

    Args:
        raw_data_dict (dict): Data dictionary with key = district and value = dataframe for the district, extracted from GSheet
        standard_mapper (dict): Mapping of raw column names to standardised column names
        minimum_columns (list): List of minimum expected columns in the raw dataset

    Returns:
        dict: Preprocessed data with 
    """
    
    preprocessed_data_dict = {}
    all_districts_data_flag = True

    # Iterate through each district in the raw data
    for districtName in raw_data_dict.keys():
        logger.info(f"Processing district {districtName}")

        # Create a copy of the raw data to avoid mutating the original dictionary
        df = raw_data_dict[districtName].copy()

        # Drop empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        logger.debug(f"Initial shape of data for district {districtName}: {df.shape}")

        # First check for empty sheet
        if len(df) <= 1:
            logger.warning(f'District {districtName} has no data.', stacklevel=2)
            all_districts_data_flag = False
            continue

        # To account for empty excel sheets with one lone value in the 10000th row
        # Placing a semi-arbitrary cap of 5:
        # Any more than 5 rows with less than 12 values disqualifies the sheet
        # This also drops rows that have only one cell filled at the start, like headings
        min_cols = 12
        k = 0
        while (df.iloc[0].notnull().sum() < min_cols) and (k < 5):
            df = df.iloc[1:].reset_index(drop=True)
            k = k + 1

        if k == 5:
            logger.warning(f'District {districtName} has no data.', stacklevel=2)
            all_districts_data_flag = False
            continue

        # Clean all headers, remove special characters
        raw_headers = df.columns.to_list()
        logger.debug(f"Original headers for district {districtName}: {raw_headers}")
        clean_headers=[clean_colname(colname=col) for col in raw_headers]

        # Set cleaned headers to the dataframe
        df.columns = clean_headers
        logger.debug(f"Cleaned headers for district {districtName}: {clean_headers}")

        # Rename all recognised columns to standard names
        header_mapper = map_column(map_dict=standard_mapper)
        df = df.rename(columns=header_mapper)
        logger.debug(f"Renamed columns for district {districtName} to standard names: {header_mapper}")

        # extract test results from test cols
        if "test_method" and "result" in df.columns:
            tests = df.apply(lambda x: extract_test_method_with_result(test_method=x["test_method"], result=x["result"]), axis=1)
            df["event.test.test1.result"], df["event.test.test2.result"] = zip(*tests)

        # where BBMP is mentioned in the remarks, change ulb to BBMP
        if "remark" in df.columns:
            df.loc[df["remark"].str.contains("BBMP", re.IGNORECASE)==True, "ulb"]="BBMP"

        # Add district name from sheet, where sheet name is BBMP, change district to Bengaluru Urban
        df["location.admin2.name"] = districtName

        df.loc[df["location.admin2.name"] == "BBMP", "location.admin2.name"] = "Bengaluru Urban"
        df.loc[df["location.admin2.name"] == "BBMP", "ulb"] = "BBMP"

        # check minimum cols present
        min_cols_missing = set(minimum_columns) - set(df.columns)

        if len(min_cols_missing)>1:
            raise Exception(f"Missing columns: District {districtName} is missing minimum required columns {min_cols_missing}")

        # filter empty rows
        df=df.dropna(how="all", axis=0)
        df=df.dropna(how="all", axis=1)

        # Add the preprocessed data to the dictionary
        preprocessed_data_dict[districtName] = df

    # Check if all districts have data
    if all_districts_data_flag:
        logger.info("All districts have data.")

    logger.info("Returning preprocessed data")

    return preprocessed_data_dict