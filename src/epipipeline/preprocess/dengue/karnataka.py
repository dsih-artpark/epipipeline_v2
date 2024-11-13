import logging
import os
import re
import warnings
from typing import (Optional, Union)

import numpy as np
import pandas as pd
from dataio.download import download_dataset_v2

from epipipeline.preprocess import (clean_colname, map_column)
import logging
import datetime


# Set up logging
logger = logging.getLogger("epipipeline.preprocess.dengue.karnataka")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)

def fetch_ka_linelist_v2(*, dsid: str,
                        sheet_codes: dict,
                        regionIDs_dict: dict,
                        year: int,
                        expected_files: Optional[list] = None,
                        datadir: str = "data") -> dict:
    """
    Fetches and processes Karnataka linelist data files.

    This function downloads a dataset identified by `dsid`, extracts relevant
    Excel files containing data for the specified `year`, and processes the
    sheets into a dictionary for further analysis. It also verifies if all
    expected files are downloaded and logs information for missing districts or
    discrepancies in the expected files.

    Arguments:
        dsid (str): The dataset identifier.
        sheet_codes (dict): A dictionary mapping sheet codes to district IDs.
        regionIDs_dict (dict): A dictionary containing region metadata including region names.
        year (int): The year for which the data is being fetched.

    Keyword Arguments:
        expected_files (list, optional): A list of filenames expected to be present. Defaults to None.
        datadir (str, optional): The directory where data files are stored. Defaults to "data".

    Raises:
        FileNotFoundError: If the files fail to download to the correct directory.
        ValueError: If multiple folders are found for the same `dsid`.

    Returns:
        dict: A dictionary with processed data from the Excel sheets.
    """

    # Download the dataset for the specified year
    logger.info(f"Downloading dataset with dsid: {dsid} for year: {year}")
    download_dataset_v2(dsid=dsid, data_state="raw", contains_any=str(year))

    # Identify the directory where the raw data is stored
    raw_data_dir = [os.path.join(datadir, name)
                    for name in os.listdir(datadir)
                    if os.path.isdir(os.path.join(datadir, name)) and name.startswith(dsid)]

    # Check if the raw data directory exists and is unique
    if len(raw_data_dir) == 0:
        logger.warning("Files failed to download to the correct directory.")
        raise FileNotFoundError("Files failed to download to the correct directory.")
    elif len(raw_data_dir) > 1:
        logger.warning("Multiple folders found for the same dsid.")
        raise ValueError("Multiple folders found for the same dsid.")
    else:
        raw_data_dir = raw_data_dir[0]  # Extract the single directory path
        logger.info(f"Data directory identified: {raw_data_dir}")

    raw_data_dict = dict()  # Initialize dictionary to store processed data
    files_recieved = []  # List to track the received files

    # Walk through the directory to find Excel files
    logger.info("Processing files in the data directory")
    for root, _dirs, files in os.walk(raw_data_dir):
        for file in files:
            # Process only .xlsx files that contain the specified year in their name
            if file.endswith(".xlsx") and str(year) in file:
                file_path = os.path.join(root, file)
                files_recieved.append(str(file_path).split("/")[-1])  # Add filename to the list
                xlsx = pd.ExcelFile(file_path)  # Load the Excel file

                # Process each sheet in the Excel file
                for sheet in xlsx.sheet_names:
                    if "DEN" in sheet:  # Only process sheets with "DEN" in their name
                        # Extract the code from the sheet name
                        code = re.sub('[^A-Za-z]+', '', sheet.replace("DEN", ""))
                        if code in sheet_codes.keys():
                            # Map the code to the corresponding district and read the sheet into the dictionary
                            raw_data_dict[sheet_codes[code]] = pd.read_excel(xlsx, sheet, header=None)
                            logger.info(f"Fetched sheet: {sheet}, Code: {sheet_codes[code]},{regionIDs_dict[sheet_codes[code]]['regionName']}") # noqa: E501

    # Check if the received files match the expected files
    if expected_files is not None:
        if set(files_recieved) != set(expected_files):
            logger.warning("Expected file list not matching files found on server")

    # Identify missing districts
    missing_districts = set(sheet_codes.values()).difference(raw_data_dict.keys())

    if len(missing_districts) != 0:
        # Log information for each missing district
        for district in missing_districts:
            districtName = regionIDs_dict[district]["regionName"]
            logger.warning(f"District {districtName} ({district}) not present in provided directory.")
    else:
        logger.info("All district tabs present in provided directory.")

    # Return the dictionary of processed data
    return raw_data_dict


def preprocess_ka_linelist_v2(*,
                              raw_data_dict,
                              regionIDs_dict,
                              no_merge_headers,
                              district_specific_errors,
                              standard_mapper,
                              required_headers,
                              ffill_cols_dict) -> dict:
    """
    Preprocess raw district-level data into a standardised format for analysis.

    Args:
        raw_data_dict (dict): Dictionary containing raw data for each district.
        regionIDs_dict (dict): Dictionary mapping district IDs to region information.
        no_merge_headers (dict): Dictionary specifying districts with different header styles.
        district_specific_errors (dict): Dictionary containing header correction mappings for specific districts.
        standard_mapper (dict): Dictionary mapping standard header names to potential variants.
        required_headers (list): List of headers that are required in the final dataset.
        ffill_cols_dict (dict): Dictionary containing list of districts and variables where "" has been used to indicate ffill.

    Returns:
        dict: Preprocessed data dictionary with cleaned and standardised data for each district.

    The function performs the following steps for each district:
        1. Create a copy of the raw data to avoid mutating the original.
        2. Drop empty rows and columns from the data.
        3. Check for empty sheets and skip them if found.
        4. Adjust header rows based on district-specific header styles.
        5. Clean headers by removing special characters and extraneous spaces.
        6. Correct any header errors specific to the district.
        7. Standardise column names to match the accepted headers.
        8. Separate columns for NS1 and IgM test results if specified.
        9. Combine metadata fields for name and address if necessary.
        10. Add district-level metadata.
        11. Check for missing required headers and log warnings if any.
        12. Log the success of the preprocessing for each district and overall.
        13. Ffill values where "" has been used
    """
    preprocessed_data_dict = {}
    all_districts_data_flag = True  # Flag to track if all districts have data

    # Iterate through each district in the raw data
    for districtID in raw_data_dict.keys():
        logger.info(f"Processing district {districtID}")
        districtName = regionIDs_dict[districtID]["regionName"]

        # BASIC CLEANING

        # Create a copy of the raw data to avoid mutating the original dictionary
        df = raw_data_dict[districtID].copy()
        # Drop empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        logger.debug(f"Initial shape of data for district {districtID}: {df.shape}")

        # First check for empty sheet
        if len(df) <= 1:
            warnings.warn(f'District {districtName} ("{districtID}") has no data.', stacklevel=2)
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
            warnings.warn(f'District {districtName} ("{districtID}") has no data.', stacklevel=2)
            all_districts_data_flag = False
            continue

        # Handle districts with different header styles
        if districtID in no_merge_headers.keys():
            if no_merge_headers[districtID] == "merged_ns1_igm_col_header":
                headers = list(df.iloc[0].fillna("igm positive"))
                df = df.iloc[1:].reset_index(drop=True)
                logger.debug(f"{headers} are the headers found for district {districtID} - {districtName}")
            else:
                headers = list(df.iloc[0])
                df = df.iloc[1:].reset_index(drop=True)
        else:
            # If the header is multiple lines due to the NS1 IgM row
            headers = [str(head1) + " " + str(head2)
                       for head1, head2 in zip(df.iloc[0].fillna(""), df.iloc[1].fillna(""))
                       ]
            df = df.iloc[2:].reset_index(drop=True)

        # Clean all headers, remove special characters
        logger.debug(f"Original headers for district {districtID}: {headers}")
        headers=[clean_colname(colname=col) for col in headers]

        # Set cleaned headers to the dataframe
        df.columns = headers
        logger.debug(f"Cleaned headers for district {districtID}: {df.columns}")

        # Correct any header errors specific to the districtID

        if districtID in district_specific_errors.keys():
            header_mapper=map_column(map_dict=district_specific_errors[districtID])
            # Rename columns based on the mapping
            df = df.rename(columns=header_mapper)
            logger.debug(f"Renamed columns for district {districtID} based on specific errors: {header_mapper}")

        # Rename all recognised columns to standard names
        header_mapper=map_column(map_dict=standard_mapper)
        df = df.rename(columns=header_mapper)
        logger.debug(f"Renamed columns for district {districtID} to standard names: {header_mapper}")

        # Handle specific case for Raichur (546) to separate columns for NS1 and IgM
        if districtID in no_merge_headers.keys():
            if no_merge_headers[districtID] == "merged_ns1_igm_cols":
                logger.debug(f"{districtID} doesn't require header merging.")
                logger.debug(f"{districtID} has the following columns: {df.columns.to_list()}")
                results = df["event.test.test1.result"].to_list()

                # Clean the data: strip spaces, lower case
                cleaned_results = [str(x).strip().lower() for x in results]

                # Create the first series
                ns1_results = ["True" if x == 'ns1' else np.nan for x in cleaned_results]
                igm_results = ["True" if x == 'elisa' else np.nan for x in cleaned_results]

                df["event.test.test1.result"] = ns1_results
                df["event.test.test2.result"] = igm_results
                logger.debug(f"Separated NS1 and IgM results for district {districtID}")

        # Ffill for districts and vars where "" has been used
        if districtID in ffill_cols_dict.keys():
            for col in ffill_cols_dict[districtID]:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: pd.NA if '"' in str(x) else str(x))
                    df[col] = df[col].ffill()

        # Combine name and address if needed
        columns = df.columns.to_list()
        if "metadata.nameAddress" not in columns:
            if "metadata.name" in columns and "metadata.address" in columns:
                df['metadata.nameAddress'] = df['metadata.name'].fillna("").astype(str) + " " + df['metadata.address'].fillna("").astype(str)  # noqa: E501
                df = df.drop(columns=["metadata.name", "metadata.address"])
                logger.debug(f"Combined metadata name and address for district {districtID}")
            elif "metadata.address" in columns:
                df["metadata.nameAddress"] = df["metadata.address"]
                df = df.drop(columns=["metadata.address"])
                logger.debug(f"Combined metadata name and address for district {districtID}")
            elif "metadata.name" in columns:
                df["metadata.nameAddress"] = df["metadata.name"]
                df = df.drop(columns = (["metadata.name"]))
                logger.debug(f"Combined metadata name and address for district {districtID}")
            else:
                df["metadata.nameAddress"] = pd.NA
                logger.info(f"Name, address, and nameAddress unavailable for {districtID}. Set to NA")

        # Add district-level metadata
        df["location.admin2.ID"] = districtID
        df["location.admin2.name"] = districtName

        # Check for missing required headers
        absent_headers = [head for head in required_headers if head not in df.columns.to_list()]

        if len(absent_headers) > 0:
            raise Exception(f"District {districtName} ({districtID}) is missing {len(absent_headers)!s} header(s): {', '.join(absent_headers)!s}.")
        else:
            logger.info(f"All headers found for district {districtName} ({districtID})")

        # Add the preprocessed data to the dictionary
        preprocessed_data_dict[districtID] = df

    # Check if all districts have data
    if all_districts_data_flag:
        logger.info("All districts have data.")

    logger.info("Returning preprocessed data")
    return preprocessed_data_dict


def fetch_ka_summary_v2(*, 
    raw_file_name: str,
    raw_folder_prefix: str, 
    raw_dsid: str,
    latest_std_date: Union[str, datetime.datetime], 
    max_date: Union[str, datetime.datetime, None],
    skip_rows: Union[int, None],
    total_row_index: Union[int, None],
    total_col_index_start: Union[int, None],
    total_col_index_end: Union[int, None]) -> dict:
    """

    Extracts relevant sheets from the daily summaries excel file, and carries out basic checks before returning the raw df

    1) Download monthly raw data file (excel workbook)
    2) Extract sheets that start with DDR and are in the date range between the latest standardised file and the latest raw file received
    3) Checks if the sheet is empty (totals for all daily columns are null)
    4) Returns raw data as a dictionary where key = date, and value = df

    Args:
        raw_file_name (str): Name of the raw excel file (with suffix/file extension)
        raw_folder_prefix (str): Name of the raw subfolder/subfolders path within the dsid
        raw_dsid (str): Raw Dataset ID (e.g., EPRDS8)
        latest_std_date (Union[str, datetime.datetime]): Date of the latest standardised daily summary available
        max_date (Union[str, datetime.datetime, None]): Date of the latest raw daily summary received from the govt
        skip_rows (Union[int, None]): Number of rows to skip (updated in metadata.yaml)
        total_row_index (Union[int, None]): Index of the 'total' row (updated in metadata.yaml)
        total_col_index_start (Union[int, None]): Start index of the cols to be checked for a null file (updated in metadata.yaml)
        total_col_index_end (Union[int, None]): End index of the cols to be checked for a null file (updated in metadata.yaml)

    Raises:
        ValueError: Invalid date format for latest_std_date
        ValueError: Invalid date format for max_date

    Returns:
        dict: dictionary where key = date of file, and value = raw df
    """

    # if raw_file_name does not contain suffix, add suffix - by default, raw files are .xlsx
    if not re.search(r"\.", raw_file_name):
        raw_file_name = raw_file_name + ".xlsx"
    
    # convert dates to pd.datetime and check date logic
    try:
        latest_std_date = pd.to_datetime(latest_std_date)
    except Exception as e:
        raise ValueError(f"Invalid date format for latest_std_date: {e}")
    
    if max_date:
        try:
            max_date = pd.to_datetime(max_date)
        except Exception as e:
            raise ValueError(f"Invalid date format for 'max_date': {e}")
    else:
        max_date = pd.to_datetime(datetime.datetime.today()).normalize()

    assert latest_std_date <= max_date, f"max_date {max_date} is not <= latest_std_date {latest_std_date}, no files to process"

    # Download the raw dataset
    folder_path = download_dataset_v2(dsid=raw_dsid, data_state="raw", contains_all=raw_file_name)
    logging.info("Downloaded raw dataset")
    # import the raw dataset
    raw_wb = pd.ExcelFile(f"data/{folder_path}/{raw_folder_prefix}/{raw_file_name}")

    raw_dict = {}

    # iterate through sheets and extract sheets that are between latest_std_date and max_date 
    if not skip_rows:
        skip_rows = 0
    

    for sheet in raw_wb.sheet_names:
        if re.search(r"DDR \d", sheet, re.IGNORECASE):
            date_str = re.search(r"\d{1,2}-\d{1,2}-\d{1,2}", sheet, re.IGNORECASE)
            if date_str:
                date_str = date_str.group(0)
                try:
                    date = pd.to_datetime(date_str, format='%d-%m-%y')
                except Exception as e:
                    logging.warning(f"Invalid date format for sheet name {sheet}. Moving to next sheet.")
                    continue
                
                if (date > latest_std_date) and (date <= max_date):
                    logging.info(f"Sheet {sheet} within date range. Processing..")
                    raw_df = pd.read_excel(f"data/{folder_path}/{raw_folder_prefix}/{raw_file_name}", sheet, skiprows=skip_rows)
                    if total_row_index and total_col_index_start and total_col_index_end:
                        if raw_df.iloc[total_row_index, total_col_index_start: total_col_index_end].eq(0).all():
                            logging.warning(f"Sheet name {sheet} has empty totals. Moving to next sheet.")
                            continue
                        else:
                            raw_dict[str(date.date())] = raw_df
                    else:
                        raw_dict[str(date.date())] = raw_df
                        logging.info(f"Processed {sheet}.")
                else:
                    logging.info(f"Sheet {sheet} not within date range. Moving to next sheet.")
                    continue

            else:
                logging.info(f"Sheet {sheet} does not have a valid date. Moving to next sheet.")
                continue
        else:  
            continue
        
    return raw_dict

