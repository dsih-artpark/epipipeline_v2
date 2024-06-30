import logging
import os
import re
import warnings
from typing import Optional

import numpy as np
import pandas as pd
from dataio.download import download_dataset_v2
from epipipeline.preprocess import clean_colname, map_column

# Set up logging
logger = logging.getLogger("epipipeline.preprocess.dengue.karnataka")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)

def fetch_ka_linelist_v2(*, dsid: str,
                        sheet_codes: dict,
                        regionIDs_dict: dict,
                        year: int,
                        expected_files: Optional[list] = None,
                        datadir: str = "data"):
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
                              default_values,
                              accepted_headers,
                              required_headers):
    """
    Preprocess raw district-level data into a standardised format for analysis.

    Args:
        raw_data_dict (dict): Dictionary containing raw data for each district.
        regionIDs_dict (dict): Dictionary mapping district IDs to region information.
        no_merge_headers (dict): Dictionary specifying districts with different header styles.
        district_specific_errors (dict): Dictionary containing header correction mappings for specific districts.
        standard_mapper (dict): Dictionary mapping standard header names to potential variants.
        default_values (dict): Dictionary of default values to be set for specific fields.
        accepted_headers (list): List of headers that are accepted in the final dataset.
        required_headers (list): List of headers that are required in the final dataset.

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
        10. Fill default values for specified fields.
        11. Add district-level metadata.
        12. Filter and order columns based on accepted headers.
        13. Check for missing required headers and log warnings if any.
        14. Log the success of the preprocessing for each district and overall.
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
        headers=[clean_colname(colname=col) for col in df.columns]

        # Set cleaned headers to the dataframe
        df.columns = headers
        logger.debug(f"Cleaned headers for district {districtID}: {headers}")

        # Correct any header errors specific to the districtID

        if districtID in district_specific_errors.keys():
            header_mapper=map_column(colame=df.columns.to_list(), map_dict=district_specific_errors[districtID])
            # Rename columns based on the mapping
            df = df.rename(columns=header_mapper)
            logger.debug(f"Renamed columns for district {districtID} based on specific errors: {header_mapper}")

        # Rename all recognised columns to standard names
        header_mapper=map_column(colname=df.columns.to_list(), map_dict=standard_mapper)
        df = df.rename(columns=header_mapper)
        logger.debug(f"Renamed columns for district {districtID} to standard names: {header_mapper}")

        # Handle specific case for Raichur (546) to separate columns for NS1 and IgM
        if districtID in no_merge_headers.keys():
            if no_merge_headers[districtID] == "merged_ns1_igm_cols":
                results = df["event.test.test1.result"].to_list()

                # Clean the data: strip spaces, lower case
                cleaned_results = [str(x).strip().lower() for x in results]

                # Create the first series
                ns1_results = ["True" if x == 'ns1' else np.nan for x in cleaned_results]
                igm_results = ["True" if x == 'elisa' else np.nan for x in cleaned_results]

                df["event.test.test1.result"] = ns1_results
                df["event.test.test2.result"] = igm_results
                logger.debug(f"Separated NS1 and IgM results for district {districtID}")

        # Combine name and address if needed
        columns = df.columns.to_list()
        if "metadata.nameAddress" not in columns:
            if "metadata.name" in columns and "metadata.address" in columns:
                df['metadata.nameAddress'] = df['metadata.name'].fillna("").astype(str) + " " + df['metadata.address'].fillna("").astype(str)  # noqa: E501
                df = df.drop(columns=["metadata.name", "metadata.address"])
            elif "metadata.address" in columns:
                df["metadata.nameAddress"] = df["metadata.address"]
                df = df.drop(columns=["metadata.address"])
            else:
                df["metadata.nameAddress"] = df["metadata.name"]
                df = df.drop(columns(["metadata.name"]))
            logger.debug(f"Combined metadata name and address for district {districtID}")

        # Set default values for specified fields
        for field, value in default_values.items():
            df[field] = value
        logger.debug(f"Set default values for district {districtID}: {default_values}")

        # Add district-level metadata
        df["location.admin2.ID"] = districtID
        df["location.admin2.name"] = districtName

        # Only take accepted columns, and order as per the data dictionary
        headers = [head for head in df.columns.to_list() if head in accepted_headers]
        headers = sorted(headers, key=accepted_headers.index)

        df = df[headers]
        logger.debug(f"Filtered and ordered columns for district {districtID} based on accepted headers")

        # Check for missing required headers
        absent_headers = [head for head in required_headers if head not in df.columns.to_list()]

        if len(absent_headers) > 0:
            warnings.warn(f"District {districtName} ({districtID}) is missing {len(absent_headers)!s} header(s): {', '.join(absent_headers)!s}.", stacklevel=2)  # noqa: E501
        else:
            logger.info(f"All headers found for district {districtName} ({districtID})")

        # Add the preprocessed data to the dictionary
        preprocessed_data_dict[districtID] = df

    # Check if all districts have data
    if all_districts_data_flag:
        logger.info("All districts have data.")

    logger.info("Returning preprocessed data")
    return preprocessed_data_dict

