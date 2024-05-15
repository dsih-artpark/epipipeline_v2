import logging
import os
import re
from typing import Optional

import numpy as np
import pandas as pd
from dataio.download import download_dataset_v2

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
                              required_headers,
                              verbose=False):

    error = []
    preprocessed_data_dict = {}

    # Iterate through each district in the raw data
    for districtID in raw_data_dict.keys():

        districtName = regionIDs_dict[districtID]["regionName"]

        # BASIC CLEANING

        df = raw_data_dict[districtID].copy()  # Create a copy - you don't want to mutate the original dictionary
        df = df.dropna(how='all').dropna(axis=1, how='all') # Drop empty rows and columns

        # First Check for empty sheet
        if len(df) <= 1:
            e = "District " + districtName + " (" + districtID + ") has no data."
            error.append(e)
            if verbose:
                print(e)
            continue

        # To account for empty excel sheets with one lone value in the 10000th row
        # Placing a semi-arbitrary cap of 5:
        # Any more than 5 rows with less than 12 values disqualifies the sheet
        # This also drops rows that have only cell in them filled at the start, like headings
        min_cols = 12
        k = 0
        while (df.iloc[0].notnull().sum() < min_cols) and (k < 5):
            df = df.iloc[1:].reset_index(drop=True)
            k = k + 1

        if k == 5:
            e = "District " + districtName + " (" + districtID + ") has no data."
            error.append(e)
            if verbose:
                print(e)
            continue

        # Some districts have different header styles.
        # This includes a single row header, and other smaller incongruities.
        if districtID in no_merge_headers.keys():
            if no_merge_headers[districtID] == "merged_ns1_igm_col_headers":
                headers = list(df.iloc[0].fillna("igm positive"))
                df = df.iloc[1:].reset_index(drop=True)
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
        for i in range(len(headers)):

            head = str(headers[i]).lower()

            # Removing special characters
            head = head.replace("\n", " ")
            head = head.replace("/", " ")

            # Remove extraneous spaces
            head = re.sub(' +', ' ', head)
            head = head.strip()

            headers[i] = head

        df.columns = headers

        # Correct any header errors specific to the districtID
        if districtID in district_specific_errors.keys():

            header_mapper = {}
            for standard_name, name_options in district_specific_errors[districtID].items():
                for option in name_options:
                    header_mapper[option] = standard_name

            df = df.rename(columns=header_mapper)

        # Rename all recognised columns to standard names

        header_mapper = {}
        for standard_name, name_options in standard_mapper.items():
            for option in name_options:
                header_mapper[option] = standard_name

        df = df.rename(columns=header_mapper)

        # for raichur (546), separating columns for ns1 and igm
        if districtID in no_merge_headers.keys():
            if no_merge_headers[districtID] == "merged_ns1_igm_cols":
                results = df["event.test.test1.result"].to_list()

                # Clean the data: strip spaces, lower case
                cleaned_results = [str(x).strip().lower() for x in results]

                # Create the first series
                ns1_results = ["True" if x == 'ns1' else np.nan for x in cleaned_results]
                igm_results = ["True" if x == 'elisa' else np.nan for x in cleaned_results]

                df["event.test.test1.result"] = ns1_results
                df["event.test.test1.result"] = igm_results

        columns = df.columns.to_list()
        if "metadata.nameAddress" not in columns:
            if "metadata.name" in columns and "metadata.address" in columns:
                df['metadata.nameAddress'] = df['metadata.name'].fillna("").astype(str) + " " + df['metadata.address'].fillna("").astype(str) # noqa: E501
                df = df.drop(["metadata.name", "metadata.address"])
            elif "metadata.address" in columns:
                df["metadata.nameAddress"] = df["metadata.address"]
                df = df.drop(["metadata.address"])
            else:
                df["metadata.nameAddress"] = df["metadata.name"]
                df = df.drop(["metadata.name"])


        for field, value in default_values.items():
            df[field] = value

        # Only taking accepted columns, and ordering as per datadictionary
        headers = [head for head in df.columns.to_list() if head in accepted_headers]
        headers = sorted(headers, key=accepted_headers.index)

        df = df[headers]

        absent_headers = [head for head in required_headers if head not in df.columns.to_list()]

        if len(absent_headers) > 0:
            e = "District " + districtName + " (" + districtID + \
                ") is missing " + str(len(absent_headers)) + " header(s): " + \
                ", ".join(absent_headers) + "."
            error.append(e)
            if verbose:
                print(e)

        df["location.district.ID"] = districtID
        preprocessed_data_dict[districtID] = df

    return preprocessed_data_dict, error
