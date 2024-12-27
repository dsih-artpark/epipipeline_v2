from typing import Optional, Union
from pathlib import Path
import os
import logging
from datetime import datetime
import glob
import zipfile
import re
import pandas as pd
import boto3

# Set up logging
logger = logging.getLogger("epipipeline.upload.dengue.karnataka")
logging.captureWarnings(True)


def unzip_folder(*, base_dir: Optional[str] = None, zip_folder_name: Optional[str] = None):
    """
    Unzips a specified ZIP folder or the latest ZIP folder in a given directory.

    Parameters:
        base_dir (str): The base directory to look for ZIP files. Defaults to the current working directory.
        zip_folder_name (str): The name of the ZIP folder to unzip. Defaults to the latest ZIP file in the directory.

    Raises:
        ValueError: If the specified directory or ZIP file does not exist.

    Returns:
        output_dir (str): Name of the unzipped directory
    """
    # Validate or set the base directory to cwd
    if base_dir:
        if not os.path.isdir(base_dir):
            logging.warning(f"Directory provided as input: {base_dir} does not exist")
            raise ValueError(f"Directory provided as input: {base_dir} does not exist.")
        base_dir = base_dir.rstrip("/")  # Remove trailing slash if present
    else:
        logging.debug("Defaulting to current directory.")
        base_dir = os.getcwd()
    
    # Validate the ZIP folder name
    if zip_folder_name:
        # Add .zip extension if not already present
        if not zip_folder_name.endswith(".zip"):
            zip_folder_name += ".zip"
        zip_folder_path = os.path.join(base_dir, zip_folder_name)

        if not os.path.isfile(zip_folder_path):
            logging.warning(f"ZIP file {zip_folder_name} does not exist in {base_dir}.")
            raise ValueError(f"ZIP file {zip_folder_name} does not exist in {base_dir}.")
    else:
        # Default to the latest ZIP file in the directory
        logging.debug(f"Defaulting to the latest ZIP folder in {base_dir}.")
        all_zip_folders = sorted(
            glob.glob(os.path.join(base_dir, "*.zip")), key=os.path.getmtime, reverse=True
        )
        if all_zip_folders:
            zip_folder_path = all_zip_folders[0]
        else:
            logging.warning(f"No ZIP folders found in {base_dir}.")
            raise ValueError(f"No ZIP folders found in {base_dir}.")

    # Unzipping the folder
    output_dir = os.path.join(base_dir, os.path.splitext(os.path.basename(zip_folder_path))[0])
    try:
        with zipfile.ZipFile(zip_folder_path, 'r') as zip_ref:
            logging.info(f"Extracting {zip_folder_path} to {output_dir}.")
            zip_ref.extractall(output_dir)
        logging.info(f"Extraction complete. Files are available in {output_dir}.")
    except Exception as e:
        logging.error(f"Failed to unzip {zip_folder_path}: {e}")
        raise 

    return output_dir


def rename_ka_dengue(*, directory: str, file_date: Optional[Union[str, pd.Timestamp]] = None):
    """
    Renames dengue files in the given directory based on specific patterns and the provided date.

    Parameters:
        directory (str): Path to the directory containing files to rename.
        file_date (str or pd.Timestamp, optional): Date for naming the files. Defaults to the current date.

    Raises:
        ValueError: If the directory does not exist or an invalid date is provided.

    Returns:
        directory (str)": Path to the directory containing files to rename.
    """
    # Validate directory
    directory = Path(directory)
    if not directory.is_dir():
        logging.warning(f"Directory provided as input: {directory} does not exist.")
        raise ValueError(f"Directory provided as input: {directory} does not exist.")

    # Validate or set file_date
    if file_date:
        try:
            file_date = pd.to_datetime(file_date)
        except Exception:
            raise ValueError("Invalid date entered.")
    else:  # Default to current date
        logging.warning("Date not provided. Defaulting to current date.")
        file_date = pd.Timestamp(datetime.now())

    year = file_date.year
    month = file_date.month

    # Retrieve all Excel files
    files = list(directory.glob("*.xlsx"))
    if len(files) != 5:
        logging.warning(f"Folder contains fewer than 5 files.")

    for file in files:
        file_name = file.name
        new_name = None

        # Match and rename files based on regex patterns
        if re.match(r"^A-1", file_name, re.IGNORECASE):
            new_name = f"{year}A1.xlsx"
        elif re.match(r"^B-1", file_name, re.IGNORECASE):
            new_name = f"{year}B1.xlsx"
        elif re.match(r"^A-2", file_name, re.IGNORECASE):
            new_name = f"{year}A2.xlsx"
        elif re.match(r"^B-2", file_name, re.IGNORECASE):
            new_name = f"{year}B2.xlsx"
        elif re.match(r"^[0-9A-Za-z]", file_name):
            new_name = f"{year}-{month:02d}.xlsx"

        # Rename file if it matches any of the regex patterns
        if new_name:
            new_path = directory / new_name
            file.rename(new_path)
            logging.info(f"Renamed '{file_name}' to '{new_name}'.")
        else:
            logging.warning(f"Skipping file '{file_name}' - does not match any pattern.")
        
    logging.debug("Renaming completed")

    return directory

def upload_ka_dengue(*, directory: str, file_date: Optional[Union[str, pd.Timestamp]] = None, remove_file: Optional[bool] = False):

   """
    Upload and tag dengue LL & Summary files in the directory provided to AWS S3 

    Args:
        directory (str): 
            The path to the directory containing the Excel files to be uploaded.
        file_date (Optional[Union[str, pd.Timestamp]]): 
            The date used for tagging the uploaded files and validating the year suffix.
            If not provided, defaults to the current date. Accepts a string in a valid date format
            or a `pd.Timestamp` object.
        remove_file (Optional[bool]): 
            Whether to remove files from the local directory after a successful upload and tagging.
            Defaults to `False`.

    Returns:
        missing_files(list): with files that were missing/failed to upload

    Raises:
        ValueError: If the directory does not exist, contains no Excel files
        ValueError: If the provided `file_date` is in an invalid format.

    Notes:
        - S3 bucket and key prefixes are currently hard-coded; these should be updated or
          parameterized as needed.
        - Ensure AWS credentials are configured in your environment before running the function.
    """

    # convert dir to path obj
    directory_path = Path(directory)

    # Validate directory
    if not directory_path.is_dir():
        logging.warning(f"Directory provided as input: {directory} does not exist.")
        raise ValueError(f"Directory provided as input: {directory} does not exist.")

    # Get list of Excel files
    files = list(directory_path.glob("*.xlsx"))
    if not files:
        logging.warning("No Excel files found in the provided directory.")
        raise ValueError("No Excel files found in the provided directory.")

    # Validate or set file_date
    if file_date:
        try:
            file_date = pd.to_datetime(file_date)
        except Exception:
            raise ValueError("Invalid date entered.")
    else:
        logging.warning("Date not provided. Defaulting to current date.")
        file_date = pd.Timestamp(datetime.now())

    # Validate that exactly 5 files are present with specific suffixes
    expected_files = ["A1", "A2", "B1", "B2"] + [str(file_date.year)]

    # Filter files that match the required suffixes (check file names)
    matching_files = [file for file in files if any(file.name.endswith(suffix) for suffix in expected_files)]

    # Extract suffixes of the matched files (get the part of the filename before the year)
    matched_suffixes = {file.name.split('_')[1] for file in matching_files}

    # Find missing files by checking if each expected suffix is present
    missing_files = set(expected_files) - matched_suffixes

    # Log a warning if there are missing files
    if len(missing_files) > 0:
        logging.warning(f"Missing files: {', '.join(missing_files)}")

    # S3 config (hard-coded)
    bucket = "dsih-artpark-01-raw-data"
    ll_prefix = "EPRDS7-KA_Dengue_Chikungunya_LL"
    sum_prefix = "EPRDS8-KA_Dengue_Chikungunya_SUM"

    # Upload to S3 using boto3 
    s3 = boto3.client("s3")

    # Upload files to S3 and add tags
    for file in matching_files:
        # Generate the S3 key
        s3_key = f"{ll_prefix}/{year_suffix}/{file.name}"

        try:
            # Upload file to S3
            s3.upload_file(str(file), bucket, s3_key)
            logging.info(f"Uploaded file: {file} to S3 bucket: {bucket} with key: {s3_key}")

            # Add tags to the uploaded file
            tag_key = file_date.strftime("%Y-%m-%d")
            tagging = {"TagSet": [{"Key": tag_key, "Value": ""}]}

            s3.put_object_tagging(Bucket=bucket, Key=s3_key, Tagging=tagging)
            logging.info(f"Tagged S3 object: {s3_key} with tag: {tagging}")
            
            # Remove file after tagging is successful
            if remove_file:
                os.remove(file)

        except Exception as e:
            missing_files.append(file)
            logging.error(f"Failed to upload or tag file: {file}. Error: {e}")

    return missing_files