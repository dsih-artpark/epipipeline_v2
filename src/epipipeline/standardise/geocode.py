import pandas as pd
import googlemaps
import logging
import time
from tqdm import tqdm
import subprocess
import geopandas as gpd
from shapely.geometry import Point
import re
from dataio.download import download_dataset_v2
import os


# Set up logging
logger = logging.getLogger("standardise.geocode")
logging.basicConfig(level=logging.INFO)
logging.captureWarnings(True)

# Get API key if stored in a local config file


def get_api_key(*, encrypted_file_path: str = "~/config.enc"):
    """Retrieves API Key stored in an openssl pbkdf2 encrypted file"

    Args:
        encrypted_file_path (str, optional): path to .enc file. Defaults to "~/config.enc".
    """
    MyAPI = None

    encrypted_file = "~/config.enc"

    # Command to decrypt using OpenSSL - You will be prompted to enter the openssl password used for encryption
    command = f"openssl aes-256-cbc -d -pbkdf2 -salt -in {encrypted_file}"

    try:
        MyAPI = subprocess.check_output(command, shell=True, text=True).strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting API Key: {e}")
        raise
    return MyAPI

# Geocoding function


def geocode(*, addresses: pd.Series, batch_size: int = 0, API_key: str):
    """Uses Google Maps API to convert addresses to lat, long positions

    Args:
        addresses (pd.Series): pandas column/series with addresses
        batch_size (int): number of rows to process in a batch, optional
        API_key (str): Google Maps API key from GCP

    Returns:
        pd.Series: pandas Series with lat, long positions
    """

    assert isinstance(
        addresses, pd.Series), "addresses must be a pandas Series"
    assert isinstance(API_key, str) and len(
        API_key) >= 35, "Invalid API key length"
    assert isinstance(
        batch_size, int) and batch_size >= 0, "Batch size must an integer >=0"

    try:
        gmaps = googlemaps.Client(key=API_key)
    except Exception as e:
        logger.error(f"Error initializing Google Maps client: {e}")
        raise

    # Function to geocode a batch of addresses
    def geocode_batch(batch: pd.Series):
        """Processes addresses in batches through Google Maps API

        Args:
            batch (pd.Series): pandas Series of addresses

        Returns:
            pd.Series: pandas Series of lat, long positions
        """
        geocoded_results = []
        for address in tqdm(batch, desc="Geocoding addresses"):
            try:
                geocode_result = gmaps.geocode(address)
                if geocode_result:
                    lat = geocode_result[0]['geometry']['location']['lat']
                    lng = geocode_result[0]['geometry']['location']['lng']
                    geocoded_results.append((lat, lng))
                else:
                    logger.warning(f"No results for address: {address}")
                    geocoded_results.append((pd.NA, pd.NA))
            except Exception as e:
                logger.error(f"Error geocoding {address}: {e}")
                geocoded_results.append((pd.NA, pd.NA))
            time.sleep(0.1)  # Time delay to respect rate limits
        return geocoded_results

    if batch_size:
        # List to store geocoded results from all batches
        all_geocoded_results = []

        # Process the DataFrame in batches
        total_batches = (len(addresses) + batch_size -
                         1) // batch_size  # Total number of batches
        for start in range(0, len(addresses), batch_size):
            end = min(start + batch_size, len(addresses))
            batch = addresses[start:end]

            # Geocode each batch
            geocoded_results = geocode_batch(batch)
            all_geocoded_results.extend(geocoded_results)

            logger.info(
                f"Processed batch {start // batch_size + 1} / {total_batches}")

        return pd.Series(all_geocoded_results)

    else:
        # Geocode all addresses at once if no batch_size is provided
        all_geocoded_results = geocode_batch(addresses)
        return pd.Series(all_geocoded_results)


def check_bounds(*, lat: float, long: float, regionID: str = None, dsid: str = None, local_file_path: str = None):
    """Validates that provided lat, long are within bounds of shape/polygon. Returns pd.NA if outside bound

    Args:
        lat (float): Latitude
        long (float): Longitude
        regionID (str, optional): dsih-artpark regionID/geojson filename if geojson on aws s3. Defaults to None.
        dsid (str, optional): dsid if geojson on aws s3
        local_file_path (str, optional): local file path with .geojson extension if geojson stored locally. Defaults to None.

    Raises:
        TypeError: Lat & Long must be float
        FileNotFoundError: If geojson file is not found
        e: Exceptions

    Returns:
        _type_: lat, long if valid or pd.NA, pd.NA
    """
    
    # Validating lat, long
    if not isinstance(lat, float) or not isinstance(long, float):
        raise TypeError("Latitude and Longitude must be floats")
    
    # Return pd.NA if null, else creating point object to compare with geojson
    if pd.isna(lat) or pd.isna(long):
        return pd.NA, pd.NA
    else:
        point = Point(long, lat)
    
    # Asserting input combinations
    assert (regionID and dsid) or (local_file_path), "Input Error: Provide regionID and dsid or local file path to geojson"
    
    # Validating regionID if provided
    if regionID and dsid:
        assert isinstance(regionID, str), "regionID must be a string"
        assert re.match(r'^(state|district|subdistrict|ulb|village)\_\d{2,6}$', regionID) or \
               re.match(r'^(zone|ward)\_\d{2,6}\-\d{1,2}$', regionID), "Invalid regionID format"
    else:
        assert isinstance(local_file_path, str) and local_file_path.endswith(".geojson"), "Input Error: Local file path must link to a geojson file"

    # Downloading and loading geojson
    if regionID and dsid:
        logger.info(f"Downloading the geojson file for {regionID}, {dsid}")
        
        folder = download_dataset_v2(dsid=dsid, contains_all=regionID, suffixes=".geojson")
        dir_path = os.path.join("data", folder)
        
        file_path = None
        for root, _dirs, files in os.walk(dir_path):
            for file in files:
                if file.startswith(regionID):
                    file_path = os.path.join(root, file)
                    break  # File found, no need to continue the loop
        
        if not file_path:
            logging.error(f"Failed to locate geojson file for regionID. Check local data folder/AWS S3 bucket")
            raise FileNotFoundError("Geojson file for regionID not found")
    else:
        file_path = local_file_path

    try:
        polygon = gpd.read_file(file_path)
    except Exception as e:
        logging.error(f"Failed to open geojson file provided: {e}")
        raise e

    # Checking if the point (long, lat) is within the polygon

    if polygon.geometry.contains(point).any():
        return (lat, long)
    else:
        return (pd.NA, pd.NA)
