import pandas as pd
import googlemaps
import logging
import time
from tqdm import tqdm
import subprocess
import geopandas as gpd
from shapely.geometry import Point
import re
import os
from typing import Union


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
            if pd.isna(address):
                geocoded_results.append((pd.NA, pd.NA))
            else:
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


def check_bounds(lat: Union[float, pd.Series], long: Union[float, pd.Series], regionID: str, geojson_dir: str = "data/GS0012DS0051-Shapefiles_India/geojsons/individual/"):
    """
    Returns lat, long positions if within a polygon, else returns Null

    Args:
        lat (Union[float, pd.Series]): Latitude (float or pd.Series)
        long (Union[float, pd.Series]): Longitude (float or pd.Series)
        regionID (str): Standardized regionID
        geojson_dir (str, optional): Directory to geojson. Defaults to "data/GS0012DS0051-Shapefiles_India/geojsons/individual/".

    Raises:
        TypeError: Invalid data type for lat, long
        ValueError: Invalid regionID
        IOError: Unable to read geojson

    Returns:
        Union[pd.Series, tuple]: lat, long if within bounds of polygon, else pd.NA
    """

    # Check lat, long input validity - either pd.Series or float - convert floats to series
    if isinstance(lat, float) and isinstance(long, float):
        lat = pd.Series([lat])
        long = pd.Series([long])
    elif not (isinstance(lat, pd.Series) and isinstance(long, pd.Series)):
        raise TypeError(
            "Latitude and Longitude must be floats or pandas Series")

    # Check validity of regionID
    if not re.match(r'^(state|district|subdistrict|ulb|village)_\d{2,6}$', regionID) and not re.match(r'^(zone|ward)_\d{2,6}-\d{1,6}$', regionID):
        raise ValueError(f"Invalid regionID: {regionID}")

    # Construct file path
    geojson_path = os.path.join(geojson_dir, regionID + ".geojson")

    # Read the geojson file
    try:
        polygon = gpd.read_file(geojson_path)
    except Exception as e:
        raise IOError(f"Failed to open geojson file: {e}")

    # Create a series of points
    points = [Point(lon, lat) if not pd.isna(lat) and not pd.isna(
        lon) else None for lon, lat in zip(long, lat)]

    # Check if each point is within any of the geometries
    contains = pd.Series([polygon.geometry.contains(
        point).any() if point else False for point in points])

    # Return lat, long  series or pd.NA
    result = pd.Series([(lat_, long_) if contained else (pd.NA, pd.NA)
                       for lat_, long_, contained in zip(lat, long, contains)])

    return result.iloc[0] if len(result) == 1 else result
