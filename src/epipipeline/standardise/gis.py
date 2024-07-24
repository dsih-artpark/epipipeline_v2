import logging
import os
import re
import subprocess
import time
from typing import Union, Tuple
import geopandas as gpd
import googlemaps
import pandas as pd
from fuzzywuzzy import process
from shapely.geometry import Point
from tqdm import tqdm
import pkg_resources
import yaml

# Set up logging
logger = logging.getLogger("epipipeline.standardise.geocode")
logging.basicConfig(level=logging.INFO)
logging.captureWarnings(True)


def dist_mapping(*, stateID: str, districtName: str, df: pd.DataFrame, threshold: int = 65) -> Tuple[str, str]:
    """Standardises district names and codes (based on LGD), provided the standardised state ID

    Args:
        stateID (str): standarised state ID
        districtName (str): raw district name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 65

    Returns:
        tuple: (LGD district name, LGD district code or admin_0 if not matched)
    """

    if stateID=="admin_0" or pd.isna(districtName):
        return (pd.NA, "admin_0")

    districtName = str(districtName).title().strip()
    districtName = re.sub(r"Gulbarga", "Kalaburagi", districtName, flags=re.IGNORECASE)
    districtName = re.sub(r"\(?\sU\)?$", " Urban", districtName, flags=re.IGNORECASE)
    districtName = re.sub(r"\(?\sR\)?$", " Rural", districtName, flags=re.IGNORECASE)
    districtName = re.sub(r"Bijapur", "Vijayapura", districtName, flags=re.IGNORECASE)
    districtName = re.sub(r"C[\.\s]*H[\.\s]*Nagara*", "Chamarajanagara", districtName, flags=re.IGNORECASE)
    districtName = re.sub(r'\b(B[ae]ngal[ou]r[ue](?!\s*Rural)|Bbmp)\b', "Bengaluru Urban", districtName, flags=re.IGNORECASE)

    districts = df[df["parentID"] == stateID]["regionName"].to_list()
    match = process.extractOne(districtName, districts, score_cutoff=threshold)
    if match:
        districtName = match[0]
        districtCode = df[(df["parentID"] == stateID) & (df["regionName"] == districtName)]["regionID"].values[0]
    else:
        districtCode = "admin_0"
    return (districtName, districtCode)  # returns original name if unmatched


def subdist_ulb_mapping(*, districtID: str, subdistName: str, df: pd.DataFrame, threshold: int = 65, childType: Union[str, list, None] = None) -> Tuple[str, str]:
    """Standardises subdistrict/ulb names and codes (based on LGD), provided the standardised district ID

    Args:
        districtID (str): standarised district ID
        subdistName (str): raw subdistrict/ulb name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 65
        childType (Union[str, list, None], optional): Specify the type(s) of children (subdistrict/ulb) to consider. Defaults to None.

    Returns:
        tuple: (LGD subdistrict/ulb name, LGD subdistrict/ulb code or admin_0 if not matched)
    """

    # if subdist name is na, return admin_0
    if districtID=="admin_0" or pd.isna(subdistName):
        return (pd.NA, "admin_0")

    # string clean subdist name
    subdistName = str(subdistName).title().strip()
    subdistName = re.sub(r'\(?\sU\)?$', "Urban",
                         subdistName, flags=re.IGNORECASE)
    subdistName = re.sub(r'\(?\sR\)?$', "Rural",
                         subdistName, flags=re.IGNORECASE)
    
    # filter for subdistricts/ulbs
    if childType:
        with open(pkg_resources.resource_filename(__name__, 'settings.yaml'), 'r') as f:
            settings = yaml.safe_load(f)
            expected_childType = settings["geo_prefixes"]

        if isinstance(childType, str):
            childType = childType.lower().strip()
            if childType not in expected_childType:
                raise ValueError(f"ChildType: {childType} does not exist in master: {expected_childType}")
            else:
                childType=[childType]
        elif isinstance(childType, list):
            childType = [str(type).lower().strip() for type in childType]
            if not childType.issubset(expected_childType):
                raise ValueError(f"Child Type: {childType} not found in master: {expected_childType}")
        else:
            raise TypeError("Child Type must be a string or list of strings")
        
        subdistricts = df[(df["parentID"] == districtID) & (df["regionID"].str.startswith(tuple(childType)))]["regionName"].to_list()
    else:
        subdistricts = df[(df["parentID"] == districtID)]["regionName"].to_list()
        
    match = process.extractOne(
        subdistName, subdistricts, score_cutoff=threshold)
    if match:
        subdistName = match[0]
        subdistCode = df[(df["parentID"] == districtID) & (
            df["regionName"] == subdistName)]["regionID"].values[0]
        return (subdistName, subdistCode)
    else:
        return (subdistName, "admin_0")  # returns original name if unmatched


def village_ward_mapping(*, subdistID: str, villageName: str, df: pd.DataFrame, threshold: int = 95) -> Tuple[str, str]:
    """Standardises village names and codes (based on LGD), provided the standardised district ID

    Args:
        subdistID (str): standarised subdistrict/ulb ID
        villageName (str): raw village/ward name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 95

    Returns:
        tuple: (LGD village/ward name, LGD village/ward code or admin_0 if not matched)
    """
    if subdistID=="admin_0" or pd.isna(villageName):
        return (pd.NA, "admin_0")

    villageName = str(villageName).title().strip()
    villages = df[df["parentID"] == subdistID]["regionName"].to_list()
    match = process.extractOne(villageName, villages, score_cutoff=threshold)
    if match:
        villageName = match[0]
        villageCode = df[(df["parentID"] == subdistID) & (
            df["regionName"] == villageName)]["regionID"].values[0]
        return (villageName, villageCode)
    else:
        return (villageName, "admin_0")  # returns original name if unmatched


def get_api_key(*, encrypted_file_path: str = "~/config.enc") -> str:
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


def geocode(*, addresses: Union[pd.Series, str], batch_size: int = 0, API_key: str) -> pd.Series:
    """Uses Google Maps API to convert addresses to lat, long positions

    Args:
        addresses (pd.Series or str): pandas series/str with address(es)
        batch_size (int): number of rows to process in a batch, optional
        API_key (str): Google Maps API key from GCP

    Returns:
        pd.Series: pandas Series with lat, long positions
    """
    if isinstance(addresses, str):
        addresses = pd.Series([addresses])
        batch_size = 0
        single_address = True
    elif isinstance(addresses, pd.Series):
        single_address = False
    else:
        raise ValueError("addresses must be either a pandas Series or a string")

    assert isinstance(API_key, str) and len(API_key) >= 35, "Invalid API key length"
    assert isinstance(batch_size, int) and batch_size >= 0, "Batch size must an integer >=0"


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
        all_geocoded_results = pd.Series(geocode_batch(addresses))

        if single_address:
            return all_geocoded_results.iloc[0]
        else:
            return all_geocoded_results


def check_bounds(*, lat: Union[float, pd.Series], long: Union[float, pd.Series], regionID: str, geojson_dir: str = "data/GS0012DS0051-Shapefiles_India/geojsons/individual/") -> Union[pd.Series, tuple]: # noqa: E501
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
    if not re.match(r'^(state|district|subdistrict|ulb|village)_\d{2,6}$', regionID) and not re.match(r'^(zone|ward)_\d{2,6}-\d{1,6}$', regionID):  # noqa: E501
        raise ValueError(f"Invalid regionID: {regionID}")

    # Construct file path
    geojson_path = os.path.join(geojson_dir, regionID + ".geojson")

    # Read the geojson file
    try:
        polygon = gpd.read_file(geojson_path)
    except Exception as e:
        raise IOError(f"Failed to open geojson file: {e}") from e

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

def clean_lat_long(*, lat: Union[str, float], long: Union[str, float]) -> Tuple[float, float]:
    """Validates lat long positions after removing extraneous elements and adding floating point where missing

    Args:
        lat (Union[str, float]): latitude
        long (Union[str, float]): longitude

    Returns:
        Tuple[Union[float, pd.NA], Union[float, pd.NA]]: Lat, long
    """

    # If lat or long is pd.NA, return pd.NA for both values
    if pd.isna(lat) or pd.isna(long):
        return (pd.NA, pd.NA)

    # Clean lat and long if they are not floats
    if not isinstance(lat, float):
        lat = re.sub(r"[^\d.-]", "", str(lat))
        try:
            lat = float(lat)
        except ValueError:
            return (pd.NA, pd.NA)
    
    if not isinstance(long, float):
        long = re.sub(r"[^\d.-]", "", str(long))
        try:
            long = float(long)
        except ValueError:
            return (pd.NA, pd.NA)

    # Validate lat
    if not (-90 <= lat <= 90):
        if '.' not in str(lat):
            lat = float(str(lat)[:2] + "." + str(lat)[2:])
        if not (-90 <= lat <= 90):
            return (pd.NA, pd.NA)
    
    # Validate long
    if not (-180 <= long <= 180):
        if '.' not in str(long):
            long = float(str(long)[:3] + "." + str(long)[3:])
        if not (-180 <= long <= 180):
            return (pd.NA, pd.NA)

    return (lat, long)



