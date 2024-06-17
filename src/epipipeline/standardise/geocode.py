
import pandas as pd
from googlemaps import Client as GoogleMaps
import googlemaps
import gmaps
import logging

logger = logging.getLogger("standardise.geocode")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)

# # Run this if you have encrypted the API key locally
# import subprocess

# # Enter the path to your openssl encoded file
# encrypted_file = "~/config.enc"

# # Command to decrypt using OpenSSL - You will be prompted to enter the openssl password used for encryption
# command = f"openssl aes-256-cbc -d -salt -in {encrypted_file}"

# try:
#     MyAPI=subprocess.check_output(command, shell=True, text=True).strip()
# except subprocess.CalledProcessError as e:
#     raise(e)

# Geocoding function

def geocode(*, full_address: str, API_key: str) -> tuple:
    """_summary_

    Args:
        full_address (str): concatenated address to include all relevant geographical fields

    Returns:
        tuple: lat, long
    """
    if pd.isna(full_address):
        return (pd.NA, pd.NA)
    else:
        assert isinstance(full_address, str) and isinstance(
            API_key, str) and len(API_key) == 39, "invalid input"
        gmaps = googlemaps.Client(key=API_key)
        try:
            geocode_result = gmaps.geocode(full_address)
            if geocode_result:
                lat = geocode_result[0]['geometry']['location']['lat']
                long = geocode_result[0]['geometry']['location']['lng']
                return (lat, long)
            else:
                logger.info(f"No result returned for {full_address")
                return (pd.NA,pd.NA)
        except Exception as e:
            logger.info(f"Error returned for {full_address}: {e}")
            return (pd.NA, pd.NA)
            
