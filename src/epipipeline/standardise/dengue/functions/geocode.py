import pandas as pd
from googlemaps import Client as GoogleMaps
import googlemaps
import gmaps

# 2 ways to retrieve API key - env file or local encryption
# Method 1 - Run this if you have set the API key in a .env file
from dotenv import load_dotenv
import os

load_dotenv()

try:
    MyAPI=os.getenv("GMAPS_API")
except:
    raise (".env var not found. Store your GMAPS API in a .env file with varname GMAPS_API")

# Method 2 - Run this if you have encrypted the API key locally
import subprocess

# Enter the path to your openssl encoded file
encrypted_file = "~/config.enc"

# Command to decrypt using OpenSSL - You will be prompted to enter the openssl password used for encryption
command = f"openssl aes-256-cbc -d -salt -in {encrypted_file}"

try:
    MyAPI=subprocess.check_output(command, shell=True, text=True)
except subprocess.CalledProcessError as e:
    raise(e)

# Geocoding function

def geocode(full_address: str, MyAPI: str) -> tuple:
    """_summary_

    Args:
        full_address (str): concatenated address to include all relevant geographical fields

    Returns:
        tuple: lat, long
    """
    assert isinstance(full_address, str) and isinstance(MyAPI,str) and len(MyAPI)==39, "invalid input"
    gmaps = googlemaps.Client(key=MyAPI)
    geocode_result = gmaps.geocode(full_address)
    lat= geocode_result[0]['geometry']['location'] ['lat']
    long= geocode_result[0]['geometry']['location']['lng']
    return pd.Series([lat,long])


