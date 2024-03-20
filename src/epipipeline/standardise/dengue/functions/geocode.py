from dotenv import load_dotenv
import os
import pandas as pd
from googlemaps import Client as GoogleMaps
import googlemaps
import gmaps

load_dotenv()

try:
    MyAPI=os.getenv("GMAPS_API")
except:
    raise (".env var not found. Store your GMAPS API in a .env file with varname GMAPS_API")

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


