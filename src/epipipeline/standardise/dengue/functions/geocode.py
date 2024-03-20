# Dependencies - dotenv
# Save 
from dotenv import load_dotenv
import os

load_dotenv()

API=os.getenv('GMAPS_API')


def geocode(full_address: str, API_Key) -> tuple:
    """_summary_

    Args:
        full_address (str): concatenated address to include all relevant geographical fields

    Returns:
        tuple: lat, long
    """
    import pandas as pd
    from googlemaps import Client as GoogleMaps
    import googlemaps
    import gmaps

    geocode_result = gmaps.geocode(full_address)
    lat= geocode_result[0]['geometry']['location'] ['lat']
    long= geocode_result[0]['geometry']['location']['lng']
    return pd.Series([lat,long])


