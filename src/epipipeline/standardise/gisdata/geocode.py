import requests


def geocode_address(address, api_key):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": api_key,
    }
    response = requests.get(base_url, params=params)
    data = response.json()

    if data["status"] == "OK":
        location = data["results"][0]["geometry"]["location"]
        lat = location["lat"]
        lon = location["lng"]
        return lat, lon, False
    else:
        error = "Geocoding failed for address " + address
        return None, None, error
