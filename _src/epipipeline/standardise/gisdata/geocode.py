import requests
from epipipeline.standardise.gisdata import get_regionIDs, get_children
import boto3
from pathlib import Path


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


def get_regionIDs_from_geometry(lat: float, lon: float, ancestorID: str,
                                provided_regionID: str, hierarchy_type: str,
                                shpdir="shapefiles/") -> list:

    ancestorType = ancestorID.split("_")[0]

    hierarchy_dict = {"revenue": ["country", "state", "district", "subdistrict", "village"],
                      "ulb": ["country", "state", "district", "ulb", "zone", "ward"]
                      }
    hierarchy_list = hierarchy_dict[hierarchy_type]
    # Check if parentType is in hierarchy
    if ancestorType not in hierarchy_list:
        raise ValueError(f"{ancestorType} is not a valid parent Type for the provided hierarchy ({hierarchy_list})")

    regionIDs_df, regionIDs_dict = get_regionIDs()

    # Creating a list of Types of regions to search, based on hierarchy list
    regionTypes = hierarchy_list[hierarchy_list.index(ancestorType):]

    # Creating a dictionary with all the children to search through, and their respective children
    # This can be integrated into the main get_children function with a deep=True
    all_children = {}

    for i in len(regionTypes):
        regionType = regionTypes[i]
        if i < len(regionTypes)-1:
            childType = regionTypes[i+1]
        else:
            childType = None

        # Adding the direct children of the ancestor
        if regionType == ancestorType:
            all_children.update({ancestorID: get_children(parentID=ancestorID, childType=childType, regionIDs_df=regionIDs_df)})

        # Adding the children based on the loop
        else:
            update_dict = {}  # Creating an update dict as you are iterating through the all_children dictionary
            for parentID, children in all_children.items():
                if regionType == parentID.split("_")[0]:
                    parentIDs = children["regionID"].to_list()
                    for parentID in parentIDs:
                        if childType is not None:
                            update_dict.update({parentID: get_children(parentID=parentID, childType=childType, regionIDs_df=regionIDs_df)})
                        else:
                            update_dict.update({parentID: None})

            all_children.update(update_dict)

    for child in all_children.keys():
        fname = parentID + ".geojson"
        localshp = Path(shpdir + fname)

        client = boto3.client('s3')
        Bucket = "dsih-artpark-03-standardised-data"

        if not localshp.is_file():
            Key = "GS0012DS0051-Shapefiles_India/geojsons/individual/" + fname
            client.download_file(Bucket=Bucket, Key=Key, Filename=localshp)
