from epipipeline.ud.download import download_dataset
from fuzzywuzzy import process
import pandas as pd
import os
import boto3
from pathlib import Path


def get_regionIDs(regionIDs_ds_info=None):

    if regionIDs_ds_info is None:
        regionIDs_ds_info = regionIDs_ds_info = {'Bucket': 'dsih-artpark-03-standardised-data',
                                                 'Prefix': 'GS0015DS0034',
                                                 'Contains': '',
                                                 'Suffix': '.csv'}

    regionIDs_dir, _ = download_dataset(ds_info=regionIDs_ds_info)
    regionIDs_df = pd.read_csv(regionIDs_dir.name + "/" + os.listdir(regionIDs_dir.name)[0])

    regionIDs_dict = {}
    for _, row in regionIDs_df.iterrows():
        regionIDs_dict[row["regionID"]] = {"regionName": row["regionName"],
                                           "parentID": row["parentID"]
                                           }
    return regionIDs_df, regionIDs_dict


def get_children(regionID, childType=None, regionIDs_df=None, deep=False,
                 hierarchy_type=None, downloadshp=False, shpdir="shapefiles/"):
    """
    Get children regions based on provided region ID and optional parameters.

    Parameters:
    - regionID (str): The ID of the region whose children are to be retrieved.
    - childType (str, optional): Type of children regions to retrieve.
                                  Defaults to None.
    - regionIDs_df (DataFrame, optional): DataFrame containing region IDs and parent IDs.
                                          Defaults to None.
    - deep (bool, optional): If True, retrieves children recursively for each child.
                              Defaults to False.
    - hierarchy_type (str, optional): Type of hierarchy to consider for region types.
                                      Defaults to None.
    - downloadshp (bool, optional): If True, download shapefiles for the retrieved children.
                                    Defaults to False.
    - shpdir (str, optional): Directory path to save downloaded shapefiles.
                               Defaults to "shapefiles/".

    Returns:
    - dict: A dictionary containing region IDs as keys and their corresponding children as values.
            Keys are structured as region IDs and values are dictionaries representing their children.

    Raises:
    - ValueError: If childType is None for a shallow search or if the ancestorType provided is not valid
                  for the provided hierarchy.
    """
    if not deep:
        if childType is None:
            raise ValueError(f"{childType} cannot be none for a shallow search")
        if regionIDs_df is None:
            regionIDs_df, _ = get_regionIDs()
        regionIDs = regionIDs_df[regionIDs_df["parentID"] == regionID].reset_index(drop=True)
        return regionIDs[regionIDs["regionID"].str.startswith(childType)].reset_index(drop=True)
    else:
        ancestorID = regionID
        # Getting Ancestor Type
        ancestorType = ancestorID.split("_")[0]

        # Getting the hierarchy list based on hierarchy type. Hardcoded for now.
        hierarchy_dict = {"revenue": ["country", "state", "district", "subdistrict", "village"],
                          "ulb": ["country", "state", "district", "ulb", "zone", "ward"]}
        hierarchy_list = hierarchy_dict[hierarchy_type]

        # Raise Error if a valid Ancestor Type is not provided
        if ancestorType not in hierarchy_list:
            raise ValueError(f"{ancestorType} is not a valid parent Type for the provided hierarchy ({hierarchy_list})")

        # Creating a list of Types of regions to search, based on hierarchy list
        regionTypes = hierarchy_list[hierarchy_list.index(ancestorType):]
        all_children = {}
        for i in range(len(regionTypes)):
            regionType = regionTypes[i]
            if i < len(regionTypes)-1:
                childType = regionTypes[i+1]
            else:
                childType = None
            if i != 0:
                parentType = regionTypes[i-1]
            else:
                parentType = None

            # Adding the direct children of the ancestor. Deep is False by default, so this is a recursive call
            if regionType == ancestorType:
                all_children.update({ancestorID: get_children(regionID=ancestorID, childType=childType, regionIDs_df=regionIDs_df)})

            # Adding the children based on the loop
            else:
                update_dict = {}  # Creating an update dict as you are iterating through the all_children dictionary
                # Iterating through the children of each parent, and getting each of their children
                for parentID, children in all_children.items():
                    if parentType == parentID.split("_")[0]:
                        nextgen_parentIDs = children["regionID"].to_list()
                        for nextgen_parentID in nextgen_parentIDs:
                            # The lowest element in hierarchy has no childType. So checking for that.
                            # Getting immediate children of each region. recursive call, but deep is False
                            if childType is not None:
                                update_dict.update({nextgen_parentID: get_children(regionID=nextgen_parentID,
                                                                                   childType=childType, regionIDs_df=regionIDs_df)})
                            else:
                                update_dict.update({nextgen_parentID: None})
                all_children.update(update_dict)

        if downloadshp:
            for child in all_children.keys():
                fname = parentID + ".geojson"
                localshp = Path(shpdir + fname)

                client = boto3.client('s3')
                Bucket = "dsih-artpark-03-standardised-data"

                if not localshp.is_file():
                    Key = "GS0012DS0051-Shapefiles_India/geojsons/individual/" + fname
                    client.download_file(Bucket=Bucket, Key=Key, Filename=localshp)

        return all_children


def fuzzy_matching(string, choices, threshold):

    # what does process.extractOne do?
    best_match = process.extractOne(str(string), choices)

    if best_match is None:
        return None
    elif best_match[1] > threshold:
        return best_match[0]
    else:
        return None
