from epipipeline.ud.download import download_dataset
from fuzzywuzzy import process
import pandas as pd
import os


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


def get_children(regionID, childType, regionIDs_df):

    regionIDs = regionIDs_df[regionIDs_df["parentID"] == regionID].reset_index(drop=True)
    return regionIDs[regionIDs["regionID"].str.startswith(childType)].reset_index(drop=True)


def fuzzy_matching(string, choices, threshold):

    # what does process.extractOne do?
    best_match = process.extractOne(str(string), choices)

    if best_match[1] > threshold:
        return best_match[0]
    else:
        return None
