from importlib.metadata import version
import pandas as pd
from epipipeline.logging_config import setup_logging

__version__ = version(__name__)
setup_logging()


def get_regionIDs(*, regionIDs_path="data/GS0015DS0034-LGD_Region_IDs_and_Names/regionids.csv"):

    regionIDs_df = pd.read_csv(regionIDs_path)

    regionIDs_dict = {}
    for _, row in regionIDs_df.iterrows():
        regionIDs_dict[row["regionID"]] = {"regionName": row["regionName"],
                                           "parentID": row["parentID"]
                                           }
    return regionIDs_df, regionIDs_dict

