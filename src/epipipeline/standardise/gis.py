import re

import pandas as pd
from fuzzywuzzy import process


def dist_mapping(*, stateID: str, districtName: str, df: pd.DataFrame, threshold: int = 65) -> tuple:
    """Standardises district names and codes (based on LGD), provided the standardised state ID

    Args:
        stateID (str): standarised state ID
        districtName (str): raw district name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 65

    Returns:
        tuple: (LGD district name, LGD district code or admin_0 if not matched)
    """

    if pd.isna(districtName):
        return (pd.NA, "admin_0")

    districtName = districtName.upper().strip()
    districtName = re.sub(r"GULBARGA", "KALABURAGI", districtName)
    districtName = re.sub(r"\(?\sU\)?$", " URBAN", districtName)
    districtName = re.sub(r"\(?\sR\)?$", " RURAL", districtName)
    districtName = re.sub(r"BIJAPUR", "VIJAYAPURA", districtName)
    districtName = re.sub('\b(B[AE]NGAL[OU]R[UE](?!\s*RURAL)|BBMP)\b', "BENGALURU URBAN", districtName)

    districts = df[df["parentID"] == stateID]["regionName"].to_list()
    match = process.extractOne(districtName, districts, score_cutoff=threshold)
    if match:
        districtName = match[0]
        districtCode = df[(df["parentID"] == stateID) & (df["regionName"] == districtName)]["regionID"].values[0]
    else:
        districtCode = "admin_0"
    return (districtName, districtCode)  # returns original name if unmatched


def subdist_ulb_mapping(districtID: str, subdistName: str, df: pd.DataFrame, threshold: int = 65) -> tuple:
    """Standardises subdistrict/ulb names and codes (based on LGD), provided the standardised district ID

    Args:
        districtID (str): standarised district ID
        subdistName (str): raw subdistrict/ulb name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 65

    Returns:
        tuple: (LGD subdistrict/ulb name, LGD subdistrict/ulb code or admin_0 if not matched)
    """
    # subdist
    if pd.isna(subdistName):
        return (pd.NA, "admin_0")

    subdistName = subdistName.upper().strip()
    subdistName = re.sub(r'\(?\sU\)?$', " URBAN",
                         subdistName, flags=re.IGNORECASE)
    subdistName = re.sub(r'\(?\sR\)?$', " RURAL",
                         subdistName, flags=re.IGNORECASE)
    subdistricts = df[df["parentID"] == districtID]["regionName"].to_list()
    match = process.extractOne(
        subdistName, subdistricts, score_cutoff=threshold)
    if match:
        subdistName = match[0]
        subdistCode = df[(df["parentID"] == districtID) & (
            df["regionName"] == subdistName)]["regionID"].values[0]
        return (subdistName, subdistCode)
    else:
        return (subdistName, "admin_0")  # returns original name if unmatched


def village_ward_mapping(subdistID: str, villageName: str, df: pd.DataFrame, threshold: int = 95) -> tuple:
    """Standardises village names and codes (based on LGD), provided the standardised district ID

    Args:
        subdistID (str): standarised subdistrict/ulb ID
        villageName (str): raw village/ward name
        df (pd.DataFrame): regions.csv as a dataframe
        threshold (int): cut-off for fuzzy matching, default set to 95

    Returns:
        tuple: (LGD village/ward name, LGD village/ward code or admin_0 if not matched)
    """
    if pd.isna(villageName):
        return (pd.NA, "admin_0")

    villageName = villageName.upper().strip()
    villages = df[df["parentID"] == subdistID]["regionName"].to_list()
    match = process.extractOne(villageName, villages, score_cutoff=threshold)
    if match:
        villageName = match[0]
        villageCode = df[(df["parentID"] == subdistID) & (
            df["regionName"] == villageName)]["regionID"].values[0]
        return (villageName, villageCode)
    else:
        return (villageName, "admin_0")  # returns original name if unmatched
