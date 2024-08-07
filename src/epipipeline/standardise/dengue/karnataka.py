import datetime
import logging
import uuid
from typing import Optional

import pandas as pd
from epipipeline.standardise import (
    active_passive,
    clean_strings,
    generate_test_count,
    opd_ipd,
    public_private,
    rural_urban,
    standardise_age,
    standardise_gender,
    standardise_test_result,
    validate_age
)
from epipipeline.standardise.dates import (extract_symptom_date, string_clean_dates, fix_year_for_ll, fix_two_dates, check_date_bounds)
from epipipeline.standardise.gis import (subdist_ulb_mapping, village_ward_mapping)

# Set up logging
logger = logging.getLogger("epipipeline.standardise.dengue.karnataka")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)


def standardise_ka_linelist_v3(*,
                               preprocessed_data_dict: dict, THRESHOLDS: dict, STR_VARS: list,
                               regionIDs_df: pd.DataFrame, regionIDs_dict: dict, data_dictionary: dict, tagDate: Optional[datetime.datetime] = None) -> pd.DataFrame:  # noqa: E501
    """standardises preprocessed line lists for KA Dengue

    Args:
        preprocessed_data_dict (dict): dictionary of preprocessed datasets, key = dist, value = preprocessed df
        THRESHOLDS (dict): from metadata.yaml, fuzzymatch thresholds for dist, subdist, village
        STR_VARS (list): from metadata.yaml, list of string vars to be cleaned
        regionIDs_df (pd.DataFrame): from aws s3 - regionids database, df of region names and ids
        regionIDs_dict (dict): from aws s3 - regionids database, dict of parent-child region names and ids
        data_dictionary (dict): from metadata.yaml
        tagDate (datetime.datetime, optional): date of raw data file. Defaults to None.

    Returns:
        pd.DataFrame: standardised dataset (without pii vars)
    """

    standardise_data_dict = dict()
    for districtID in preprocessed_data_dict.keys():

        districtName = regionIDs_dict[districtID]
        df = preprocessed_data_dict[districtID].copy()
        df["demographics.age"] = df["demographics.age"].apply(
            lambda x: standardise_age(age=x))

        # Validate Age - 0 to 105
        df["demographics.age"] = df["demographics.age"].apply(
            lambda x: validate_age(age=x))

        # Bin Age
        df["demographics.ageRange"] = pd.cut(df["demographics.age"].replace(pd.NA, -999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False)  # noqa: E501
        df.loc[df["demographics.age"].isna(), "demographics.ageRange"] = pd.NA

        # Standardise Gender - Male, Female, Unknown
        df["demographics.gender"] = df["demographics.gender"].apply(
            lambda x: standardise_gender(gender=x))

        # Standardise Result variables - Positive, Negative, Unknown
        df["event.test.test1.result"] = df["event.test.test1.result"].apply(
            lambda x: standardise_test_result(result=x))
        df["event.test.test2.result"] = df["event.test.test2.result"].apply(
            lambda x: standardise_test_result(result=x))

        # Generate test count - [0,1,2]
        df["event.test.numberOfTests"] = df.apply(lambda x: generate_test_count(test1=x["event.test.test1.result"], test2=x["event.test.test2.result"]), axis=1)  # noqa: E501

        # Standardise case variables
        # OPD, IPD
        if "case.opdOrIpd" not in df.columns.to_list():
            logger.info(f"District {districtName} ({districtID}) does not have OPD-IPD info")
            df["case.opdOrIpd"] = pd.NA
        else:
            df["case.opdOrIpd"] = df["case.opdOrIpd"].apply(
                lambda x: opd_ipd(s=x))

        # Public, Private
        if "case.publicOrPrivate" not in df.columns.to_list():
            logger.info(f"District {districtName} ({districtID}) does not have Public-Private info")
            df["case.publicOrPrivate"] = pd.NA
        else:
            df["case.publicOrPrivate"] = df["case.publicOrPrivate"].apply(
                lambda x: public_private(s=x))

        # Active, Passive
        if "case.surveillance" not in df.columns.to_list():
            logger.info(f"District {districtName} ({districtID}) does not have Active-Passive Surveillance info")
            df["case.surveillance"] = pd.NA
        else:
            df["case.surveillance"] = df["case.surveillance"].apply(
                lambda x: active_passive(s=x))

        # Urban, Rural
        if "case.urbanOrRural" not in df.columns.to_list():
            logger.info(f"District {districtName} ({districtID}) does not have Urban vs Rural info")
            df["case.urbanOrRural"] = pd.NA
        else:
            df["case.urbanOrRural"] = df["case.urbanOrRural"].apply(
                lambda x: rural_urban(s=x))

        # Fix date variables
        datevars = ["event.symptomOnsetDate",
                    "event.test.sampleCollectionDate", "event.test.resultDate"]

        # Extract symptom date where number of days is entered instead of date
        new_dates = df.apply(lambda x: extract_symptom_date(
            symptomDate=x["event.symptomOnsetDate"], sampleDate=x["event.test.sampleCollectionDate"]), axis=1)
        df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(
            *new_dates)

        # Then, string clean dates and fix year errors to current/previous (if dec)/next (if jan)
        for var in datevars:
            df[var] = df[var].apply(lambda x: string_clean_dates(Date=x))
            df[var] = df[var].apply(lambda x: fix_year_for_ll(Date=x))

        # Then, carry out year and date logical checks and fixes on symptom and sample date first
        result = df.apply(lambda x: fix_two_dates(
            earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"]), axis=1)
        df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(
            *result)

        # Then, carry out year and date logical checks and fixes on symptom and sample date first
        result = df.apply(lambda x: fix_two_dates(
            earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"]), axis=1)
        df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(
            *result)

        # One last time on symptom and sample date..for convergence..miracles do happen!
        result = df.apply(lambda x: fix_two_dates(
            earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"]), axis=1)
        df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(
            *result)

        # Coerce dates to >= min date or <= current date, and format dates to ISO format
        for var in datevars:
            df[var] = df.apply(lambda x, var=var : check_date_bounds(
                Date=x[var], districtName=x["location.admin2.name"], districtID=x["location.admin2.ID"]), axis=1)
            df[var] = df[var].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        # Setting primary date - symptom date > sample date > result date
        df["metadata.primaryDate"] = df["event.symptomOnsetDate"].fillna(
            df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])

        # Clean string vars
        for var in STR_VARS:
            if var in df.columns:
                df[var] = df[var].astype(str)
                df[var] = df[var].apply(lambda x: clean_strings(s=x))

        # Geo-mapping
        # If BBMP data is shared, move BBMP tag from district to subdistrict/ulb column
        # df.loc[df["location.admin2.name"]=="Bbmp", "location.admin3.name"]="BBMP"

        assert len(df[df["location.admin2.ID"] == "admin_0"]
                   ) == 0, "District(s) missing"

        # Map subdistrict/ulb name to standardised LGD name and code
        subdist = df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["location.admin3.name"], df=regionIDs_df,  # noqa: E501
                                                         threshold=THRESHOLDS["subdistrict"]), axis=1)
        df["location.admin3.name"], df["location.admin3.ID"] = zip(*subdist)

        # Map village/ward name to standardised LGD name and code
        villages = df.apply(lambda x: village_ward_mapping(subdistID=x["location.admin3.ID"], villageName=x["location.admin5.name"],
                                                           df=regionIDs_df, threshold=THRESHOLDS["village"]), axis=1)
        df["location.admin5.name"], df["location.admin5.ID"] = zip(*villages)

        # Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
        df["location.admin.hierarchy"] = df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("Revenue" if x.startswith("subdistrict") else "admin_0"))  # noqa: E501

        # Generate admin coarseness
        df["location.admin.coarseness"] = df["location.admin5.ID"].fillna(df["location.admin4.ID"]).fillna(df["location.admin3.ID"]).fillna(df["location.admin2.ID"]).fillna(df["location.admin1.ID"]).fillna(df["location.country.ID"]).str.split("_").str.get(0)

        # Fillna for geovars
        for vars in ["location.admin2.ID", "location.admin3.ID", "location.admin4.ID", "location.admin5.ID"]:
            df[vars]=df[vars].fillna("admin_0")

        # Drop duplicates across all vars after standardisation
        df.drop_duplicates(inplace=True)

        # Drop empty rows for key vars
        df = df.dropna(subset=["metadata.nameAddress", "metadata.primaryDate",
                       "demographics.age", "demographics.gender"], thresh=2)

        # Generate recordID after standardisation and de-duplication
        df["metadata.recordID"] = [uuid.uuid4() for i in range(len(df))]

        # generate patient UUIDs
        df["age_limit"] = df["demographics.age"].apply(lambda x: f"{x-1}-{x+1}")
        df["metadata.patientID"] = df.groupby(by=['metadata.name', 'metadata.contact', 'demographics.gender', 'age_limit', 'metadata.address']).ngroup().apply(lambda x: uuid.uuid4())
        
        duplicate_patients = len(df[df.duplicated(subset="metadata.patientID")])

        if duplicate_patients > 0:
            logger.warning(f"There are {duplicate_patients} duplicate patients")


        # Sort headers after removing PII vars

        headers = [col for col in df.columns.to_list(
        ) if data_dictionary[col]["access"]]

        headers = sorted(headers, key=list(data_dictionary.keys()).index)
        df = df[headers]

        standardise_data_dict[districtID] = df

    return standardise_data_dict


