import logging
import uuid

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
    validate_age,
)
from epipipeline.standardise.dates import fix_symptom_date, fix_two_dates, string_clean_dates  # , fix_year_hist
from epipipeline.standardise.gis import subdist_ulb_mapping, village_ward_mapping

# Set up logging
logger = logging.getLogger("epipipeline.standardise.dengue.karnataka")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)

def standardise_ka_linelist_v3(*,
                               preprocessed_data_dict, CURRENT_YEAR, THRESHOLDS, STR_VARS,
                               regionIDs_df, regionIDs_dict, tagDate=None):

    standardise_data_dict = dict()
    for districtID in preprocessed_data_dict.keys():

        districtName = regionIDs_dict[districtID]
        df = preprocessed_data_dict[districtID].copy()
        df["demographics.age"]=df["demographics.age"].apply(lambda x: standardise_age(x))

        # Validate Age - 0 to 105
        df["demographics.age"]=df["demographics.age"].apply(lambda x: validate_age(x))

        # Bin Age
        df["demographics.ageRange"]=pd.cut(df["demographics.age"].fillna(-999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False) # noqa: E501
        df.loc[df["demographics.age"].isna(), "demographics.ageRange"]=pd.NA

        # Standardise Gender - MALE, FEMALE, UNKNOWN
        df["demographics.gender"]=df["demographics.gender"].apply(lambda x: standardise_gender(x))

        ### Standardise Result variables - POSITIVE, NEGATIVE, UNKNOWN
        df["event.test.test1.result"]=df["event.test.test1.result"].apply(lambda x: standardise_test_result(x))
        df["event.test.test2.result"]=df["event.test.test2.result"].apply(lambda x: standardise_test_result(x))

        ## Generate test count - [0,1,2]
        df["event.test.numberOfTests"]=df.apply(lambda x: generate_test_count(x["event.test.test1.result"], x["event.test.test2.result"]), axis=1) # noqa: E501

        # Standardise case variables
        ## OPD, IPD
        if "case.opdOrIpd" not in df.columns():
            logger.info(f"District {districtName} ({districtID}) does not have OPD-IPD info")
        else:
            df["case.opdOrIpd'"]=df["case.opdOrIpd'"].apply(lambda x: opd_ipd(x))

        ## PUBLIC, PRIVATE
        if "case.publicOrPrivate" not in df.columns():
            logger.info(f"District {districtName} ({districtID}) does not have Public-Private info")
        else:
            df["case.publicOrPrivate"]=df["case.publicOrPrivate"].apply(lambda x: public_private(x))

        ## ACTIVE, PASSIVE
        if "case.surveillance" not in df.columns():
            logger.info(f"District {districtName} ({districtID}) does not have Active-Passive Surveillance info")
        else:
            df["case.surveillance"]=df["case.surveillance"].apply(lambda x: active_passive(x))

        # URBAN, RURAL
        if "case.urbanOrRural" not in df.columns():
            logger.info(f"District {districtName} ({districtID}) does not have Urban vs Rural info")
        else:
            df["case.urbanOrRural"]=df["case.urbanOrRural"].apply(lambda x: rural_urban(x))

        # Fix date variables
        datevars=["event.symptomOnsetDate", "event.test.sampleCollectionDate","event.test.resultDate"]

        # Fix symptom date where number of days is entered instead of date
        new_dates=df.apply(lambda x: fix_symptom_date(x["event.symptomOnsetDate"], x["event.test.resultDate"]), axis=1)
        df["event.symptomOnsetDate"], df["event.test.resultDate"] = zip(*new_dates)

        # Then, string clean dates and fix year errors to current/previous (if dec)/next (if jan)
        for var in datevars:
            df[var]=df[var].apply(lambda x: string_clean_dates(x))
            # df[var]=df[var].apply(lambda x: fix_year_hist(x,CURRENT_YEAR))

        # Then, carry out year and date logical checks and fixes on symptom and sample date first
        result=df.apply(lambda x: fix_two_dates(x["event.symptomOnsetDate"], x["event.test.sampleCollectionDate"]), axis=1)
        df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

        # Then, carry out year and date logical checks and fixes on symptom and sample date first
        result=df.apply(lambda x: fix_two_dates(x["event.test.sampleCollectionDate"], x["event.test.resultDate"]), axis=1)
        df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(*result)

        # One last time on symptom and sample date..for convergence..miracles do happen!
        result=df.apply(lambda x: fix_two_dates(x["event.symptomOnsetDate"], x["event.test.sampleCollectionDate"]), axis=1)
        df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

        # format dates to ISO format
        for var in datevars:
            df[var]=pd.to_datetime(df[var]).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Setting primary date - symptom date > sample date > result date
        df["metadata.primaryDate"]=df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])

        # Clean string vars
        for var in STR_VARS:
            if var in df.columns:
                df[var]=df[var].apply(lambda x: clean_strings(x))

        # Geo-mapping
        ## Note: can be optimised to improve geo-mapping
        # Move BBMP from district to subdistrict/ulb field
        # df.loc[df["location.admin2.name"]=="BBMP", "location.admin3.name"]="BBMP"

        assert len(df[df["location.admin2.ID"]=="admin_0"])==0, "District(s) missing"

        # Map subdistrict/ulb name to standardised LGD name and code
        subdist=df.apply(lambda x: subdist_ulb_mapping(x["location.admin2.ID"], x["location.admin3.name"], regionIDs_df,
        THRESHOLDS["subdistrict"]), axis=1)
        df["location.admin3.name"], df["location.admin3.ID"]=zip(*subdist)

        # Map village/ward name to standardised LGD name and code
        villages=df.apply(lambda x: village_ward_mapping(x["location.admin3.ID"], x["location.admin5.name"],
                            regionIDs_df, THRESHOLDS["village"] ), axis=1)
        df["location.admin5.name"], df["location.admin5.ID"]=zip(*villages)

        # Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
        df["location.admin.hierarchy"]=df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("REVENUE" if x.startswith("subdistrict") else "admin_0"))  # noqa: E501

        # Drop duplicates across all vars after standardisation
        df.drop_duplicates(inplace=True)

        # Generate recordID after standardisation and de-duplication
        df["metadata.recordID"]=[uuid.uuid4() for i in range(len(df))]

        standardise_data_dict[districtID] = df

    return standardise_data_dict
