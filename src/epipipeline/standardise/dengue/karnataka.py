import datetime
import logging
import uuid
from typing import (Optional, Union)

import pandas as pd
import re
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
from epipipeline.standardise.gis import (dist_mapping, subdist_ulb_mapping, village_ward_mapping)
from epipipeline.preprocess import (clean_colname, map_column)

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
        df["event.test.numberOfTests"] = df.apply(lambda x: generate_test_count(test_results=[x["event.test.test1.result"], x["event.test.test2.result"]]), axis=1)  # noqa: E501

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
        df["location.admin.hierarchy"] = df["location.admin3.ID"].apply(lambda x: "admin_0" if pd.isnull(x) else ("Revenue" if x.startswith("subdistrict") else ("ULB" if x.startswith("ulb") else "admin_0")))  # noqa: E501

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


def standardise_ka_summary_v2(raw_dict: dict,
    drop_cols: Union[int, None],
    header_mapper: dict,
    min_cols: Union[list,None],
    data_dict: dict,
    regions: pd.DataFrame) -> dict: 

    """Standardises daily summaries

    Args:
        raw_dict (dict): dictionary with key = date of report, value = df (raw data)
        drop_cols (Union[int, None]): col index to be used
        header_mapper (dict): dict mapping standardised colnames to a list of preprocessed colnames
        min_cols (Union[list,None]): list of minimum cols the dataframe must have
        data_dict (dict): full list of cols the std dataframe must have
        regions (pd.DataFrame): dataframe of LGD regionids and names

    Raises:
        ValueError: invalid date for summary
        Exception: file missing min cols

    Returns:
        dict: key = date of summary, value = std df
    """
    
    standardised_dict={}

    for key in raw_dict.keys():
        logging.info(f"Processing {key}")
        try:
            date = pd.to_datetime(key, format="%Y-%m-%d")
        except Exception as e:
            raise ValueError(f"Invalid date format for {key}.")

        df = raw_dict[key]
        
        if drop_cols:
            df = df.iloc[:,:drop_cols]

        logging.info("Cleaning merged headers..")

        # forward fill unnamed and nan in current columns
        for i in range(1, len(df.columns)):
            if (re.match("Unnamed", str(df.columns[i]), re.IGNORECASE)) or (re.match("NaN", str(df.columns[i]), re.IGNORECASE)):
                df.columns.values[i] = df.columns.values[i-1]

        # identify index where df starts - i.e., S.No. is 1 - not ideal, explore pivot column
        df_start = df[df.iloc[:, 0] == 1].index[0]

        # for each header row in the dataframe (except last), forward fill if nan
        for row in range(df_start-1):
            df.iloc[row] = df.iloc[row].ffill()

        # for each header row in the dataframe,upward merge
        for row in range(df_start):
            row_data = df.iloc[row].to_list()
            for i in range(len(row_data)):
                if not re.search("nan", str(row_data[i]), re.IGNORECASE):
                    df.columns.values[i] = re.sub(r"[\d\-\(\)\s]+", "", df.columns.values[i].strip())+"_" + re.sub(r"[\d\-\(\)\s]+", "", str(row_data[i]).strip())

        logging.info("Dropping extraneous cols & rows")

        # drop village, etc.
        drop_cols = [col for col in df.columns if re.search(
            r"Taluk|Village|PHC|Population|Block|Remarks", col, re.IGNORECASE)]
        df.drop(columns=drop_cols, inplace=True)

        # remove header rows
        df = df.iloc[df_start:, :]

        # removed empty rows
        df = df.dropna(axis=0, how="all")
        df = df.dropna(axis=1, how="all")

        logging.info(f"Extraneous cols & rows removed : {df.columns, df.shape[0]}.")

        # standardising colnames
        headers=[clean_colname(colname=col) for col in df.columns]

        df.columns=headers

        standard_headers = map_column(map_dict=header_mapper)

        df = df.rename(columns=standard_headers)

        logging.info(f"Standardised colnames: {df.columns}.")

        # check that min cols are present
        if min_cols:
            if not set(min_cols).issubset(set(df.columns)):
                raise Exception(f"File is missing minimum required columns - {set(min_cols).difference(set(df.columns))}. Current columns are: {df.columns}. Re-run the code/update header_mapper in metadata.yaml.")

        # add standardised cols from metadata.yaml
        # adding standard list of columns from metadata that are not present in the dataset
        for col in data_dict:
            if col not in df.columns:
                if "default_value" in data_dict[col]:
                    df[col] = data_dict[col]["default_value"]
                else:
                    df[col] = pd.NA

        # extract BBMP from S.No. to district - the district
        df["sl_no"] = df["sl_no"].astype(str)
        df.loc[(df["sl_no"].str.contains(r"[Cc]ity") == True),"location.admin3.name"] = "BBMP"
        df.loc[(df["location.admin3.name"] == "BBMP"),"location.admin2.name"] = "Bengaluru Urban"

        # drop total, rows with district name missing
        df = df[(df["location.admin2.name"].str.contains(r"[Tt]otal") == False) & (df["sl_no"].str.contains(r"[Tt]otal") == False) & (df["location.admin2.name"].isna() == False)]

        # geo-mapping - districts
        logging.info("Standardising district and sub-districts.")

        # Map district name to standardised LGD name and code
        dists = df.apply(lambda x: dist_mapping(stateID=x["location.admin1.ID"], districtName=x["location.admin2.name"], df=regions), axis=1)

        df["location.admin2.name"], df["location.admin2.ID"] = zip(*dists)

        assert len(df[df["location.admin2.ID"] == "admin_0"]) == 0, "District(s) missing"

        # Map subdistrict/ulb name to standardised LGD name and code
        subdist = df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["location.admin3.name"], df=regions), axis=1)
        df["location.admin3.name"], df["location.admin3.ID"] = zip(*subdist)

        # Extract admin hierarchy from admin3.ID - ULB, REVENUE, admin_0 (if missing ulb/subdistrict LGD code)
        df["location.admin.hierarchy"] = df["location.admin3.ID"].apply(lambda x: pd.NA if pd.isna(x) else "ULB" if x.startswith("ulb") else "Revenue" if x.startswith("subdistrict") else "admin_0")

        # Drop duplicates across all vars after standardisation
        df.drop_duplicates(inplace=True)

        # Generate recordID after standardisation and de-duplication
        df["metadata.recordID"] = [uuid.uuid4() for i in range(len(df))]

        # Generate recordDate from dict key
        df["metadata.recordDate"] = date.strftime('%Y-%m-%dT%H:%M:%SZ')
        df["metadata.ISOWeek"] = date.isocalendar().week

        # Cleaning int cols
        for col in df.columns:
            if col.startswith("daily") or col.startswith("cumulative"):
                df[col] = df[col].fillna(0).astype(int)

        # sorting and filtering headers
        headers = [col for col in df.columns.to_list() if data_dict[col]["access"]]

        headers = sorted(headers, key=list(data_dict.keys()).index)
        df = df[headers]
        
        # export file
        standardised_dict[key] = df
        
        logging.info(f"Standardised {key}")

    return standardised_dict
