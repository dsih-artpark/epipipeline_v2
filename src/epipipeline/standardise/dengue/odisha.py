import pandas as pd
import numpy as np
import re
import dataio.download
from dataio.download import (download_dataset_v2,fetch_data_documentation)
from epipipeline.standardise.gis import (dist_mapping, subdist_ulb_mapping, village_ward_mapping)
from epipipeline.standardise import ( extract_gender_age, standardise_age, standardise_gender, validate_age, standardise_test_result, clean_strings, rural_urban, event_death)
from epipipeline.standardise.dates import (string_clean_dates)
import yaml
import os
import uuid

# set-up metadata from metadata.yaml
metadata=fetch_data_documentation(dsid = "EP0005DS0066")
metadata = metadata["tables"]["odisha_dengue_ll"]
# dependent on metadata above
data_dictionary = metadata["data_dictionary"]
dsid = metadata["admin"]["dsid"]["preprocessed"]
str_cols = metadata["admin"]["config"]["str_cols"]

# regions
regions=pd.read_csv("regions/regionids.csv")

# download preprocessed files
pp_csvs = download_dataset_v2(dsid=dsid, data_state = "preprocessed")

# Standardisation
for file in os.listdir(f"data/{pp_csvs}"):
    df=pd.read_csv(f"data/{pp_csvs}{file}")

    # add standardised cols
    for col in data_dictionary.keys():
        if "default_value" in data_dictionary[col].keys():
            df[col] = data_dictionary[col]["default_value"]
        elif col not in df.columns:
            df[col]=pd.NA

    ## result date - find out about cycle of dates 
    df["event.test.resultDate"]=df["event.test.resultDate"].apply(lambda x: string_clean_dates(Date=x))
    df["event.test.resultDate"]=df["event.test.resultDate"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df["metadata.primaryDate"]=df["event.test.resultDate"]

    # Extract gender, age - values are swapped
    res = df.apply(lambda x: extract_gender_age(gender=x["demographics.gender"], age=x["demographics.age"]), axis=1)
    df["demographics.gender"], df["demographics.age"] = zip(*res)

    # Standardise Age
    df["demographics.age"] = df["demographics.age"].apply(lambda x: standardise_age(age=x))

    # # Validate Age - 0 to 105
    df["demographics.age"] = df["demographics.age"].apply(lambda x: validate_age(age=x))

    # Bin Age
    df["demographics.ageRange"] = pd.cut(df["demographics.age"].fillna(-999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False)
    df.loc[df["demographics.age"].isna(), "demographics.ageRange"] = pd.NA

    # Standardise Gender
    df["demographics.gender"] = df["demographics.gender"].apply(lambda x: standardise_gender(gender=x))

    # Standardise Result variables - POSITIVE, NEGATIVE, UNKNOWN
    df["event.test.test1.result"] = df["event.test.test1.result"].apply(lambda x: standardise_test_result(result=x))
    df["event.test.test2.result"] = df["event.test.test2.result"].apply(lambda x: standardise_test_result(result=x))

    # Standardise Case variables
    df["case.urbanOrRural"] = df["case.urbanOrRural"].apply(lambda x: rural_urban(s=x))

    # Standardise event.death
    df["event.death"] = df["event.death"].apply(lambda x: event_death(s=x))

    # Clean string vars
    for var in str_cols:
        if var in df.columns:
            df[var] = df[var].apply(lambda x: clean_strings(s=x))

    # Extract mobile from address

    ## GEOGRAPHY
    # districts
    res=df.apply(lambda x: dist_mapping(stateID=x["location.admin1.ID"], districtName=x["location.admin2.name"], df=regions), axis=1)
    df["location.admin2.name"], df["location.admin2.ID"]=zip(*res)

    # subdists
    res=df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"], subdistName=x["location.admin3.name"], df=regions), axis=1)
    df["location.admin3.name"], df["location.admin3.ID"]=zip(*res)

    # village/ward
    res=df.apply(lambda x: village_ward_mapping(subdistID=x["location.admin3.ID"], villageName=x["location.admin5.name"], df=regions), axis=1)
    df["location.admin5.name"], df["location.admin5.ID"]= zip(*res)

    # admin hierarchy
    df["location.admin.hierarchy"] = df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("Revenue" if x.startswith("subdistrict") else "admin_0"))  # noqa: E501

    # Generate admin coarseness - rewrite function
    L=["location.admin2.ID", "location.admin3.ID", "location.admin5.ID"]

    for vars in L:
        df[vars]=df[vars].replace("admin_0", np.nan)

    df["location.admin.coarseness"]=df["location.admin5.ID"].fillna(df["location.admin3.ID"]).fillna(df["location.admin2.ID"]).fillna(df["location.admin1.ID"]).str.split("_").str.get(0)

    for vars in L:
        df[vars]=df[vars].fillna("admin_0")

    # filter empty rows
    df=df.dropna(how="all", axis=0)
    df=df.dropna(how="all", axis=1)

    # add standardised cols

    # drop duplicates - log duplicates
    df=df.drop_duplicates()

    # generate recordID
    df["metadata.recordID"]=[uuid.uuid4() for _ in range(len(df))]

    # filter/order vars
    df=df[data_dictionary.keys()]

    # filter pii vars
    for var in data_dictionary.keys():
        if not data_dictionary[var]["access_type"]:
            df.drop(columns=var)

    # export current file
    df.to_csv(f"standardised_{file}.csv", index=False)
    #------
