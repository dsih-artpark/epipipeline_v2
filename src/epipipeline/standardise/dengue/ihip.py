import datetime
import logging
import uuid
from typing import Optional

import pandas as pd
from epipipeline.standardise import (
    clean_strings,
    generate_test_count,
    opd_ipd,
    standardise_age,
    standardise_gender,
    standardise_test_result,
    validate_age
)
from epipipeline.standardise.dates import (string_clean_dates, fix_year_for_ll, fix_two_dates, check_date_bounds)
from epipipeline.standardise.gis import (clean_lat_long, dist_mapping, subdist_ulb_mapping, village_ward_mapping)

# Set up logging
logger = logging.getLogger("epipipeline.standardise.dengue.ihip")

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)

def standardise_ihip_v2(*, preprocessed_data_dict: dict, 
                        data_dictionary: dict,
                        date_vars: list, 
                        id_vars: list,
                        str_vars: list,
                        geo_vars: list,
                        limit_year: Optional[int] = None,
                        min_date: Optional[datetime.datetime] = None,
                        max_date: Optional[datetime.datetime] = None,
                        regions: pd.DataFrame
                        ) -> dict:
    """Standardises preprocessed data for IHIP (GoK & BBMP)

    Args:
        preprocessed_data_dict (dict): dictionary of preprocessed data with key - district name and value - preprocessed dataframe
        data_dictionary (dict): dictionary of standardised column names
        date_vars (list): list of date vars (standardised names)
        id_vars (list): list of id vars (standardised names)
        str_vars (list): list of str vars (standardised names)
        geo_vars (list): list of geo vars (standardised names)
        regions (pd.DataFrame): dataframe of regionids
        limit_year (Optional[int], optional): Year to limit dates to. Defaults to +-1 year
        min_date (Optional[datetime.datetime], optional): Min date of data. Defaults to None.
        max_date (Optional[datetime.datetime], optional): Max date of data. Defaults to date of running code for standardisation.

    Returns:
        dict: Standardised dictionary with keys = district name and values - standardised dataframe
    """
    standardised_data_dict = dict()

    for districtName in preprocessed_data_dict.keys():

        df = preprocessed_data_dict[districtName].copy()
        
        # add standardised cols
        for col in data_dictionary.keys():
            if "default_value" in data_dictionary[col].keys():
                df[col] = data_dictionary[col]["default_value"]
            elif col not in df.columns:
                df[col]=pd.NA

        logger.debug(f"Fixing date vars")

        # Fix datevars
        for datevar in date_vars:
            # clean dates
            df[datevar] = df[datevar].apply(lambda x: string_clean_dates(Date = x))

            # check year range
            if limit_year:
                df[datevar] = df[datevar].apply(lambda x: fix_year_for_ll(Date=x, Year=limit_year, limitYear=True))
            else:
                df[datevar] = df[datevar].apply(lambda x: fix_year_for_ll(Date=x))

        # fix logical check on symptom date >= sample date >= result date
        # specify lower and upper bounds
        if min_date and max_date:
            # fix symptom and sample date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], tagDate = max_date, minDate = min_date), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)
            
            # fix sample and result date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"], tagDate = max_date, minDate = min_date), axis=1)
            df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(*result)

            # fix symptom and sample date again
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], tagDate = max_date, minDate = min_date), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)
            
            # check date range and format to str to avoid changes in dates while working with Excel
            for datevar in date_vars:
                df[datevar] = df[datevar].apply(lambda x: check_date_bounds(Date=x, minDate=min_date, tagDate=max_date))
                df[datevar] = df[datevar].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # specify lower bound, upper bound not specified - defaults to date of running script
        elif min_date:
            # fix symptom and sample date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], minDate = min_date), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

            # fix sample and result date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"], minDate = min_date), axis=1)
            df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(*result)

            # fix symptom and sample date again
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], minDate = min_date), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

            # check date range and format to str to avoid changes in dates while working with Excel
            for datevar in date_vars:
                df[datevar] = df[datevar].apply(lambda x: check_date_bounds(Date=x, minDate=min_date))
                df[datevar] = df[datevar].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # specify upper bound, lower bound not specified
        elif max_date:
            # fix symptom and sample date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], tagDate = max_date), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

            # fix sample and result date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"], tagDate = max_date), axis=1)
            df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(*result)

            # fix symptom and sample date again
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"], tagDate = max_date), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

            # check date range and format to str to avoid changes in dates while working with Excel
            for datevar in date_vars:
                df[datevar] = df[datevar].apply(lambda x: check_date_bounds(Date=x, tagDate=max_date))
                df[datevar] = df[datevar].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        else:
            # lower and upper bounds not specified - upper bound defaults to date of running script
            # fix symptom and sample date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"]), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

            # fix sample and result date
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.test.sampleCollectionDate"], lateDate=x["event.test.resultDate"]), axis=1)
            df["event.test.sampleCollectionDate"], df["event.test.resultDate"] = zip(*result)

            # fix symptom and sample date again
            result = df.apply(lambda x: fix_two_dates(earlyDate=x["event.symptomOnsetDate"], lateDate=x["event.test.sampleCollectionDate"]), axis=1)
            df["event.symptomOnsetDate"], df["event.test.sampleCollectionDate"] = zip(*result)

             # check dates to max date after swapping above, as raw data failing check date bounds may be due to swap issues
            for datevar in date_vars:
                df[datevar] = df[datevar].apply(lambda x: check_date_bounds(Date=x))
                df[datevar] = df[datevar].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
        # Setting primary date - symptom date > sample date > result date
        df["metadata.primaryDate"] = df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"]) # noqa: E501

        logger.debug(f"Standardising demographics, str vars and case vars")

        # Standardise Age
        df["demographics.age"] = df["demographics.age"].apply(lambda x: standardise_age(age=x))

        # Validate Age - 0 to 105
        df["demographics.age"] = df["demographics.age"].apply(lambda x: validate_age(age=x))

        # Bin Age
        df["demographics.ageRange"] = pd.cut(df["demographics.age"].fillna(-999), bins=[0, 1, 6, 12, 18, 25, 45, 65, 105], include_lowest=False)
        df.loc[df["demographics.age"].isna(), "demographics.ageRange"] = pd.NA

        #Standardise Gender
        df["demographics.gender"] = df["demographics.gender"].apply(lambda x: standardise_gender(gender=x))

        # Standardise Result variables
        df["event.test.test1.result"] = df["event.test.test1.result"].apply(lambda x: standardise_test_result(result=x))
        df["event.test.test2.result"] = df["event.test.test2.result"].apply(lambda x: standardise_test_result(result=x))

        # Generate test count - [0,1,2]
        df["event.test.numberOfTests"] = df.apply(lambda x: generate_test_count(test1=x["event.test.test1.result"], test2=x["event.test.test2.result"]), axis=1)  # noqa: E501

        # Standardise case variables - OPD, IPD
        df["case.opdOrIpd"] = df["case.opdOrIpd"].apply(lambda x: opd_ipd(s=x))

        # Clean ID vars
        for id in id_vars:
            df[id] = df[id].str.replace(r"[^0-9\-]", "", regex=True)

        # Clean string vars
        for var in str_vars:
            if var in df.columns:
                df[var] = df[var].apply(lambda x: clean_strings(s=x))

        logger.debug(f"Mapping geovars")

        # Clean lat, long
        res=df.apply(lambda x: clean_lat_long(lat = x["location.geometry.latitude.provided"], long = x["location.geometry.longitude.provided"]), axis=1)
        df["location.geometry.latitude.provided"], df["location.geometry.longitude.provided"] = zip(*res)

        # Some entries need to be forced to pd.NA
        for var in ["location.admin2.name", "location.admin3.name", "ulb", "location.admin5.name"]:
            df[var]=df[var].replace("", pd.NA).replace("-", pd.NA).replace(" ", pd.NA)

        res=df.apply(lambda x: dist_mapping(stateID="state_29", districtName=x["location.admin2.name"], df=regions), axis=1)
        df["location.admin2.name"], df["location.admin2.ID"]=zip(*res)

        if len(df[df["location.admin2.ID"]=="admin_0"]) > 0:
            logger.warning(f"Cannot map district names to code: {df[df['location.admin2.ID']=='admin_0']['location.admin2.name'].to_list()}")
        
        # Standardise subdistrict names and map to codes 
        ## first nullify subdistrict where ulb is provided
        df.loc[df["ulb"].isna()==False, "location.admin3.name"]=pd.NA

        # then standardise subdistricts and map to codes
        res=df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"] , subdistName=x["location.admin3.name"], df=regions, childType="subdistrict"), axis=1)
        df["location.admin3.name"], df["location.admin3.ID"]=zip(*res)

        # Standardise ulbs and map to codes
        res=df.apply(lambda x: subdist_ulb_mapping(districtID=x["location.admin2.ID"] , subdistName=x["ulb"], df=regions, childType="ulb"), axis=1)
        df["ulb"], df["ulb_id"]=zip(*res)

        # Where ulb is mapped, move to subdistrict column (admin3 = either subdistrict or ulb)
        df.loc[df["ulb"].notna(), ["location.admin3.name", "location.admin3.ID"]] = df.loc[df["ulb"].notna(), ["ulb", "ulb_id"]].values

        # Drop ulb columns after moving it to subdistrict above
        df.drop(columns=["ulb", "ulb_id"], inplace=True)

        # Standardise villages and wards and map to codes
        res=df.apply(lambda x: village_ward_mapping(subdistID=x["location.admin3.ID"], villageName=x["location.admin5.name"], df=regions), axis=1)
        df["location.admin5.name"], df["location.admin5.ID"]= zip(*res)

        # Admin hierarchy
        df["location.admin.hierarchy"] = df["location.admin3.ID"].apply(lambda x: "ULB" if x.startswith("ulb") else ("Revenue" if x.startswith("subdistrict") else "admin_0"))  # noqa: E501

        # Generate admin coarseness
        df["location.admin.coarseness"] = df["location.admin5.ID"].fillna(df["location.admin4.ID"]).fillna(df["location.admin3.ID"]).fillna(df["location.admin2.ID"]).fillna(df["location.admin1.ID"]).fillna(df["location.country.ID"]).str.split("_").str.get(0)

        # Fillna for geovars
        for vars in geo_vars:
            df[vars]=df[vars].fillna("admin_0")

        # drop duplicates
        df=df.drop_duplicates()

        # generate patient UUIDs
        df["age_limit"] = df["demographics.age"].apply(lambda x: f"{x-1}-{x+1}")
        df["metadata.patientID"] = df.groupby(by=['metadata.name', 'metadata.contact', 'demographics.gender', 'age_limit', 'metadata.address']).ngroup().apply(lambda x: uuid.uuid4())
        
        duplicate_patients = len(df[df.duplicated(subset="metadata.patientID")])

        if duplicate_patients > 0:
            logger.warning(f"There are {duplicate_patients} duplicate patients")

        # filter/order vars
        # remove pii vars

        headers = [col for col in df.columns.to_list(
        ) if data_dictionary[col]["access"]]

        headers = sorted(headers, key=list(data_dictionary.keys()).index)
        df = df[headers]

        logger.debug(f"{districtName} standardised")
        
        # return standardised dataset
        standardised_data_dict[districtName] = df

    return standardised_data_dict
            