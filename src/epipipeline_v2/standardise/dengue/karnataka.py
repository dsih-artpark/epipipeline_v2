import pandas as pd
import numpy as np
import datetime
from epipipeline_v2.standardise.demographics import standardise_age
from epipipeline_v2.standardise.demographics import standardise_gender
from epipipeline_v2.standardise.gisdata.karnataka import get_sd_vill_v1
from epipipeline_v2.standardise.dates import parse_date
from epipipeline_v2.ud.download import download_file_from_URI
import warnings
from datetime import datetime as dt
import re
import requests
import yaml


def id2code(id_):
    return id_.split("_")[-1]


def id2code_dist(id_):
    if id_.startswith("district"):
        return id_.split("_")[-1]
    else:
        return 0


def id2code_ulb(id_):
    if id_.startswith("ulb"):
        return id_.split("_")[-1]
    else:
        return 0


def sanity(date):
    try:
        if date.date() <= datetime.date.today():
            return date
        else:
            return pd.NaT
    except TypeError:
        return pd.NaT


def standardise_ka_linelist_v1(preprocessed_data_dict, regionIDs_dict,
                               regionIDs_df, thresholds):

    standardised_data_dict = {}
    for districtID in preprocessed_data_dict.keys():

        df = preprocessed_data_dict[districtID].copy()

        # Demographics
        df = df[df.notnull().sum(axis=1) >= 10].reset_index(drop=True)
        df["demographics.age"] = df["demographics.age"].apply(standardise_age)
        df['demographics.gender'] = df['demographics.gender'].apply(standardise_gender)

        location_sd_vill = df.apply(lambda row: get_sd_vill_v1(
            row['location.district.ID'],
            row['location.subdistrict.name'],
            row['location.village.name'],
            regionIDs_dict=regionIDs_dict,
            regionIDs_df=regionIDs_df,
            thresholds=thresholds),
              axis=1
              )
        df = df.drop(columns=["location.subdistrict.name", "location.village.name"])
        location_df = pd.DataFrame([item for item in location_sd_vill],
                                   columns=["location.subdistrict.ID", "location.subdistrict.name",
                                            "location.village.ID", "location.village.name"
                                            ]
                                   )
        df = pd.concat([df, location_df], axis=1)
        df["location.district.name"] = regionIDs_dict[districtID]["regionName"]

        for date in ["event.symptomOnsetDate",
                     "event.test.sampleCollectionDate",
                     "event.test.resultDate"]:

            df[date] = df[date].apply(parse_date)

        standardised_data_dict[districtID] = df

    all_columns = set()
    for _, value in standardised_data_dict.items():
        all_columns = all_columns.union(set(value.columns.to_list()))

    missing_cols = set()
    for _, value in standardised_data_dict.items():
        missing_cols = missing_cols.union(all_columns.difference(set(value.columns.to_list())))

    missing_cols = list(missing_cols)

    df = df.drop(columns=missing_cols, errors='ignore')

    df = pd.concat(standardised_data_dict.values(), ignore_index=True)
    column_order = ['type', 'dashboard_date', 'state_code', 'district_code', 'subdistrict_code', 'ulb_code', 'village_code',
                    'zone_name', 'ward_number', 'phc', 'subcenter', 'lat', 'lng', 'age', 'gender', 'test_method',
                    'case_type', 'district_name', 'subdistrict_name', 'village_name', 'year']

    df["type"] = "individual"
    df["dashboard_date"] = df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])
    df["dashboard_date"] = pd.to_datetime(df["dashboard_date"], errors='coerce')
    df["dashboard_date"] = df["dashboard_date"].apply(sanity)

    df["district_code"] = df["location.district.ID"].apply(id2code)
    df["subdistrict_code"] = df["location.subdistrict.ID"].apply(id2code)
    df["village_code"] = df["location.village.ID"].apply(id2code)

    df["district_name"] = df["location.district.name"]
    df["subdistrict_name"] = df["location.subdistrict.name"]
    df["village_name"] = df["location.village.name"]

    df['state_code'] = 29

    empty_cols = ['zone_name', 'ward_number',
                  'phc', 'subcenter',
                  'lat', 'lng', 'year', 'test_method', 'ulb_code']
    for empty_col in empty_cols:

        df[empty_col] = None

    df["age"] = df["demographics.age"]
    df["gender"] = df["demographics.gender"]
    df["case_type"] = "confirmed"

    df = df[column_order]

    return df


# Function converts v2 summaries to v1

def get_ka_daily_summary_v1(summary):

    message = "The v1 format for Daily Summaries is outdated, and will not be supported in future releases."  # noqa

    warnings.warn(message, DeprecationWarning, stacklevel=2)
    # Convert to v1
    summary["type"] = "summary"
    summary["state_code"] = 29
    summary["district_code"] = summary["location.regionID"].apply(id2code_dist)
    summary["ulb_code"] = summary["location.regionID"].apply(id2code_ulb)

    summary = summary.rename({"record.date": "dashboard_date",
                              "daily.suspected": "suspected",
                              "daily.tested": "tested",
                              "daily.deaths": "mortality"
                              }, axis="columns")

    drop_cols = ["location.regionID", "location.regionName",
                 "daily.igm_positives", "daily.ns1_positives", "daily.total_positives",
                 "cumulative.suspected", "cumulative.tested",
                 "cumulative.igm_positives", "cumulative.ns1_positives",
                 "cumulative.total_positives", "cumulative.deaths"]
    summary = summary.drop(columns=drop_cols)
    add_keys = ['subdistrict_code', 'village_code', 'zone_name',
                'ward_number', 'phc', 'subcenter', 'lat', 'lng',
                'age', 'gender', 'test_method', 'case_type', 'district_name',
                'subdistrict_name', 'village_name', 'year']

    for key in add_keys:
        summary[key] = np.nan

    column_order = ['type', 'dashboard_date',
                    'state_code', 'district_code', 'subdistrict_code', 'ulb_code', 'village_code',
                    'zone_name', 'ward_number', 'phc', 'subcenter', 'lat', 'lng', 'age', 'gender',
                    'test_method', 'case_type', 'suspected', 'tested', 'mortality',
                    'district_name', 'subdistrict_name', 'village_name', 'year']

    summary = summary[column_order]

    return summary


def get_ka_daily_summary_v2(raw_URI, preprocess_metadata,
                            regionIDs_df, regionIDs_dict,
                            datadict_github_raw_url, dataset_info=None,
                            source_file="oneday", require_all_headers=True,
                            verbose=False):

    error = []
    if source_file != "oneday":
        e = "Only Single Day Files Supported"
        print(e)
        return None, [e]

    file = download_file_from_URI(raw_URI, extension="xlsx")

    date = dt.strptime(raw_URI.split("/")[-1].replace(".xlsx", ""), "%Y-%m-%d")

    header_row_indices = preprocess_metadata['indices']['header_row_indices']
    row_indices = eval(preprocess_metadata['indices']['row_indices'])
    col_indices = eval(preprocess_metadata['indices']['col_indices'])

    excel = pd.read_excel(file.name, header=None)

    given_headers = None
    for header_row_index in header_row_indices:
        if given_headers is None:
            given_headers = excel.iloc[:, col_indices].loc[header_row_index].fillna("")
        else:
            given_headers += excel.iloc[:, col_indices].loc[header_row_index].fillna("")

    given_headers = list(given_headers)
    summary_v2 = excel.iloc[row_indices, col_indices].reset_index(drop=True)

    reference_headers = preprocess_metadata["reference_headers"]
    preprocessed_headers = preprocess_metadata['preprocessed_headers']

    for i in range(len(given_headers)):
        head = str(given_headers[i]).lower()
        head = re.sub(' +', ' ', head)
        head = head.strip()

        if head != reference_headers[i]:
            e = reference_headers[i] + " not found for " + date.strftime("%Y-%m-%d") + ". Assuming hardcoded order regardless."
            if verbose:
                print(e)
            error += [e]

    summary_v2.columns = preprocessed_headers

    bbmp_posn = re.sub('[^A-Za-z0-9 ]+', '', str(summary_v2["s.no"].loc[31])).strip()

    if bbmp_posn == "Bangaluru City BBMP":
        summary_v2["regionName"].loc[31] = "BBMP"

    summary_v2 = summary_v2.drop("s.no", axis="columns")
    district_mapping = preprocess_metadata['district_mapping']

    mapping_df = pd.DataFrame(district_mapping.items(), columns=["regionID", "regionName"])

    summary_v2 = pd.merge(summary_v2, mapping_df, on="regionName", indicator="_success", how="outer")
    success = summary_v2[["regionID", "regionName", "_success"]]
    summary_v2 = summary_v2.drop("_success", axis="columns")

    failure = success[success._success != "both"].reset_index(drop=True)

    if len(failure) > 0:
        for index, row in failure.iterrows():
            if row['_success'] == "left_only":
                e = date.strftime("%Y-%m-%d") + ": District " + str(row["regionName"]) + " not found in reference."
                error += [e]
                if verbose:
                    print(e)
            elif row['_success'] == "right_only":
                districtID = row["regionID"]
                district_name = regionIDs_dict[districtID]["regionName"]
                e = "District " + district_name + " (" + districtID + ") not found in file for " + date.strftime("%Y-%m-%d") + "."
                error += [e]

                if verbose:
                    print(e)

    summary_v2 = pd.merge(summary_v2.drop("regionName", axis="columns"), regionIDs_df[["regionID", "regionName"]],
                          on="regionID", how="inner")
    summary_v2["record.date"] = date
    summary_v2 = summary_v2.rename(columns={"regionName": "location.regionName",
                                            "regionID": "location.regionID"
                                            }
                                   )
    datadict_response = requests.get(datadict_github_raw_url, allow_redirects=True)
    datadict = datadict_response.content.decode("utf-8")
    datadict = yaml.safe_load(datadict)

    accepted_headers = list(datadict["columns"].keys())

    if require_all_headers:
        accepted_headers_set = set(accepted_headers)
        headers_set = set(summary_v2.columns.to_list())

        diff1 = accepted_headers_set.difference(headers_set)
        diff2 = headers_set.difference(accepted_headers_set)
        if len(diff1) > 0:
            e = "Mismatch: " + str(list(diff1)) + " columns not found in file headers"
            error += [e]
            if verbose:
                print(e)
        if len(diff2) > 0:
            e = "Mismatch: " + str(list(diff2)) + " columns not found in file headers"
            error += [e]
            if verbose:
                print(e)

        summary_v2 = summary_v2[accepted_headers]

    summary_v2 = summary_v2.fillna(0)

    return summary_v2, error
