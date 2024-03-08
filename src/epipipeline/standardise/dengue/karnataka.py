import pandas as pd
import numpy as np
import datetime
from epipipeline.standardise.demographics import standardise_age
from epipipeline.standardise.demographics import standardise_gender
from epipipeline.standardise.gisdata.karnataka import get_sd_vill_v1
from epipipeline.standardise.dates import validate_dates
from epipipeline.ud.download import download_file_from_URI
from epipipeline.ud.upload import upload_files
import boto3
import tempfile
import warnings
from datetime import datetime as dt
import re
import requests
import yaml


pd.set_option('future.no_silent_downcasting', True)


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


def clean_test_method(series, test_type):

    def clean_string(string):
        # Convert to lowercase
        string = string.lower()

        # Remove special characters except '+' and '-'
        string = re.sub(r'[^a-zA-Z0-9+\-]+', '', string)
        string = re.sub(r'\s+', ' ', string).strip()

        return string

    def contains_keywords(string):
        if test_type == "ns1":
            keywords = ["ns1", "positive", "+ve"]
            if any(keyword in string for keyword in keywords) or string == "1":
                return True
            else:
                return pd.NA
        elif test_type == "igm":
            keywords = ["igm", "mac elisa", "positive", "+ve"]
            if any(keyword in string for keyword in keywords) or string == "1":
                return True
            else:
                return pd.NA

    cleaned_series = series.apply(clean_string)
    contains_keywords_series = cleaned_series.apply(contains_keywords)

    return contains_keywords_series


def standardise_ka_linelist_v1(preprocessed_data_dict, regionIDs_dict,
                               regionIDs_df, thresholds, year, accepted_headers, version="v2"):

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

        date_columns = ["event.symptomOnsetDate", "event.test.sampleCollectionDate", "event.test.resultDate"]

        df = validate_dates(df=df, year_of_data=year, date_columns=date_columns)

        df["event.test.test1.result"] = clean_test_method(df["event.test.test1.result"].astype(str), test_type="ns1")
        if "event.test.test2.result" in df.columns:
            df["event.test.test2.result"] = clean_test_method(df["event.test.test2.result"].astype(str), test_type="igm")

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

    if version == "v2":
        df["metadata.primaryDate"] = df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])  # noqa: E501

        headers = [head for head in df.columns.to_list() if head in accepted_headers]
        headers = sorted(headers, key=accepted_headers.index)
        df = df[headers]

        return df
    elif version == "v1":

        message = "The v1 format for Linelists is deprecated, and will not be supported in future releases."  # noqa
        warnings.warn(message, DeprecationWarning, stacklevel=3)

        column_order = ['type', 'dashboard_date', 'state_code', 'district_code', 'subdistrict_code', 'ulb_code', 'village_code',
                        'zone_name', 'ward_number', 'phc', 'subcenter', 'lat', 'lng', 'age', 'gender', 'test_method',
                        'case_type', 'district_name', 'subdistrict_name', 'village_name', 'year']

        df["type"] = "individual"
        df["dashboard_date"] = df["event.symptomOnsetDate"].fillna(df["event.test.sampleCollectionDate"]).fillna(df["event.test.resultDate"])  # noqa: E501
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

    message = "The v1 format for Daily Summaries is deprecated, and will not be supported in future releases."  # noqa
    warnings.warn(message, DeprecationWarning, stacklevel=3)

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
        summary_v2.loc[31, "regionName"] = "BBMP"

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

    accepted_headers = list(datadict["fields"].keys())

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


def update_summaries_by_day_on_S3(raw_URI_Prefix, std_URI_Prefix, year, metadata,
                                  regionIDs_df, regionIDs_dict,
                                  version="v2", START_DATE=dt.strptime("2023-07-01", "%Y-%m-%d"),
                                  verbose=False
                                  ):

    s3 = boto3.client('s3')

    dse_all = []

    # Get all objects available in source bucket

    raw_Bucket = raw_URI_Prefix.removeprefix("s3://").split("/")[0]
    raw_Prefix = raw_URI_Prefix.removeprefix("s3://").removeprefix(raw_Bucket + "/")

    raw_objects = s3.list_objects_v2(Bucket=raw_Bucket, Prefix=raw_Prefix + str(year) + "-")
    # raw_keys = [obj["Key"] for obj in raw_objects.get("Contents", []) if obj["Size"] > 0]
    raw_dates = [obj["Key"].split("/")[-1].removesuffix(".xlsx")
                 for obj in raw_objects.get("Contents", []) if obj["Size"] > 0]

    # Get all objects in the destination bucket

    std_Bucket = std_URI_Prefix.removeprefix("s3://").split("/")[0]
    std_Prefix = std_URI_Prefix.removeprefix("s3://").removeprefix(std_Bucket + "/")

    std_objects = s3.list_objects_v2(Bucket=std_Bucket, Prefix=std_Prefix + str(year) + "-")
    # std_keys = [obj["Key"] for obj in std_objects.get("Contents", []) if obj["Size"] > 0]
    std_dates = [obj["Key"].split("/")[-1].removesuffix(".csv")
                 for obj in std_objects.get("Contents", []) if obj["Size"] > 0]

    to_standardise = list(set(raw_dates).difference(set(std_dates)))

    tmpdir = tempfile.TemporaryDirectory()

    if len(to_standardise) == 0:
        e = "No fresh daily summaries available"
        dse_all += [e]
        if verbose:
            print(e)

    for date in to_standardise:
        if dt.strptime(date, "%Y-%m-%d") >= START_DATE:

            raw_URI = raw_URI_Prefix + date + ".xlsx"

            summary, dse = get_ka_daily_summary_v2(raw_URI=raw_URI,
                                                   preprocess_metadata=metadata['preprocess'],
                                                   regionIDs_df=regionIDs_df,
                                                   regionIDs_dict=regionIDs_dict,
                                                   datadict_github_raw_url=metadata['datadictionary_github_raw_url'],
                                                   verbose=verbose
                                                   )
            if version == "v1":
                summary = get_ka_daily_summary_v1(summary)

            std_fname = tmpdir.name + "/" + date + ".csv"
            summary.to_csv(path_or_buf=std_fname, index=False)

            std_Key = std_Prefix + date + ".csv"
            upload_files(Bucket=std_Bucket, Key=std_Key, Filename=std_fname)

            e = "Daily Summary " + str(version) + " Upload Successful for " + str(date)
            dse += [e]
            if verbose:
                print(e)

            if len(dse) > 0:
                dse_all += dse

    return dse_all
