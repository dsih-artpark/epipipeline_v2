import pandas as pd
import datetime
from epipipeline_v2.standardise.demographics import standardise_age
from epipipeline_v2.standardise.demographics import standardise_gender
from epipipeline_v2.standardise.gisdata.karnataka import get_sd_vill_v1
from epipipeline_v2.standardise.dates import parse_date


def id2code(id_):
    return id_.split("_")[-1]


def sanity(date):
    try:
        if date.date() <= datetime.date.today():
            return date
        else:
            return pd.NaT
    except TypeError:
        return pd.NaT


def standardise_ka_v1(preprocessed_data_dict, regionIDs_dict,
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
    for key, value in standardised_data_dict.items():
        all_columns = all_columns.union(set(value.columns.to_list()))

    missing_cols = set()
    for key, value in standardised_data_dict.items():
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
