import pandas as pd
import re
import os


def read_ka_linelist(raw_data_dir, sheet_codes, regionIDs_dict, verbose=False):

    raw_data_dict = dict()

    for file in os.listdir(raw_data_dir.name):
        if file.endswith(".xlsx"):

            xlsx_file = pd.ExcelFile(raw_data_dir.name + "/" + file)

            for sheet in xlsx_file.sheet_names:
                if "DEN" in sheet:
                    code = re.sub('[^A-Za-z]+', '', sheet.replace("DEN", ""))
                    if code in sheet_codes.keys():
                        raw_data_dict[sheet_codes[code]] = pd.read_excel(xlsx_file, sheet, header=None)

    missing_districts = set(sheet_codes.values()).difference(raw_data_dict.keys())
    error = []
    if len(missing_districts) != 0:
        for district in missing_districts:
            district_name = regionIDs_dict[district]["regionName"]
            error.append("District " + district_name + " (" + district + ") not present in provided directory.")
    else:
        error.append("All district tabs present in provided directory.")

    if verbose:
        for e in error:
            print(e)

    return raw_data_dict, error


def preprocess_ka_linelist_v2(raw_data_dict, preprocess_metadata,
                              accepted_headers, regionIDs_dict):

    error = []
    preprocessed_data_dict = {}
    for district in raw_data_dict.keys():

        district_name = regionIDs_dict[district]["regionName"]

        df = raw_data_dict[district].copy()  # Create a copy - you don't want to mutate the original dictionary
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # First Check for empty sheet
        if len(df) <= 1:
            e = "District " + district_name + " (" + district + ") has no data."
            error.append(e)
            print(e)
            continue

        # To account for empty excel sheets with one lone value in the 10000th row
        # Placing a semi-arbitrary cap of 5:
        # Any more than 5 rows with less than 12 values disqualifies the sheet

        min_cols = 12
        k = 0
        while (df.iloc[0].notnull().sum() < min_cols) and (k < 5):
            df = df.iloc[1:].reset_index(drop=True)
            k = k + 1

        if k == 5:
            e = "District " + district_name + " (" + district + ") has no data."
            error.append(e)
            print(e)
            continue

        if district in preprocess_metadata["no_merge_headers"]:
            # If the header is only one line.
            headers = list(df.iloc[0])
            df = df.iloc[1:].reset_index(drop=True)
        else:
            # If the header is multiple lines due to the NS1 IgM row
            headers = [str(head1) + " " + str(head2)
                       for head1, head2 in zip(df.iloc[0].fillna(""), df.iloc[1].fillna(""))
                       ]
            df = df.iloc[2:].reset_index(drop=True)

        # Checking if ns1 and igm columns are BOTH present
        ns1 = False
        igm = False

        for i in range(len(headers)):

            head = str(headers[i]).lower()

            # Removing special characters
            head = head.replace("\n", " ")
            head = head.replace("/", " ")

            # Remove extraneous spaces
            head = re.sub(' +', ' ', head)
            head = head.strip()
            if "ns1" in head:
                ns1 = True
            if "igm" in head or "ig m" in head:
                igm = True

            headers[i] = head

        df.columns = headers

        # Correct any header errors specific to the district
        if district in preprocess_metadata["header_mapper"]["district_specific_errors"].keys():

            header_mapper = {}
            for standard_name, name_options in preprocess_metadata["header_mapper"]["district_specific_errors"][district].items():
                for option in name_options:
                    header_mapper[option] = standard_name

            df = df.rename(column=header_mapper)

        # Rename all recognised columns to standard names

        header_mapper = {}
        for standard_name, name_options in preprocess_metadata['header_mapper']['all'].items():
            for option in name_options:
                header_mapper[option] = standard_name

        df = df.rename(columns=header_mapper)

        df["location.district.ID"] = district

        if ns1:
            df["event.test.test1.code"] = "91064-6"
            df["event.test.test1.name"] = "Dengue virus NS1 Ag [Presence] in Serum or Plasma by Immunoassay"
        if igm and ns1:
            df["event.test.test2.code"] = "25338-5"
            df["event.test.test2.name"] = "Dengue virus IgM Ab [Presence] in Serum"

        # The Column Mapper assumes IgM is always Test #2.
        # If NS1 is missing, this code makes IgM Test #1
        if igm and not ns1:
            df = df.rename(columns={"event.test.test2.result": "event.test.test1.result"})
            df["event.test.test1.code"] = "25338-5"
            df["event.test.test1.name"] = "Dengue virus IgM Ab [Presence] in Serum"

        # Only taking accepted columns, and ordering as per datadictionary
        headers = [head for head in df.columns.to_list() if head in accepted_headers]
        headers = sorted(headers, key=accepted_headers.index)

        df = df[headers]

        if district in preprocess_metadata["required_headers"]["district_specific"].keys():
            required_headers = preprocess_metadata["required_headers"]["district_specific"][district]
        else:
            required_headers = preprocess_metadata["required_headers"]["general"]

        absent_headers = [head for head in required_headers if head not in df.columns.to_list()]

        if len(absent_headers) > 0:
            e = "District " + district_name + " (" + district + \
                ") is missing " + str(len(absent_headers)) + " header(s): " + \
                ", ".join(absent_headers) + "."
            error.append(e)
            print(e)

        preprocessed_data_dict[district] = df

    return preprocessed_data_dict, error
