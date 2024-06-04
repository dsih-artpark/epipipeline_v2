import logging

import requests
import yaml
from dataio.download import download_dataset_v2
from epipipeline import get_regionIDs
from epipipeline.logging_configure import logging_configure
from epipipeline.preprocess.dengue.karnataka import fetch_ka_linelist_v2

logging_configure(log_level=logging.INFO, log_file='app.log')

metadata_github_raw_url = "https://raw.githubusercontent.com/dsih-artpark/catalogue/epipipeline/info/EP/EP0005DS0014-KA_Dengue_LL/metadata.yaml"

metadata_response = requests.get(metadata_github_raw_url, allow_redirects=True)
metadata = metadata_response.content.decode("utf-8")
metadata = yaml.safe_load(metadata)

year = 2024
verbose = True

download_dataset_v2(dsid="GS0015DS0034")
regionIDs_df, regionIDs_dict = get_regionIDs()

raw_data_dict = fetch_ka_linelist_v2(dsid=metadata["admin"]["ds_ids"]["raw"],
                                     sheet_codes=metadata["admin"]["config"]["sheet_codes"],
                                     regionIDs_dict=regionIDs_dict,
                                     expected_files=metadata["admin"]["config"]["expected_files"][year],
                                     year=year)


