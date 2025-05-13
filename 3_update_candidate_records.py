#%%

import os
import requests
import pandas as pd
import json
import time
import re
from datetime import datetime
import functions.update_funcs as update_funcs

# Load Airtable API credentials
with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()

base_id = 'applZn1P0abVQM8NC'
table_id = 'tblESIlv9ie05ab7z'  # Main table ID

#candidate_update = pd.read_csv("data/candidate_species_tbl_update_test.csv")
candidate_update = pd.read_csv("output/merged_spp_candidate.csv")
url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

all_records = update_funcs.fetch_all_records(url, headers)

date_fields = []

category_fields = ["Domain", "Candidate list", "Priority"]

# Convert all columns in merged_spp_candidate to strings and standardize missing values
candidate_update = candidate_update.astype(str).applymap(lambda x: "NA" if pd.isna(x) or str(x).strip().lower() == "nan" else x)


# Standardize "Date nominated"
candidate_update["Date nominated"] = candidate_update["Date nominated"].replace(
    {pd.NA: " ", None: " ", "nan": " ", "NA": " "}
)

candidate_update["Date nominated"] = pd.to_numeric(candidate_update["Date nominated"], errors='coerce')
candidate_update["Date nominated"] = candidate_update["Date nominated"].fillna(9999)

update_funcs.update_rows(candidate_update, date_fields, url, headers, all_records)

# %%
