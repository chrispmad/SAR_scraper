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
table_id = 'tblPDiZ80zCvQrIUD'  # Main table ID

# Load update data
#merg_risk_status_update = pd.read_csv("data/species_at_risk_update_test.csv", encoding='utf-8-sig').astype(str)
merg_risk_status_update = pd.read_csv("output/risk_status_merged.csv", encoding='utf-8-sig')
merg_risk_status_update.fillna("NA", inplace=True)

#%% 
url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

#%% 
all_records = update_funcs.fetch_all_records(url, headers)

#%% 

date_fields = [
    "COSEWIC last assessment date", "Estimated re-assessment", "Scheduled Assessment",
    "Last status change", "Date added"
]

category_fields = ["Taxonomic group", "COSEWIC status"]
# Replace missing values for category fields
for field in category_fields:
    merg_risk_status_update[field] = merg_risk_status_update[field].replace(
        {pd.NA: "none", None: "none", "nan": "none", "NA": "none"}
    )

#%% 
update_funcs.update_rows(merg_risk_status_update, date_fields, url, headers, all_records)
# %%
