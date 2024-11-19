#%%
import os
from pyairtable import Api
from airtable import Airtable
import requests
import pandas as pd
import csv
import time
import json
import functions.airtable_functions as airfuncs


merged_risk_status = pd.read_csv("output/risk_status_merged.csv", encoding='utf-8-sig')
merged_spp_candidate = pd.read_csv("output/merged_spp_candidate.csv", encoding='utf-8-sig')
#fileToAdd = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')  



with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()
    print(f"USERNAME = {username}")

#fileToAdd = pd.read_csv("data/risk_registry.csv", encoding='utf-8-sig')      
#fileToAdd = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')  

base_id = 'applZn1P0abVQM8NC' # the base of the workspace - change as appropriate 
table_id = 'tblPDiZ80zCvQrIUD' # Risk registry

# Define the API endpoint and headers
url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Get the current records uploaded to Airtable
current_records = airfuncs.fetch_records(url, headers, base_id, table_id)
# Delete all records from the current Airtable page
if current_records:            
    airfuncs.delete_records(url, base_id, table_id, headers, current_records)
    
# convert all types of file to string - this should be changed later to be appropriate types
merged_risk_status = merged_risk_status.astype(str) 

#upload the new file
for index, row in merged_risk_status.iterrows():
    data = {"fields": row.to_dict()}

    
    print(data)
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
      print(f"Error uploading row {index}: {response.json()}")

    time.sleep(0.2)  # Add a delay between requests 



######################
# Next Table
######################

table_id = 'tblESIlv9ie05ab7z' # Risk registry


url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Get the current records uploaded to Airtable
current_records = airfuncs.fetch_records(url, headers, base_id, table_id)
# Delete all records from the current Airtable page
if current_records:            
    airfuncs.delete_records(url, base_id, table_id, headers, current_records)
    
# convert all types of file to string - this should be changed later to be appropriate types
merged_spp_candidate = merged_spp_candidate.astype(str) 

#upload the new file
for index, row in merged_spp_candidate.iterrows():
    data = {"fields": row.to_dict()}

    
    print(data)
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
      print(f"Error uploading row {index}: {response.json()}")

    time.sleep(0.2)  # Add a delay between requests





# %%
