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

with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()
    print(f"USERNAME = {username}")

fileToAdd = pd.read_csv("data/risk_registry.csv", encoding='utf-8-sig')      
#fileToAdd = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')  

base_id = 'applZn1P0abVQM8NC' # the base of the workspace - change as appropriate
table_id = 'tblmLlhGM6XKVo7WI' # Risk registry

# Define the API endpoint and headers
url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

unique_field = "Scientific name"

search_string = ["BC" , "British Columbia" , "Pacific", "British"]

fileToAdd = fileToAdd[fileToAdd.apply(lambda row: row.str.contains(
    '|'.join(search_string), case = False)).any(axis=1)]








#with open("data/cosewic_spp_specialist_candidate_list.csv") as f:
#    csvFile = csv.reader(f)
#    for lines in csvFile:
#        print(lines)
          

fileToAdd = fileToAdd.fillna('')
if 'index' in fileToAdd.columns:
    fileToAdd['index'] = fileToAdd['index'].astype(str)

fileToAdd = airfuncs.create_unique_id(fileToAdd)

# Get the current records uploaded to Airtable
current_records = airfuncs.fetch_records(url, headers, base_id, table_id)
# Delete all records from the current Airtable page
if current_records:            
    airfuncs.delete_records(url, base_id, table_id, headers, current_records)


# convert all types of file to string - this should be changed later to be appropriate types
fileToAdd = fileToAdd.astype(str)


#upload the new file
for index, row in fileToAdd.iterrows():
    data = {"fields": row.to_dict()}

    
    print(data)
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
      print(f"Error uploading row {index}: {response.json()}")

    time.sleep(0.2)  # Add a delay between requests (optional)

# %%
