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
merged_risk_status = merged_risk_status.replace("nan", pd.NA)
merged_spp_candidate = merged_spp_candidate.replace("nan", pd.NA)

with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()
    print(f"USERNAME = {username}")
    
 
 
# Function to clean up NaN values and format "Taxonomic group" as a list
def clean_row(row):
    cleaned_row = {}
    for key, value in row.items():
        if pd.isna(value):  
            cleaned_row[key] = None  # Convert NaN to None
        elif key == "Taxonomic group":  
            cleaned_row[key] = [item.strip() for item in str(value).split(",")]  # Convert to list
        elif key == "COSEWIC status":
            cleaned_row[key] = [item.strip() for item in str(value).split(",")]  # Convert to list   
        else:
            cleaned_row[key] = value  
    return cleaned_row    
#%%



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
    


#merged_risk_status_cleaned = merged_risk_status.apply(clean_row, axis=1)

#%%

#upload the new file
# Upload each row

# Define which columns should be treated as multiple select fields
# Airtable data requirements for multiple select fields are that they are an array of strings
# add more column names here when they need to be multiple select in Airtable
multiple_select_fields = ["COSEWIC status", "Taxonomic group"]

for index, row in merged_risk_status.iterrows():
    row_dict = clean_row(row.to_dict())  # Apply clean_row function

    # Ensure multiple select fields are single strings (pick first value if list)
    for field in multiple_select_fields:
        if field in row_dict:
            if isinstance(row_dict[field], list):  
                row_dict[field] = row_dict[field][0] if row_dict[field] else None  # Convert list to single value
            elif isinstance(row_dict[field], str):
                row_dict[field] = row_dict[field].strip()  # Ensure it's a clean string

    data = {"fields": row_dict}

    
    #print(f"Uploading row {index}: {json.dumps(data, indent=2)}")  # Debugging

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)

        if response.status_code != 200:
            print(f"Error uploading row {index}: {response.json()}")

        time.sleep(0.2)

    except requests.exceptions.Timeout:
        print(f"Timeout error for row {index}")
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        break




#%%
######################
# Next Table
######################
base_id = 'applZn1P0abVQM8NC' # the base of the workspace - change as appropriate 
table_id = 'tblESIlv9ie05ab7z' # Risk registry


multiple_select_fields = ["Candidate list", "Priority"]

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
    


#upload the new file
for index, row in merged_spp_candidate.iterrows():
    row_dict = clean_row(row.to_dict())  # Apply clean_row function

    # Ensure multiple select fields are single strings (pick first value if list)
    for field in multiple_select_fields:
        if field in row_dict:
            if isinstance(row_dict[field], list):  
                row_dict[field] = row_dict[field][0] if row_dict[field] else None  # Convert list to single value
            elif isinstance(row_dict[field], str):
                row_dict[field] = row_dict[field].strip()  # Ensure it's a clean string

    # Convert "Taxonomic group" to a string if it's a list
    if "Taxonomic group" in row_dict and isinstance(row_dict["Taxonomic group"], list):
        row_dict["Taxonomic group"] = ", ".join(row_dict["Taxonomic group"])  # Convert list to a comma-separated string

    # Convert "Date nominated" to an integer (year only) **without a function**
    if "Date nominated" in row_dict:
        try:
            row_dict["Date nominated"] = int(row_dict["Date nominated"])  # Convert directly
        except (ValueError, TypeError):
            row_dict["Date nominated"] = None  # Handle invalid values
    
    data = {"fields": row_dict}

    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        print(f"Error uploading row {index}: {response.json()}")

    time.sleep(0.2)  # Add a delay between requests





# %%
