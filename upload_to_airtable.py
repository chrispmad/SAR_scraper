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
    
# convert all types of file to string - this should be changed later to be appropriate types
merged_risk_status = merged_risk_status.astype(str) 

merged_risk_status.fillna("NA", inplace=True)  # Use "" for blanks if preferred
merged_risk_status = merged_risk_status.applymap(lambda x: "NA" if str(x).lower() == "nan" else x)
merged_risk_status["Date added"] = merged_risk_status["Date added"].fillna("1900-01-01").replace("nan", "1900-01-01").replace("NA", "1900-01-01")
merged_risk_status["Taxonomic group"] = merged_risk_status["Taxonomic group"].fillna("none").replace("nan", "none").replace("NA", "none")
merged_risk_status["Last status change"] = merged_risk_status["Last status change"].fillna("1900-01-01").replace("nan", "1900-01-01").replace("NA", "1900-01-01")
merged_risk_status["Scheduled Assessment"] = merged_risk_status["Scheduled Assessment"].fillna("1900-01-01").replace("nan", "1900-01-01").replace("NA", "1900-01-01").replace("No date found", "1900-01-01")
merged_risk_status["COSEWIC last assessment date"] = merged_risk_status["COSEWIC last assessment date"].fillna("1900-01-01").replace("nan", "1900-01-01").replace("NA", "1900-01-01")
merged_risk_status["Estimated re-assessment"] = merged_risk_status["Estimated re-assessment"].fillna("1900-01-01").replace("nan", "1900-01-01").replace("NA", "1900-01-01")


merged_risk_status["COSEWIC status"] = merged_risk_status["COSEWIC status"].fillna("none").replace("nan", "none").replace("NA", "none")
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

#upload the new file
# Upload each row

# Define which columns should be treated as multiple select fields
# Airtable data requirements for multiple select fields are that they are an array of strings
# add more column names here when they need to be multiple select in Airtable
multiple_select_fields = ["COSEWIC status", "Taxonomic group"]

for index, row in merged_risk_status.iterrows():
    row_dict = row.to_dict()

    # Convert multiple select fields to lists if they are not already
    for field in multiple_select_fields:
        if field in row_dict and isinstance(row_dict[field], str):  
            row_dict[field] = [item.strip() for item in row_dict[field].split(",")]

    data = {"fields": row_dict}  

    #print(f"Uploading row {index}: {json.dumps(data, indent=2)}")

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)

        if response.status_code != 200:
            print(f"Error uploading row {index}: {response.json()}")

        time.sleep(0.2)  # Small delay to prevent rate limits

    except requests.exceptions.Timeout:
        print(f"Timeout error for row {index}: Server took too long to respond.")
    except requests.exceptions.RequestException as e:
        print(f"Network error while uploading row {index}: {e}")
        break  # Stop if there's a major issue




#%%
######################
# Next Table
######################
base_id = 'applZn1P0abVQM8NC' # the base of the workspace - change as appropriate 
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
merged_spp_candidate = merged_spp_candidate.applymap(lambda x: "NA" if str(x).lower() == "nan" else x)
merged_spp_candidate["Date nominated"] = merged_spp_candidate["Date nominated"].fillna(" ").replace("nan", " ").replace("NA", " ")



#upload the new file
for index, row in merged_spp_candidate.iterrows():
    data = {"fields": row.to_dict()}

    
    #print(data)
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
      print(f"Error uploading row {index}: {response.json()}")

    time.sleep(0.2)  # Add a delay between requests





# %%
