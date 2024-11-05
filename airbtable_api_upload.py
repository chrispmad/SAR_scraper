#%%
import os
from pyairtable import Api
from airtable import Airtable
import requests
import pandas as pd
import csv
import time
import json

# %%
with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()
    print(f"USERNAME = {username}")


base_id = 'appiMHrjH9cntzZC7' # the base of the workspace - change as appropriate
table_id = 'tblaXWXnP0t9Ggdze' # change
view_id = 'viwcjEPJe70stwTwe?blocks=hide' # change
unique_field = "Scientific name"


airtable_url = f"https://api.airtable.com/v0/{base_id}"

# %%
# Define the API endpoint and headers
url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

#with open("data/cosewic_spp_specialist_candidate_list.csv") as f:
#    csvFile = csv.reader(f)
#    for lines in csvFile:
#        print(lines)
          
fileToAdd = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')      

#%%
def get_existing_records():
    records = []
    offset = None
    
    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers = headers, params = params)
        if response.status_code !=200:
            print("Error fetching records: ", response.json())
            return records
        data = response.json()
        records.extend(data['records'])
        
        offset = data.get("offset")
        if not offset:
            break
    # Extract the unique field from each record
    existing_ids = {record['fields'].get(unique_field): record['id'] for record in records if unique_field in record['fields']}
    return existing_ids    
# %%

existing_records = get_existing_records()

#%%
for index, row in fileToAdd.iterrows():
    unique_value = row[unique_field]
    if unique_value in existing_records:
        print(f"{index} with {unique_field} already exsists. Skipping...")
        continue
    
    data = {
        "fields":row.to_dict()
    }
    response = requests.post(url, headers=headers, data = json.dumps(data))
    
    if response.status_code != 200:
        print(f"Error uploading row {index}: {response.json()}")
    time.sleep(0.2)
    
    
# %%

def delete_record_by_col(column_name, target_value):
    
     # Airtable formula to filter records with the target value in the specified column
    params = {
        'filterByFormula': f"{{{column_name}}} = '{target_value}'"
    }
    
     # Fetch records that match the target value
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print("Error fetching records:", response.json())
        return response.json()
    
    data = response.json()
    records = data.get("records", [])
    
    # If no records match the target value
    if not records:
        print(f"No records found with {column_name} = '{target_value}'.")
        return {"message": "No matching records found."}
    
    results = {}
    for record in records:
        record_id = record['id']
        delete_url = f"{url}/{record_id}"
        delete_response = requests.delete(delete_url, headers=headers)
        
        if delete_response.status_code == 200:
            print(f"Record with ID {record_id} deleted successfully.")
            results[record_id] = {"status": "deleted"}
        else:
            print(f"Error deleting record {record_id}: {delete_response.json()}")
            results[record_id] = {"status": "error", "details": delete_response.json()}
    
    return results


# %%
delete_record_by_col("Scientific name", "Spea bombifrons")
# %%
def upload_data(file_to_add, unique_field, existing_records):
    
    for index, row in file_to_add.iterrows():
        unique_value = row[unique_field]
        
        # Check if the record already exists
        if unique_value in existing_records:
            #print(f"Row {index} with {unique_field} '{unique_value}' already exists. Skipping...")
            continue
        
        # Prepare the data for uploading
        data = {
            "fields": row.to_dict()
        }
        
        # Make the POST request to upload data
        response = requests.post(url, headers=headers, data=json.dumps(data))
        
        # Check for success or error in the response
        if response.status_code == 200:
            print(f"Row {index} uploaded successfully!")
        #else:
        #    print(f"Error uploading row {index}: {response.json()}")

        # Sleep to respect rate limits of the Airtable API
        time.sleep(0.2)


#%%
upload_data(fileToAdd, unique_field , existing_records)
# %%
