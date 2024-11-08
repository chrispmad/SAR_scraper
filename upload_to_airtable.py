#%%
import os
from pyairtable import Api
from airtable import Airtable
import requests
import pandas as pd
import csv
import time
import json

#upload data
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

#get existing records 
#%%
# This will pull the table from airtable and check if they already exist in the file to be uploaded
# the url, table, headers, and params all need to be set outside of this function 
# the unique field is the parameter that you are searching for - again set outside the function.
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


#delete records
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

fileToAdd = pd.read_csv("data/risk_registry.csv", encoding='utf-8-sig')      

copydata = fileToAdd
search_string = ["BC" , "British Columbia" , "Pacific", "British"]

fileToAdd = copydata[copydata.apply(lambda row: row.str.contains(
    '|'.join(search_string), case = False)).any(axis=1)]


#fileToAdd = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')      

with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()
    print(f"USERNAME = {username}")


base_id = 'applZn1P0abVQM8NC' # the base of the workspace - change as appropriate
table_id = 'tblINdkiG9Nkfb44q' # change
""" view_id = 'viwcjEPJe70stwTwe?blocks=hide' # change """
unique_field = "Scientific name"


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
          
#%%
fileToAdd = fileToAdd.fillna('')
if 'index' in fileToAdd.columns:
    fileToAdd['index'] = fileToAdd['index'].astype(str)
# %%

for i in range(5):
    try:
        index, row = next(fileToAdd.iterrows())
        data = {"fields": row.to_dict()}
        print(json.dumps(data))
        
        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code != 200:
            print(f"Error uploading row {index}: {response.json()}")

        time.sleep(0.2)  # Add a delay between requests (optional)
    except StopIteration:
        # Reached the end of the generator (no more rows)
        break


    
    
#%%
""" for column_name in fileToAdd.columns:
    # Create a new DataFrame with only the current column and its first 4 rows
    column_df = fileToAdd[[column_name]].head(4)

    for index, row in column_df.iterrows():
        data = {"fields": {column_name: row[column_name]}}  # Send only the current column

        print(data)
        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code != 200:
            print(f"Error uploading row {index} for column {column_name}: {response.json()}")

        time.sleep(0.2)  # Add a delay between requests (optional)    """

# %%
""" delete_record_by_col("Scientific name", "Spea bombifrons") """

#%%

fileToAdd = fileToAdd.astype(str)

for index, row in fileToAdd.iterrows():
    data = {"fields": row.to_dict()}

    
    print(data)
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
      print(f"Error uploading row {index}: {response.json()}")

    time.sleep(0.2)  # Add a delay between requests (optional)
# %%
