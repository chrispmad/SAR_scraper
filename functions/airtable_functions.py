#%%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import csv
import re
import os

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

#%%
# Deletes a record from Airtable, based on the column name and the string you pass it.
# takes the name of the column to use a unique identifier - if you delete Group - Amphibians, then they will all get deleted
# urls etc need to be set before hand.
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

#%%
# upload the data to the airtable table that has been identified. Make sure they are not existing records.
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
