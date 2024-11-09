
from pyairtable import Api
from airtable import Airtable
import requests
import pandas as pd
import csv
import time
import json


# Deletes a record from Airtable, based on the column name and the string you pass it.
# takes the name of the column to use a unique identifier - if you delete Group - Amphibians, then they will all get deleted
# urls etc need to be set before hand.
def delete_record_by_col(url, base_id, table_id, headers, column_name, target_value):
    
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


# upload the data to the airtable table that has been identified. Make sure they are not existing records.
def upload_data(url, base_id, table_id, headers, file_to_add):
    
    for index, row in file_to_add.iterrows():
           
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

def fetch_records(url, headers, base_id, table_id):
    records = []
    
    while True:
        response = requests.get(url, headers=headers)
        response_json = response.json()
        records.extend(response_json.get('records', []))
        
        offset = response_json.get('offset')
        if not offset:
            break
        url = f'https://api.airtable.com/v0/{base_id}/{table_id}?offset={offset}'
    return records

def delete_records(url, base_id, table_id, headers, records):
    record_id_to_delete = [record['id'] for record in records]
    batch_size = 10
    #print(record_id_to_delete)
    for i in range(0, len(record_id_to_delete), batch_size):
        print(record_id_to_delete[i:i+batch_size])
        batch = record_id_to_delete[i:i+batch_size]
        url = f'https://api.airtable.com/v0/{base_id}/{table_id}?' + '&'.join([f'records[]={id}' for id in batch])
        response = requests.delete(url=url, headers=headers)
        if response.status_code == 200:
            print(f"Successfully deleted batch: {batch}")
        else:
            print(f"Error deleting batch. Response: {response.text}")    

            
def create_unique_id(df):
    df['UniqueID'] = df.apply(lambda row: 
                              f"{row['Scientific name']} - {row['COSEWIC common name'] or '<blank>'} - {row['COSEWIC population'] or row['Legal population']}", 
                              axis=1)
    return df      