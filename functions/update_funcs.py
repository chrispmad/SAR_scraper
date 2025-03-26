import os
import requests
import pandas as pd
import json
import time
import re
from datetime import datetime

def fetch_all_records(url, headers):
    """Fetch all records from an Airtable table."""
    all_records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"⚠️ Error fetching records: {response.json()}")
            return []
        data = response.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return all_records


def find_record_id(unique_id, all_records):
    """Finds the Airtable record ID by searching the fetched records."""
    cleaned_unique_id = clean_unique_id(unique_id)
    for record in all_records:
        if clean_unique_id(record['fields'].get('Unique_ID', '')) == cleaned_unique_id:
            return record['id'], record['fields']  # Return both ID and existing fields
    print(f"❌ Record not found for Unique_ID: {unique_id}, Cleaned ID: {cleaned_unique_id}")
    return None, None


def clean_unique_id(unique_id):
    """Cleans the Unique_ID string."""
    cleaned_id = unique_id.strip()
    cleaned_id = re.sub(r'[^\x20-\x7E]', '', cleaned_id)  # Remove non-printable characters
    return cleaned_id


def update_rows(data, date_fields, url, headers, all_records):
    """Syncs records in Airtable: updates existing, adds new, deletes missing."""
    date_formats = ["%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]  # Supported input formats
    
    airtable_ids = {rec['fields'].get('Unique_ID'): rec['id'] for rec in all_records if 'fields' in rec}
    uploaded_ids = set(data['Unique_ID'].dropna().astype(str))
    
    # Process each row in the uploaded data
    for index, row in data.iterrows():
        row_dict = row.to_dict()
        unique_id = row_dict.get("Unique_ID")
        if not unique_id:
            print(f"⏭️ Skipping row {index} due to missing Unique_ID")
            continue
        
        # Process date fields
        for field in date_fields:
            if field in row_dict and pd.notna(row_dict[field]):
                if isinstance(row_dict[field], pd.Timestamp):
                    row_dict[field] = row_dict[field].strftime("%Y-%m-%d")
                elif isinstance(row_dict[field], str):
                    row_dict[field] = row_dict[field].split()[0]  # Remove time component
                else:
                    print(f"⚠️ Unexpected format in {field}: {row_dict[field]}")
        
        if unique_id in airtable_ids:
            # Update existing record if changed
            record_id = airtable_ids[unique_id]
            existing_record = next((rec['fields'] for rec in all_records if rec['id'] == record_id), {})
            if row_dict == existing_record:
                continue  # Skip update if no changes
            
            record_url = f"{url}/{record_id}"
            update_data = {"fields": row_dict}
            try:
                print(f"🚀 Updating Airtable row {index} (Unique_ID: {unique_id})...")
                response = requests.patch(record_url, headers=headers, data=json.dumps(update_data), timeout=10)
                if response.status_code != 200:
                    print(f"⚠️ Error updating row {index} (Unique_ID: {unique_id}): {response.json()}")
                time.sleep(0.2)
            except requests.exceptions.RequestException as e:
                print(f"🌐 Network error: {e}")
                break
        else:
            # Add new record
            try:
                print(f"➕ Adding new record for Unique_ID: {unique_id}")
                response = requests.post(url, headers=headers, data=json.dumps({"fields": row_dict}), timeout=10)
                if response.status_code != 200:
                    print(f"⚠️ Error adding Unique_ID {unique_id}: {response.json()}")
                time.sleep(0.2)
            except requests.exceptions.RequestException as e:
                print(f"🌐 Network error: {e}")
                break
    
    # Delete records that are no longer in the uploaded dataset
    records_to_delete = [record_id for uid, record_id in airtable_ids.items() if uid not in uploaded_ids]
    for record_id in records_to_delete:
        record_url = f"{url}/{record_id}"
        try:
            print(f"🗑️ Deleting record {record_id} from Airtable...")
            response = requests.delete(record_url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"⚠️ Error deleting record {record_id}: {response.json()}")
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network error: {e}")
            break