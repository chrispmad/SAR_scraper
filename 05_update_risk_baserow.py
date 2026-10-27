#%%
import pandas as pd
import time
from datetime import datetime
from baserowapi import Baserow
import json
import sys 

# --- CONFIG ---
TOKEN_FILE = "login/baserow_token.txt" # Ensure this file contains your JWT!
CSV_FILE = "output/risk_status_merged.csv"
TABLE_ID = 730152
DATE_FIELDS = [
    "COSEWIC last assessment date",
    "Estimated re-assessment",
    "Scheduled Assessment",
    "Last status change",
    "Date added",
]
CHUNK_SIZE = 50 # Only used for 'add_rows'
KEY_COLUMN = "Unique_ID"
DEFAULT_DATE = "1900-01-01" 

# --- Initialize Baserow Client ---
try:
    with open(TOKEN_FILE) as f:
        BASEROW_TOKEN = f.readline().strip() 

    baserow = Baserow(token=BASEROW_TOKEN)
    table = baserow.get_table(TABLE_ID)
except Exception as e:
    print(f"🛑 Initialization Error (Check Token/ID): {e}")
    sys.exit(1)

# --- Load CSV ---
try:
    df = pd.read_csv(CSV_FILE, encoding="utf-8-sig")
    df[KEY_COLUMN] = df[KEY_COLUMN].astype(str).str.strip()
    df.replace([pd.NA, None], "NA_MARKER", inplace=True) 
except Exception as e:
    print(f"🛑 CSV Loading Error: {e}")
    sys.exit(1)


# --- Get Existing Rows and Build Map ---

# Fetch all field names once
try:
    field_names = table.field_names
    existing_rows = table.get_rows(include=field_names) 
except Exception as e:
    print(f"🛑 Baserow API Error during Field/Row Fetch (Check Token/Table Permissions): {e}")
    sys.exit(1)

# Build map: {trimmed_unique_id: baserow_row_object}
record_map = {}
for r in existing_rows:
    try:
        # Access the primary key value using square bracket notation
        key_value = r[KEY_COLUMN] 
    except KeyError:
        print(f"Warning: Primary key '{KEY_COLUMN}' not found in a fetched row.")
        continue
    
    if key_value:
        record_map[key_value.strip()] = r


# --- Prepare updates and inserts with Change Detection ---
batch_updates_payload = []
batch_creates_payload = []

print("🔎 Starting change detection...")

for _, row in df.iterrows():
    row_data = {} 
    is_different = False
    
    name = row[KEY_COLUMN] 
    is_update = name in record_map

    for col in df.columns:
        # 1. Prepare the new value from CSV/DF
        val = row[col]
        
        if col in DATE_FIELDS:
            if val == "NA_MARKER":
                val = DEFAULT_DATE
            else:
                try:
                    val = pd.to_datetime(val).strftime("%Y-%m-%d")
                except Exception:
                    val = DEFAULT_DATE
        
        # 2. Use empty string "" for blank Text/Long Text fields.
        elif val == "NA_MARKER":
            val = ""

        # --- Change Detection Logic (Only for updates) ---
        if is_update:
            baserow_row = record_map[name]
            
            # Use .to_dict() for safe comparison of field values
            existing_row_dict = baserow_row.to_dict() 
            existing_val = existing_row_dict.get(col) 

            # Normalize existing value for comparison
            if existing_val is None or str(existing_val) == "None" or str(existing_val).strip() == "":
                 existing_val_norm = ""
            else:
                 existing_val_norm = str(existing_val)
                 
            # Compare the new value (val) with the existing normalized value
            if str(val) != existing_val_norm:
                is_different = True
        
        # Add field to payload using the column name
        row_data[col] = val
    
    # Final assignment to batch lists
    if is_update:
        if is_different:
            # Access 'id' as a direct attribute of the Row object
            row_id = record_map[name].id
            batch_updates_payload.append({"id": row_id, **row_data})
    else:
        batch_creates_payload.append(row_data)

print(f"Summary: {len(batch_updates_payload)} rows to update, {len(batch_creates_payload)} rows to create.")


# --- Batch Update Existing Rows ---
if batch_updates_payload:
    print(f"Updating {len(batch_updates_payload)} records...")
    try:
        # 🛑 FIX: Removed 'chunk_size' argument as it caused an AttributeError
        updated_rows = table.update_rows(batch_updates_payload)
        print(f"✅ Successfully updated {len(updated_rows)} records.")
    except Exception as e:
        print(f"🛑 Batch Update Failed: {e}")

# --- Batch Create New Rows ---
if batch_creates_payload:
    print(f"Creating {len(batch_creates_payload)} new records...")
    try:
        # Keeping chunk_size here as it should be valid for add_rows
        added_rows = table.add_rows(batch_creates_payload, chunk_size=CHUNK_SIZE)
        print(f"🆕 Successfully created {len(added_rows)} records.")
    except Exception as e:
        print(f"🛑 Batch Create Failed: {e}")


print("🎉 Baserow sync complete!")
# %%