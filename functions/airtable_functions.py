from pyairtable import Api
from airtable import Airtable
import requests
import pandas as pd
import csv
import time
import json
import numpy as np
import re
import datetime


# Deletes a record from Airtable, based on the column name and the string you pass it.
# takes the name of the column to use a unique identifier - if you delete Group - Amphibians, then they will all get deleted
# urls etc need to be set before hand.
def delete_record_by_col(url, base_id, table_id, headers, column_name, target_value):

    # Airtable formula to filter records with the target value in the specified column
    params = {"filterByFormula": f"{{{column_name}}} = '{target_value}'"}

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
        record_id = record["id"]
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
        data = {"fields": row.to_dict()}

        # Make the POST request to upload data
        response = requests.post(url, headers=headers, data=json.dumps(data))

        # Check for success or error in the response
        if response.status_code == 200:
            print(f"Row {index} uploaded successfully!")
        # else:
        #    print(f"Error uploading row {index}: {response.json()}")

        # Sleep to respect rate limits of the Airtable API
        time.sleep(0.2)


def fetch_records(url, headers, base_id, table_id):
    records = []

    while True:
        response = requests.get(url, headers=headers)
        response_json = response.json()
        records.extend(response_json.get("records", []))

        offset = response_json.get("offset")
        if not offset:
            break
        url = f"https://api.airtable.com/v0/{base_id}/{table_id}?offset={offset}"
    return records


def delete_records(url, base_id, table_id, headers, records):
    record_id_to_delete = [record["id"] for record in records]
    batch_size = 10
    # print(record_id_to_delete)
    for i in range(0, len(record_id_to_delete), batch_size):
        print(record_id_to_delete[i : i + batch_size])
        batch = record_id_to_delete[i : i + batch_size]
        url = f"https://api.airtable.com/v0/{base_id}/{table_id}?" + "&".join(
            [f"records[]={id}" for id in batch]
        )
        response = requests.delete(url=url, headers=headers)
        if response.status_code == 200:
            print(f"Successfully deleted batch: {batch}")
        else:
            print(f"Error deleting batch. Response: {response.text}")


def create_unique_id(df):
    df["UniqueID"] = df.apply(
        lambda row: f"{row['Scientific name']} - {row['COSEWIC common name'] or '<blank>'} - {row['COSEWIC population'] or row['Legal population'] or '<blank>'}",
        axis=1,
    )
    return df


def determine_cosewic_domain(row):
    taxonomic_group = row["Taxonomic group"]
    common_name = row["COSEWIC common name"]
    if taxonomic_group == "Molluscs":
        if any(
            name in common_name
            for name in [
                "abalone",
                "oyster",
                "mussel",
                "lanx",
                "capshell",
                "Hot Springs Snail",
                "Abalone",
                "Oyster",
                "Mussel",
                "Lanx",
                "Capshell",
            ]
        ):
            return "Aquatic"
        else:
            return "Terrestrial"
    elif any(
        group in taxonomic_group
        for group in ["Fishes (freshwater)", "Fishes (marine)", "Mammals (marine)"]
    ):
        return "Aquatic"
    elif any(
        group in taxonomic_group
        for group in [
            "Amphibians",
            "Arthropods",
            "Birds",
            "Lichens",
            "Mammals (terrestrial)",
            "Mosses",
            "Reptiles",
            "Vascular Plants",
        ]
    ):
        return "Terrestrial"
    else:
        return ""

# Function to get suggested re-assessment date
def reassessment_date(row):

    last_assesement = pd.to_datetime( row["COSEWIC last assessment date"])
    return (last_assesement + pd.DateOffset(years = 10))


    
def extract_date(group_text):
    date_match = re.search(r"(\w+ \d{4})", group_text)
    if date_match:
        month_str, year_str = date_match.group(1).split()
        year = int(year_str)
        month = datetime.datetime.strptime(month_str, '%B').month
        date_obj = datetime.date(year, month, 1)
        return date_obj.strftime('%Y-%m-%d')  # Format the date as YYYY-MM-DD
    else:
        return "No date found"
     
     
     # Function to prioritize columns with suffix 'x'
def prioritize_x_column(df):
    # for each column in the dataframe
    for col in df.columns:
        #is the column name ends in x,
        if col.endswith('_x'):
            #the _X is removed and this forms thew new column name. Then it used the data
            # in the column to fill this new colum in.
            df[col[:-2]] = df[col]
            # if the column ends in a y
        elif col.endswith('_y'):
            # if the cell is empty, fill it in with what is in y.
            df[col[:-2]] = df[col].fillna(df[col[:-2]])
            # drop the names ending in _x or _y
    df.drop(df.filter(regex='_x|_y').columns, axis=1, inplace=True)
    
    
def cList_Domain_col(row):
    group = row['Group'].lower()
    common_name = row["Common name"].lower()

    if group == "molluscs":
        if any(
            name in common_name
            for name in [
                "abalone",
                "oyster",
                "mussel",
                "lanx",
                "capshell",
                "hot springs snail",
            ]
        ):
            return "Aquatic"
        else:
            return "Terrestrial"
    elif any(
        grp in group
        for grp in ["amphibians", "arthropods", "birds", "lichens", "mosses",
                     "reptiles", "terrestrial mammals", "vascular plants"]
    ):
        return "Terrestrial"
    elif any(
        grp in group
        for grp in ["freshwater fishes", "marine fishes", "marine mammals"]
    ):
        return "Aquatic"
    else:
        return ""

        
        
def determine_domain_general(row):
    taxonomic_group = row["Taxonomic group"]
    common_name = row["Common name"]

    if taxonomic_group.lower() == "molluscs":
        if any(
            name.lower() in common_name.lower()
            for name in [
                "abalone",
                "oyster",
                "mussel",
                "lanx",
                "capshell",
                "hot springs snail",
            ]
        ):
            return "Aquatic"
        else:
            return "Terrestrial"
    elif any(
        group.lower() in taxonomic_group.lower()
        for group in ["fishes (freshwater)", "fishes (marine)", "mammals (marine)",
                      "Marine fishes"]
    ):
        return "Aquatic"
    elif any(
        group.lower() in taxonomic_group.lower()
        for group in [
            "amphibians",
            "arthropods",
            "birds",
            "lichens",
            "mammals (terrestrial)",
            "mosses",
            "reptiles",
            "vascular plants",
            "Terrestrial mammals"
        ]
    ):
        return "Terrestrial"
    else:
        return ""     
    
    
    