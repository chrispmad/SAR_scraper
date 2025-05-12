# %%
import importlib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import functions.airtable_functions as airfuncs
import functions.update_funcs as updatefuncs
importlib.reload(updatefuncs)
import re  # Regular expressions!
import numpy as np

import csv
from pyairtable import Api
from airtable import Airtable
import requests
import json
import time
from datetime import datetime
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from urllib.parse import urlparse, unquote
import re

# Function to easily run a subscript python file locally,
# like source() in R.
def run_subscript(filepath):
    exec(open(filepath, encoding="utf-8").read())

#%% 
# Import functions defined in a subscript.
# run_subscript("manipulate_csvs_subscripts/functions.py")

#%% 
# Load in data files scraped from COSEWIC webpages
risk_registry = pd.read_csv("data/risk_registry.csv", encoding="utf-8-sig")
candidate_list = pd.read_csv(
    "data/cosewic_spp_specialist_candidate_list.csv", encoding="ISO-8859-1"
)
status_report = pd.read_csv(
    "data/cosewic_status_reports_prep.csv", encoding="ISO-8859-1"
)
species_tbl = pd.read_csv("data/candidate_species_tbl.csv", encoding="ISO-8859-1")

#%% 
# Apply some basic data filters
run_subscript("manipulate_csvs_subscripts/data_filters.py")

#%% 
# Clean up population field.
run_subscript("manipulate_csvs_subscripts/clean_population_field.py")

#%% 
# Run various lines that add columns and clean data for the Risk Registry table. 
run_subscript("manipulate_csvs_subscripts/assorted_risk_registry_cleaning.py")

#%% 
# Run various lines that add columns and clean data for the Status Report table. 
run_subscript("manipulate_csvs_subscripts/assorted_status_report_cleaning.py")

#%% 
#Merge the tables.
merged_risk_status = pd.merge(
    risk_registry,
    status_report[[  # Selecting the necessary columns from status_report
        "Unique_ID", "Domain", "Taxonomic group", "Scientific name",
        "COSEWIC common name", "COSEWIC population", "COSEWIC status",
        "Scheduled Assessment", "Range"  # Add any other columns that might have missing values
    ]],
    on="Unique_ID",
    how="outer",
    suffixes=("", "_status")  # Keep distinct column names for status_report
)
#%% 
run_subscript("manipulate_csvs_subscripts/make_merged_risk_status_tbl.py")
#%%
# This is part 2 COSEWIC candidate list table: 
# Make 
run_subscript("manipulate_csvs_subscripts/make_merged_spp_candidate_tbl_from_candidate_list_and_species_tbl.py")

#%% 
# merging the two tables
species_tbl["Date nominated"] = species_tbl["Date_nominated"]
del species_tbl["Date_nominated"]
# species_tbl = species_tbl.rename(columns={"Date_nominated": "Date nominated"})

species_tbl_unique = set(species_tbl['Unique_ID'])
#candidate_list = candidate_list[~candidate_list["Unique_ID"].isin(species_tbl_unique)]
#%% 
merged_spp_candidate = species_tbl.merge(candidate_list, on = 'Unique_ID', how = 'outer')

airfuncs.prioritize_x_column(merged_spp_candidate)

cols_merged_order = [
    "Unique_ID",
    "Domain",
    "Taxonomic group",
    "Scientific name",
    "COSEWIC common name",
    "Candidate list",
    "Priority",
    "Date nominated",
    "Rationale"
]

cols_remaining = [col for col in species_tbl.columns if col not in cols_merged_order]

merged_spp_candidate = merged_spp_candidate[cols_merged_order]

#%%

# Final cleaning
run_subscript("manipulate_csvs_subscripts/final_tbl_cleaning.py")
#%% 
# %%

# If we're happy with the results, save those to disk
todays_date = datetime.today().strftime('%Y-%m-%d')
merged_risk_status.to_csv("output/risk_status_merged.csv", index=False)
merged_risk_status.to_csv("output/risk_status_merged"+todays_date+".csv", index=False)
#merged_risk_status.to_csv(sharepoint_link+"risk_status_merged"+todays_date+".csv", index=False)

file_path = "/risk_status_merged_"+todays_date+".csv"
upload_file_name = f"/risk_status_merged_{todays_date}.csv"

# Open the file and upload it to SharePoint
#with open(file_path, "rb") as file_content:
#    target_file = target_folder.upload_file(upload_file_name, file_content)
#    ctx.execute_query()

merged_spp_candidate.to_csv("output/merged_spp_candidate.csv", index=False)
merged_spp_candidate.to_csv("output/merged_spp_candidate"+todays_date+".csv", index=False)
#merged_spp_candidate.to_csv(sharepoint_link + "merged_spp_candidate"+todays_date+".csv", index = False)
risk_status_colnames = list(merged_risk_status.columns)
spp_candidate_colnames = list(merged_spp_candidate.columns)

# save column names to create the tables in airtable
df= pd.DataFrame(risk_status_colnames)
df = df.transpose()
df.to_csv("output/col_names/risk_status_colnames.csv", index = False, header=False)
df.to_csv("output/col_names/risk_status_colnames"+todays_date+".csv", index = False, header=False)

df= pd.DataFrame(spp_candidate_colnames)
df = df.transpose()

df.to_csv("output/col_names/spp_candidate_colnames.csv", index = False, header=False)
df.to_csv("output/col_names/spp_candidate_colnames"+todays_date+".csv", index = False, header=False)

#%% 