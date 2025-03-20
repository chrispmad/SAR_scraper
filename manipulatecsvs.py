# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import functions.airtable_functions as airfuncs
import re  # Regular expressions!
import numpy as np
import datetime
import csv
from pyairtable import Api
from airtable import Airtable
import requests
import json
import time
from datetime import date
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from urllib.parse import urlparse, unquote
import re

#%%

risk_registry = pd.read_csv("data/risk_registry.csv", encoding="utf-8-sig")
candidate_list = pd.read_csv(
    "data/cosewic_spp_specialist_candidate_list.csv", encoding="ISO-8859-1"
)
status_report = pd.read_csv(
    "data/cosewic_status_reports_prep.csv", encoding="ISO-8859-1"
)
species_tbl = pd.read_csv("data/candidate_species_tbl.csv", encoding="ISO-8859-1")





# BC / Pacific Filter
risk_registry = risk_registry[
    risk_registry["Range"].str.contains(
        r"(BC|British Columbia|Pacific|British)", na=False
    )
]
candidate_list = candidate_list[
    candidate_list["Location"].str.contains(
        r"(BC|British Columbia|Pacific|British)", na=False
    )
]
status_report = status_report[
    status_report["Canadian range / known or potential jurisdictions 1"].str.contains(
        r"(BC|British Columbia|Pacific|British)", na=False
    )
]
species_tbl = species_tbl[
    species_tbl["Canadian range / known or potential jurisdictions"].str.contains(
        r"(BC|British Columbia|Pacific|British)", na=False
    )
]

# Replace any occurrences of 'Populations' in the population field.
risk_registry["Legal population"] = risk_registry["Legal population"].str.replace(r"\b[Pp]opulations?\b", "population", regex=True)
risk_registry["COSEWIC population"] = risk_registry["COSEWIC population"].str.replace(r"\b[Pp]opulations?\b", "population", regex=True)



# Add estimated re-assessment date (10 years from most recent assessment)
risk_registry["Estimated re-assessment"] = pd.to_datetime(
    risk_registry["COSEWIC last assessment date"]
) + pd.DateOffset(years=10)

# Apply John's function to ascertain the domain of the species. Just to risk_registry, I think.
risk_registry["Domain"] = risk_registry.apply(airfuncs.determine_cosewic_domain, axis=1)

# Create unique ID, based on Chrissy's logic.
cosewic_population_filled = (
    risk_registry["COSEWIC population"]
    .fillna(risk_registry["Legal population"])
    .fillna("NA")
)

risk_registry["Unique_ID"] = (
    risk_registry["Scientific name"]
    + " - "
    + risk_registry["COSEWIC common name"].fillna(" <blank> ")
    + " ("
    + cosewic_population_filled
    + ")"
)

# Remove "(NA)" from UniqueID column.
risk_registry["Unique_ID"] = risk_registry["Unique_ID"].str.replace(
    " (NA)", "", regex=False
)

def merge_rows(group):
    """
    Merges multiple rows within a group by keeping the first non-null value for each column.

    Args:
        group (pd.DataFrame): Grouped DataFrame.

    Returns:
        pd.Series: A single merged row.
    """
    return group.apply(lambda col: col.dropna().iloc[0] if not col.dropna().empty else None)




# for where the legal common name is white sturgeon - if the COSEWIC common name is blank, then the COSEWIC common name should be white sturgeon
risk_registry.loc[(risk_registry["Legal common name"] == "White Sturgeon") & (risk_registry["COSEWIC common name"].isnull()), "COSEWIC common name"] = "White Sturgeon"
#update the Fraser River population to include the Nechako and Mid Fraser populations
risk_registry.loc[
    (risk_registry["Legal common name"] == "White Sturgeon") & 
    (risk_registry["COSEWIC population"] == "Upper Fraser River population"),
    ["COSEWIC population", "Legal population"]
] = "Upper Fraser River (plus Nechako, Mid Fraser) population"

# merge the rows where the legal population are the same, overwrite the data if the cell entry is NA



# Only apply merging to "White Sturgeon"
white_sturgeon = risk_registry[risk_registry["Legal common name"] == "White Sturgeon"]

# remove the unique_id column, it should be made after all changes are done
white_sturgeon = white_sturgeon.drop(columns=["Unique_ID"])

#White Sturgeon with Legal Population = Lower Fraser, 2 records: Merge together duplicate rows, replacing blank cells with information from other row
mask = (white_sturgeon["Legal common name"] == "White Sturgeon") & (white_sturgeon["Legal population"] == "Lower Fraser River population")
# Select only the relevant rows
sturgeon_subset = white_sturgeon[mask]
# Merge duplicate rows by filling NaNs with available values
merged_row = sturgeon_subset.ffill().bfill().drop_duplicates()
# Drop old rows and replace with the merged row
white_sturgeon = pd.concat([white_sturgeon[~mask], merged_row], ignore_index=True)


#replace the NA with "Non-active" for the COSEWIC status
white_sturgeon["COSEWIC status"] = white_sturgeon["COSEWIC status"].fillna("Non-active")



#White Sturgeon with Legal Population = Upper Columbia, 2 records: Delete the record that DOES NOT say COSEWIC Status = Endangered

# Group by "Legal population" and apply merging function to ALL columns
#merged_sturgeon = white_sturgeon.groupby("Legal population", as_index=False).apply(merge_rows).reset_index(drop=True)

# White Sturgeon with Legal Population = Upper Columbia, 2 records: Delete the record that DOES NOT say COSEWIC Status = Endangered
mask = (white_sturgeon["Legal common name"] == "White Sturgeon") & (white_sturgeon["Legal population"] == "Upper Columbia River population")
sturgeon_subset = white_sturgeon[mask]
# Keep the row with COSEWIC status = Endangered
merged_row = sturgeon_subset[sturgeon_subset["COSEWIC status"] == "Endangered"]
# Drop old rows and replace with the merged row
white_sturgeon = pd.concat([white_sturgeon[~mask], merged_row], ignore_index=True)

# Create unique ID, based on Chrissy's logic.
cosewic_population_filled = (
    white_sturgeon["COSEWIC population"]
    .fillna(white_sturgeon["Legal population"])
    .fillna("NA")
)

white_sturgeon["Unique_ID"] = (
    white_sturgeon["Scientific name"]
    + " - "
    + white_sturgeon["COSEWIC common name"].fillna(" <blank> ")
    + " ("
    + cosewic_population_filled
    + ")"
)


# Keep all other rows unchanged
other_species = risk_registry[risk_registry["Legal common name"] != "White Sturgeon"]

# Combine back the merged White Sturgeon data with other species
risk_registry = pd.concat([white_sturgeon, other_species], ignore_index=True)


# fix the na to be non-active
risk_registry.loc[(risk_registry["COSEWIC status"].isnull()), "COSEWIC status"] = "Non-active"

# to be combined, it is risk_registry and status_report.
# What else was wrong?


risk_registry["COSEWIC population"] = risk_registry["COSEWIC population"].replace("populations", "population")
# Define the regex pattern
pattern = r"\b[Pp]opulations\b"
mask = risk_registry.apply(lambda col: col.astype(str).str.contains(pattern, na=False, regex=True)).any(axis=1)
# Filter and print the rows
filtered_rows = risk_registry[mask]
# Drop completely blank rows (if needed)
filtered_rows = filtered_rows.dropna(how="all")
print(filtered_rows) 

# %%
# Time to apply Chrissy's final logic to the federal risk registry:
# 1. If there is only 1 unique row and missing values for the COSEWIC rows:
# ensure the "COSEWIC status" has a value or add the value Non-active if blank.
# Make no other changes.
# e.g., White Sturgeon: no legal population, Kootenay, Middle Fraser, Nechako, upper Kootenay.
# If there are 2 duplicate rows, merge and replace blanks with values from the other row (e.g., Lower Fraser White Sturgeon)
# If there are conflicting values in the 2 duplicate rows, use the values from the row that has the more recent date between "COSEWIC last assessment date" and "Date added"
# e.g., White Sturgeon: lower Fraser

# 1.
id_col = ["Unique_ID"]
single_rows = risk_registry.groupby(id_col).filter(lambda x: len(x) == 1)
risk_registry.loc[single_rows.index, "COSEWIC status"] = single_rows[
    "COSEWIC status"
].fillna("Non-active")

# %%
# 2. Note: there seems to be no duplicates currently.
duplicates = risk_registry[risk_registry.duplicated(id_col, keep=False)]

# Reorganize columns!
cols_first = [
    "Unique_ID",
    "Domain",
    "Taxonomic group",
    "Scientific name",
    "COSEWIC common name",
    "COSEWIC population",
    "COSEWIC status",
    "COSEWIC last assessment date",
    "Estimated re-assessment",
    "Legal common name",
]
# Ensure all columns are kept by using set operations
all_columns = list(risk_registry.columns)
cols_remaining = [col for col in all_columns if col not in cols_first]

# Reorder DataFrame
risk_registry = risk_registry[cols_first + cols_remaining]





#%%
### COSEWIC Status reports

# Replace any occurrences of 'Populations' in the population field.
status_report["Common name"] = status_report["Common name"].str.replace(r"\b[Pp]opulations?\b", "population", regex=True)

status_report["Range"] = status_report["Canadian range / known or potential jurisdictions 1"]

# Create Unique_ID column.
status_report["Unique_ID"] = (
    status_report["Scientific name"] + " - " + status_report["Common name"]
)

# Pull out Population from 'Common name' column, save to new column.
status_report["Population"] = status_report["Common name"].str.extract(r"\((.*?)\)")

# Remove population and the surrounding parentheses from Common name column.
status_report["Common name"] = status_report["Common name"].str.extract(
    r"([a-zA-Z \-']*)"
)

# Add Domain column
conditions = [
    status_report["Taxonomic group"].isin(
        ["Freshwater fishes", "Marine fishes", "Marine mammals", "Echinodermata"]
    )
    | status_report["Common name"].str.match(
        ".*([aA]balone|[oO]yster|[mM]ussel|[lL]anx|[cC]apshell|Hot Springs Snail|[pP]hysa|[pP]ebblesnail)"
    ),
    status_report["Taxonomic group"].isin(
        [
            "Amphibians",
            "Arthropods",
            "Birds",
            "Lichens",
            "Terrestrial mammals",
            "Mosses",
            "Reptiles",
            "Vascular plants",
        ]
    ),
]
choices = ["Aquatic", "Terrestrial"]
status_report["Domain"] = np.select(conditions, choices, default="Other")

# Clean up values in Taxonomic group column
status_report["Taxonomic group"] = status_report["Taxonomic group"].str.replace(
    "Freshwater fishes", "Fishes (freshwater)"
)
status_report["Taxonomic group"] = status_report["Taxonomic group"].str.replace(
    "Marine fishes", "Fishes (marine)"
)
status_report["Taxonomic group"] = status_report["Taxonomic group"].str.replace(
    "Marine mammals", "Mammals (marine)"
)
status_report["Taxonomic group"] = status_report["Taxonomic group"].str.replace(
    "Terrestrial mammals", "Mammals (terrestrial)"
)

# Make COSEWIC Common Name column.
status_report["COSEWIC common name"] = status_report["Common name"]

# Make COSEWIC Population column.
status_report["COSEWIC population"] = status_report["Population"]

# Make COSEWIC status column.
status_report["COSEWIC status"] = status_report["Last assessment"]
status_report["COSEWIC status"] = np.where(
    status_report["COSEWIC status"].str.contains("Not applicable"), "No status", ""
)
#rename the column
status_report = status_report.rename(columns={'Last assessment': 'COSEWIC last assessment'})


# gets the date and adds a Day value of "01" as per Chrissy
status_report['Scheduled Assessment'] = status_report['Group'].apply(airfuncs.extract_date)


#Merge the tables here
#%%

col_order_merged = ["Unique_ID",
                    "Domain", "Taxonomic group",
                    "Scientific name", "COSEWIC common name",
                    "COSEWIC population", "COSEWIC status",
                    "COSEWIC last assessment date", "Estimated re-assessment",
                    "Scheduled Assessment"]


risk_registry_id = set(risk_registry['Unique_ID'])
status_report = status_report[~status_report["Unique_ID"].isin(risk_registry_id)]

# if the unique id matches between the two tables, then the columns are merged. Replace the value in "Scheduled Assessment" in the risk registry table with the "Scheduled assessment" value in the status report table
merged_risk_status = pd.merge(risk_registry, status_report[["Unique_ID","Scheduled Assessment"]], on="Unique_ID", how = "outer")
merged_risk_status["Scheduled Assessment"] = merged_risk_status["Scheduled Assessment"].fillna(" ")
airfuncs.prioritize_x_column(merged_risk_status)

merged_risk_status = pd.merge(risk_registry, status_report[["Unique_ID","Domain", "Taxonomic group", "Scientific name", "COSEWIC common name",
                                                           "COSEWIC population", "COSEWIC status",
                                                           "Scheduled Assessment"]], on="Unique_ID", how = "outer")
#This function will take the newly merged table and retain anything that is in risk registry but also in status_reports.
# If the field is empty in risk registry, then it will use what is in status_report. Then the column names are fixed, to remove the x and y 
airfuncs.prioritize_x_column(merged_risk_status)

# if

#merged_risk_status = merged_risk_status[col_order_merged]


# %%
""" fileToAdd = fileToAdd[
    fileToAdd.apply(
        lambda row: row.str.contains("|".join(search_string), case=False)
    ).any(axis=1)
] """


#%%
# This is part 2 COSEWIC candidate list table
species_tbl["Unique_ID"] = species_tbl.apply(lambda row: f"{row['Scientific name']} - {row['Common name'] or '<blank>'}", axis = 1)
species_tbl["Domain"] = species_tbl.apply(airfuncs.determine_domain_general, axis=1)


# Clean up values in Taxonomic group column
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "Freshwater fishes", "Fishes (freshwater)"
)
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "Marine fishes", "Fishes (marine)"
)
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "Marine mammals", "Mammals (marine)"
)
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "Terrestrial mammals", "Mammals (terrestrial)"
)

species_tbl["COSEWIC common name"] = species_tbl["Common name"]

species_tbl["Candidate list"] = "COSEWIC"

species_tbl["Priority"] = "COSEWIC - Group 1"

#Take the first 4 characters from the Category and assign them as "Date_nominated"
species_tbl = species_tbl.assign(Date_nominated=species_tbl["Category"].str[:4])
# I need to change the numeric year into a date, with the day being 01 and month being 01
#species_tbl["Date_nominated"] = pd.to_datetime(species_tbl["Date_nominated"], format='%Y').dt.strftime('%Y-%m-%d')

# Reorganize columns!
cols_sp_first = [
    "Unique_ID",
    "Domain",
    "Taxonomic group",
    "Scientific name",
    "COSEWIC common name",
    "Candidate list",
    "Priority",
    "Date_nominated",
    "Rationale",
]

# Get all remaining columns that are not in cols_sp_first
cols_remaining = [col for col in species_tbl.columns if col not in cols_sp_first]

# Reorder DataFrame by placing cols_sp_first at the beginning and appending the rest
species_tbl = species_tbl[cols_sp_first + cols_remaining]

# %%
# working on the candidate table

candidate_list = candidate_list.rename(columns={"group": "Priority"})
candidate_list["Unique_ID"] = candidate_list.apply(lambda row: f"{row['Scientific name']} - {row['Common name'] or '<blank>'}", axis = 1) 
candidate_list["Domain"] = candidate_list.apply(airfuncs.cList_Domain_col, axis = 1)



# Clean up values in Taxonomic group column
candidate_list["Taxonomic group"] = candidate_list["Group"].astype(str).str.replace(
    "Freshwater Fishes", "Fishes (freshwater)"
)
candidate_list["Taxonomic group"] = candidate_list["Group"].astype(str).str.replace(
    "Marine Fishes", "Fishes (marine)"
)
candidate_list["Taxonomic group"] = candidate_list["Group"].astype(str).str.replace(
    "Marine Mammals", "Mammals (marine)"
)
candidate_list["Taxonomic group"] = candidate_list["Group"].astype(str).str.replace(
    "Terrestrial Mammals", "Mammals (terrestrial)"
)

candidate_list["COSEWIC common name"] = candidate_list["Common name"]

candidate_list["Candidate list"] = "SSC"

candidate_list["Priority"] = "SSC - " + candidate_list["Priority"].str[:7]

cols_candidate_first = [
    "Unique_ID",
    "Domain",
    "Taxonomic group",
    "Scientific name",
    "COSEWIC common name",
    "Candidate list",
    "Priority"
]

candidate_list = candidate_list[cols_candidate_first]

# %%
# merging the two tables

species_tbl = species_tbl.rename(columns={"Date_nominated": "Date nominated"})


species_tbl_unique = set(species_tbl['Unique_ID'])
candidate_list = candidate_list[~candidate_list["Unique_ID"].isin(species_tbl_unique)]

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
    "Rationale",
    
]

cols_remaining = [col for col in species_tbl.columns if col not in cols_merged_order]

merged_spp_candidate = merged_spp_candidate[cols_merged_order]

# %%




# link to save the files
sharepoint_link = r"https://bcgov.sharepoint.com/:f:/r/teams/09277-AquaticSAR/Shared%20Documents/Aquatic%20SAR/SAR%20Data/Airtable%20uploads?csf=1&web=1&e=22KJIf"


# Parse and clean up the URL
parsed_url = urlparse(sharepoint_link)
path_parts = parsed_url.path.split("/r/")  # Split at "/r/" to get the folder path

# Extracting the SharePoint base URL and folder path
sharepoint_url = "https://bcgov.sharepoint.com/"  # Update site name
folder_url = unquote(path_parts[-1])  # Decoding '%20' into spaces

# Authenticate using the Azure app credentials
ctx = ClientContext(sharepoint_url).with_credentials(ClientCredential(client_id, client_secret))
target_folder = ctx.web.get_folder_by_server_relative_url(folder_url)

#%%








todays_date = date.today().strftime('%Y-%m-%d')
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

merged_risk_status = merged_risk_status.fillna("")

merged_risk_status['Estimated re-assessment'] = merged_risk_status['Estimated re-assessment'].fillna(" ")
#merged_risk_status['Estimated re-assessment'] = merged_risk_status['Estimated re-assessment'].replace(pd.Timestamp('1900-01-01'), " ")

merged_spp_candidate = merged_spp_candidate.fillna("")




# %%
# Initialize WebDriver (ensure 'chromedriver' is in your PATH)
# driver = webdriver.Chrome()

# # Open the webpage
# driver.get(
#     "https://bcgov.sharepoint.com/"
# )

# # Wait for the page to load fully
# time.sleep(1)
# %%


##
# Authentication - Requires Azure app registration

with open("login/sharepointlogin.txt", "r") as text_file:
    data = text_file.readlines()
    
client_id = data[0].strip("\n")
client_secret = data[1]


# driver.find_element(By.ID, "i0116").send_keys(client_id)

# login_button = driver.find_element(By.XPATH, ('//*[@id="idSIButton9"]')) 
# login_button.click()
# time.sleep(1)

# driver.find_element(By.ID, "i0118").send_keys(client_secret)
# login_button = driver.find_element(By.XPATH, ('//*[@id="idSIButton9"]')) 
# login_button.click()
# time.sleep(1)

app_principal = {  
    'client_id': '--client-id-goes-here--',  
    'client_secret': '--client-secret-goes-here--',  
}  




# %%
