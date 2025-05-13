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

#%%

risk_registry = pd.read_csv("data/risk_registry.csv", encoding="utf-8-sig")
candidate_list = pd.read_csv(
    "data/cosewic_spp_specialist_candidate_list.csv", encoding="ISO-8859-1"
)
status_report = pd.read_csv(
    "data/cosewic_status_reports_prep.csv", encoding="ISO-8859-1"
)
species_tbl = pd.read_csv("data/candidate_species_tbl.csv", encoding="ISO-8859-1")



# Note! Some species don't list BC in the risk registry Range column, BUT those species
# are listed as living in BC in the 'status report' table. Find those species to make sure
# we don't accidentally delete them from the risk registry!

species_to_retain_in_risk_registry

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

## remove where the cosewic population has "watershed" just delete it
risk_registry["COSEWIC population"] = risk_registry["COSEWIC population"].str.replace(
    "Watershed", "River", case=False, regex=True
).str.strip()  # Remove trailing spaces if "Watershed" was at the end

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

status_report["Taxonomic group"] = status_report["Taxonomic group"].str.replace(
    "Echinodermata (ad hoc)", "Echinodermata"
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

def extract_month_year(text):
    if isinstance(text, str):
        words = text.split()
        if words[:2] == ["Not", "applicable"]:
            return "9999-01-01"  # Set default date if "Not applicable"
        elif len(words) >= 2:
            month_year_str = f"01 {' '.join(words[-2:])}"  # Prefix "01" to last two words
            try:
                # Convert to datetime object and format as YYYY-MM-DD
                date_obj = datetime.strptime(month_year_str, "%d %B %Y")
                return date_obj.strftime("%Y-%m-%d")  
            except ValueError:
                print(f"⚠️ Invalid date format: {month_year_str}")  # Debugging for unexpected cases
                return None  # Return None for invalid dates
    return None  # Return None if condition not met

def extract_status(text):
    if isinstance(text, str):
        words = text.split()
        if words[:2] == ["Not", "applicable"]:  
            return "Not applicable"  # Return as-is
        elif len(words) > 2:
            return " ".join(words[:-2])  # Get everything except the last two words
    return None  # Return None if there are 2 or fewer words and not "Not applicable"


status_report["COSEWIC status"] = status_report["COSEWIC last assessment"].apply(extract_status)
status_report["COSEWIC last assessment corrected"] = status_report["COSEWIC last assessment"].apply(extract_month_year)



#Merge the tables here
#%%

# Merge while keeping both sets of columns
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

# List of columns to update from status_report, including "Range" and others
columns_to_update = [
    "Domain", "Taxonomic group", "Scientific name", 
    "COSEWIC common name", "COSEWIC population", 
    "COSEWIC status", "Scheduled Assessment", "Range"
]

# Fill missing values in risk_registry with status_report ONLY IF risk_registry has NaN
for col in columns_to_update:
    status_col = f"{col}_status"  # The column from status_report
    if status_col in merged_risk_status:
        # Use combine_first to fill NaN in risk_registry with values from status_report
        merged_risk_status[col] = merged_risk_status[col].combine_first(merged_risk_status[status_col])

# Drop extra columns from status_report (e.g., 'Domain_status', 'Range_status')
merged_risk_status = merged_risk_status.drop(columns=[f"{col}_status" for col in columns_to_update if f"{col}_status" in merged_risk_status])

# Fill any remaining missing values across the merged dataframe (optional, if you want to apply a global fill for missing values)
merged_risk_status = merged_risk_status.fillna("NA")  # Or use another placeholder or method as needed

# This function will take the newly merged table and retain anything that is in risk_registry but also in status_reports
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


species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "(?i)Freshwater fishes", "Fishes (freshwater)", regex=True
)
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "(?i)Marine fishes", "Fishes (marine)", regex=True
)
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "(?i)Marine mammals", "Mammals (marine)", regex=True
)
species_tbl["Taxonomic group"] = species_tbl["Taxonomic group"].str.replace(
    "(?i)Terrestrial mammals", "Mammals (terrestrial)", regex=True
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

#Regex=True means to treat the expressions as regular expressions, not strings
candidate_list["Common name"] = candidate_list["Common name"].astype(str).str.replace(
    "(?i)Populations", "population", regex=True
)

candidate_list = candidate_list.rename(columns={"group": "Priority"})
candidate_list["Unique_ID"] = candidate_list.apply(lambda row: f"{row['Scientific name']} - {row['Common name'] or '<blank>'}", axis = 1) 
candidate_list["Domain"] = candidate_list.apply(airfuncs.cList_Domain_col, axis = 1)
candidate_list["Taxonomic group"] = candidate_list["Group"]


# Clean up values in Taxonomic group column (case-insensitive)
candidate_list["Taxonomic group"] = candidate_list["Taxonomic group"].astype(str).str.replace(
    "(?i)Freshwater Fishes", "Fishes (freshwater)", regex=True
)
candidate_list["Taxonomic group"] = candidate_list["Taxonomic group"].astype(str).str.replace(
    "(?i)Marine Fishes", "Fishes (marine)", regex=True
)
candidate_list["Taxonomic group"] = candidate_list["Taxonomic group"].astype(str).str.replace(
    "(?i)Marine Mammals", "Mammals (marine)", regex=True
)
candidate_list["Taxonomic group"] = candidate_list["Taxonomic group"].astype(str).str.replace(
    "(?i)Terrestrial Mammals", "Mammals (terrestrial)", regex=True
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
#candidate_list = candidate_list[~candidate_list["Unique_ID"].isin(species_tbl_unique)]

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




# # link to save the files
# sharepoint_link = r"https://bcgov.sharepoint.com/:f:/r/teams/09277-AquaticSAR/Shared%20Documents/Aquatic%20SAR/SAR%20Data/Airtable%20uploads?csf=1&web=1&e=22KJIf"


# # Parse and clean up the URL
# parsed_url = urlparse(sharepoint_link)
# path_parts = parsed_url.path.split("/r/")  # Split at "/r/" to get the folder path

# # Extracting the SharePoint base URL and folder path
# sharepoint_url = "https://bcgov.sharepoint.com/"  # Update site name
# folder_url = unquote(path_parts[-1])  # Decoding '%20' into spaces

# # Authenticate using the Azure app credentials
# ctx = ClientContext(sharepoint_url).with_credentials(ClientCredential(client_id, client_secret))
# target_folder = ctx.web.get_folder_by_server_relative_url(folder_url)

#%%
## Fixing the NAs and the dates
# convert all types of file to string - this should be changed later to be appropriate types
merged_risk_status = merged_risk_status.astype(str) 

# Ensure true NaN values are properly handled
merged_risk_status.replace({pd.NA: "NA", None: "NA"}, inplace=True)
merged_risk_status = merged_risk_status.applymap(lambda x: "NA" if pd.isna(x) or str(x).strip().lower() == "nan" else x)

# Define specific replacements for date and categorical fields
date_fields = [
    "Date added", "Last status change", "Scheduled Assessment", 
    "COSEWIC last assessment date", "Estimated re-assessment"
]
category_fields = ["Taxonomic group", "COSEWIC status"]

# Replace missing values for date fields
for field in date_fields:
    merged_risk_status[field] = merged_risk_status[field].replace(
        {pd.NA: "9999-01-01", None: "9999-01-01", "nan": "9999-01-01", "NA": "9999-01-01", "No date found": "9999-01-01", "NaT" : "9999-01-01"}
    )

# Replace missing values for category fields
for field in category_fields:
    merged_risk_status[field] = merged_risk_status[field].replace(
        {pd.NA: "none", None: "none", "nan": "none", "NA": "none"}
    )

# Convert all columns in merged_spp_candidate to strings and standardize missing values
merged_spp_candidate = merged_spp_candidate.astype(str).applymap(lambda x: "NA" if pd.isna(x) or str(x).strip().lower() == "nan" else x)

# Standardize "Date nominated" field separately
merged_spp_candidate["Date nominated"] = merged_spp_candidate["Date nominated"].replace(
    {pd.NA: "9999", None: "9999", "nan": "9999", "NA": "9999"}
)


#%%






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

# merged_risk_status = merged_risk_status.fillna("")

# merged_risk_status['Estimated re-assessment'] = merged_risk_status['Estimated re-assessment'].fillna(" ")
# #merged_risk_status['Estimated re-assessment'] = merged_risk_status['Estimated re-assessment'].replace(pd.Timestamp('1900-01-01'), " ")

# merged_spp_candidate = merged_spp_candidate.fillna("")


# %%
