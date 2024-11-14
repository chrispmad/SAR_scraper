# %%
import pandas as pd
import functions.airtable_functions as airfuncs
import re  # Regular expressions!
import numpy as np

risk_registry = pd.read_csv("data/risk_registry.csv", encoding="utf-8-sig")
candidate_list = pd.read_csv(
    "data/cosewic_spp_specialist_candidate_list.csv", encoding="ISO-8859-1"
)
status_report = pd.read_csv(
    "data/cosewic_status_reports_prep.csv", encoding="ISO-8859-1"
)
species_tbl = pd.read_csv("data/candidate_species_tbl.csv", encoding="ISO-8859-1")
# %%

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
risk_registry["Legal population"] = risk_registry["Legal population"].str.replace(
    "populations", "population", regex=True
)

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
    + "-"
    + risk_registry["COSEWIC common name"].fillna(" <blank> ")
    + " ("
    + cosewic_population_filled
    + ")"
)

# Remove "(NA)" from UniqueID column.
risk_registry["Unique_ID"] = risk_registry["Unique_ID"].str.replace(
    " (NA)", "", regex=False
)

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
# Reorder DataFrame columns by placing cols_first at the beginning and appending the rest
risk_registry = risk_registry[
    cols_first + [col for col in risk_registry.columns if col not in cols_first]
]

### COSEWIC Status reports

# Replace any occurrences of 'Populations' in the population field.
status_report["Common name"] = status_report["Common name"].str.replace(
    "Population", "population", regex=True
)

# Create Unique_ID column.
status_report["Unique_ID"] = (
    status_report["Scientific name"] + "-" + status_report["Common name"]
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
        ["Freshwater fishes", "Marine fishes", "Marine mammals"]
    )
    | status_report["Common name"].str.match(
        ".*([aA]balone|[oO]yster|[mM]ussel|[lL]anx|[cC]apshell|Hot Springs Snail)"
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

# %%
fileToAdd = fileToAdd[
    fileToAdd.apply(
        lambda row: row.str.contains("|".join(search_string), case=False)
    ).any(axis=1)
]

airfuncs.reassessment_date(risk_registry[:1])
# %%
