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
species_tbl["Date_nominated"] = species_tbl["Category"].str[:4]
# species_tbl = species_tbl.assign(Date_nominated=species_tbl["Category"].str[:4])
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
    "Rationale"
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

candidate_list["Priority"] = candidate_list["group"]
del candidate_list["group"]
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

candidate_list = candidate_list[["Unique_ID",
    "Domain",
    "Taxonomic group",
    "Scientific name",
    "COSEWIC common name",
    "Candidate list",
    "Priority"]]



