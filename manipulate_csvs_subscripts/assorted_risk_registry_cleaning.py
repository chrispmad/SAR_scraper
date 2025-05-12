## remove where the cosewic population has "watershed" or "River" - delete those keywords
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

# White Sturgeon specific clean-up
exec(open("manipulate_csvs_subscripts/white_sturgeon_bits.py").read())

#%% 

# fix the na to be non-active
risk_registry.loc[(risk_registry["COSEWIC status"].isnull()), "COSEWIC status"] = "Non-active"


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

