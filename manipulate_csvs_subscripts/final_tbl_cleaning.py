
## Fixing the NAs and the dates
# convert all types of file to string - this should be changed later to be appropriate types
merged_risk_status = merged_risk_status.astype(str) 

# Ensure true NaN values are properly handled
merged_risk_status.replace({pd.NA: "NA", None: "NA"}, inplace=True)
merged_risk_status = merged_risk_status.map(lambda x: "NA" if pd.isna(x) or str(x).strip().lower() == "nan" else x)

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
merged_spp_candidate = merged_spp_candidate.astype(str).map(lambda x: "NA" if pd.isna(x) or str(x).strip().lower() == "nan" else x)

# Standardize "Date nominated" field separately
merged_spp_candidate["Date nominated"] = merged_spp_candidate["Date nominated"].replace(
    {pd.NA: "9999", None: "9999", "nan": "9999", "NA": "9999"}
)
