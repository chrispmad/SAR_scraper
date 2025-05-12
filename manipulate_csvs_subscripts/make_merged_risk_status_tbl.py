# Merge while keeping both sets of columns
# merged_risk_status = pd.merge(
#     risk_registry,
#     status_report[[  # Selecting the necessary columns from status_report
#         "Unique_ID", "Domain", "Taxonomic group", "Scientific name",
#         "COSEWIC common name", "COSEWIC population", "COSEWIC status",
#         "Scheduled Assessment", "Range"  # Add any other columns that might have missing values
#     ]],
#     on="Unique_ID",
#     how="outer",
#     suffixes=("", "_status")  # Keep distinct column names for status_report
# )

#%% 
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
