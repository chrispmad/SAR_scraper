# Note: some of this code may be redundant.

# Replace any occurrences of 'Populations' in the population field.
risk_registry["Legal population"] = risk_registry["Legal population"].str.replace(r"\b[Pp]opulations?\b", "population", regex=True)
risk_registry["COSEWIC population"] = risk_registry["COSEWIC population"].str.replace(r"\b[Pp]opulations?\b", "population", regex=True)

risk_registry["COSEWIC population"] = risk_registry["COSEWIC population"].replace("populations", "population")
# Define the regex pattern
mask = risk_registry.apply(lambda col: col.astype(str).str.contains(r"\b[Pp]opulations\b", na=False, regex=True)).any(axis=1)
# Filter and print the rows
filtered_rows = risk_registry[mask]
# Drop completely blank rows (if needed)
filtered_rows = filtered_rows.dropna(how="all")