

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

print("Finished White Sturgeon Clean-up")