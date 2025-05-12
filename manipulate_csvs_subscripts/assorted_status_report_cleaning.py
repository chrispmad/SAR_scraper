

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
# status_report = status_report.rename(columns={'Last assessment': 'COSEWIC last assessment'})
status_report["COSEWIC last assessment"] = status_report['Last assessment']

# Pull date out from "Group" column. Some rows say "Date not determined" - those get a 9999-01-01.
status_report['Scheduled Assessment'] = status_report['Group'].apply(updatefuncs.extract_month_year_from_group)
# status_report['Scheduled Assessment'] = status_report['Group'].apply(updatefuncs.extract_month_year)

status_report["COSEWIC status"] = status_report["COSEWIC last assessment"].apply(updatefuncs.extract_status)
status_report["COSEWIC last assessment corrected"] = status_report["COSEWIC last assessment"].apply(updatefuncs.extract_month_year)

print("Finished the assorted status report cleaning steps.")