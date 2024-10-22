# Packages

library(tidyverse)
library(readxl)

# Read in datasets

# ========================

# 1. Federal Risk Registry
frr = read_csv("data/risk_registry.csv")

# 2. COSEWIC Status Reports Prep
csrp = read_csv("data/cosewic_status_reports_prep.csv")

# 3. COSEWIC candidate species
ccs = read_csv("data/candidate_species_tbl.csv")

# =======================

# Apply filtering and creation of new column steps as outlined by Chrissy

# 1. FRR

frr_clean = frr |> 
  dplyr::mutate(Domain = dplyr::case_when(
    stringr::str_detect(`Taxonomic group`,"(Fishes \\((freshwater|marine)\\)|Mammals \\(marine\\))") ~ 'Aquatic',
    stringr::str_detect(`Taxonomic group`,"(Amphibians|Arthropods|Birds|Lichens|Mammals \\(terrestrial\\)|Mosses|Reptiles|Vascular Plants)") ~ 'Terrestrial',
    `Taxonomic group` == 'Molluscs' & stringr::str_detect(`COSEWIC common name`,"(Abalone|Oyster|Mussel|Lanx|Capshell)") ~ 'Aquatic',
    `Taxonomic group` == 'Molluscs' & !stringr::str_detect(`COSEWIC common name`,"(Abalone|Oyster|Mussel|Lanx|Capshell)") ~ 'Terrestrial',
    T ~ 'Other'
  )) |> 
# Correct all instances of 'populations' to 'population' in columns 'COSEWIC population' and 'Legal population'.
  dplyr::mutate(`COSEWIC population` = stringr::str_replace_all(`COSEWIC population`,"populations","population"),
                `Legal population` = stringr::str_replace_all(`Legal population`,"populations","population")) |> 
# Add unique identifier column: first 2-3 letters of genus and species and population.
  dplyr::mutate(`COSEWIC population` = tidyr::replace_na(`COSEWIC population`,"general")) |>
  dplyr::mutate(unique_id = paste(
    stringr::str_extract(`Scientific name`,"^.{3}"), 
    stringr::str_extract(`Scientific name`, "(?<= ).{3}"),
    stringr::str_extract(`COSEWIC population`, "^.{3}"),
    sep = "_")) |> 
  dplyr::select(unique_id, dplyr::everything()) |> 
# Add reassessment date
  dplyr::mutate(`Estimated re-assessment` = `COSEWIC last assessment date` + lubridate::years(10)) |> 
# Filter frr to just BC and Pacific.
  dplyr::filter(stringr::str_detect(Range, "(British Columbia|Pacific Ocean)"))

# Need to arrange the data in some way that makes sense, so that rows that imply an update
# are treated as such, and perhaps original rows are dropped?




# 2. COSEWIC Status Reports Prep
csrp_clean = csrp |> 
  # Filter to those rows including BC
  dplyr::filter(stringr::str_detect(`Canadian range / known or potential jurisdictions 1`,"BC")) |> 
  # Pull out the month and year from the 'Group' column that we made during the Python webscraping
  dplyr::mutate(`Scheduled Assessment` = stringr::str_extract(Group,'[a-zA-Z]+ [0-9]+')) |> 
  dplyr::mutate(`Scheduled Assessment` = tidyr::replace_na(`Scheduled Assessment`, "Unclear")) |> 
  dplyr::mutate(last_assessment_date = stringr::str_extract(`Last assessment`,"[a-zA-Z]+ [0-9]+")) |> 
  dplyr::mutate(last_assessment_date = lubridate::my(last_assessment_date)) |>
  dplyr::mutate(`Common name with population` = `Common name`) |> 
  dplyr::mutate(`Common name` = stringr::str_remove_all(`Common name`," \\(.*")) |> 
  dplyr::select(`Taxonomic group`,`Common name`,`Scientific name`,`Common name with population`,
                dplyr::everything())

# 3. COSEWIC candidate species
ccs_clean = ccs |> 
  # Filter for just British Columbia/BC/Pacific
  dplyr::filter(stringr::str_detect(`Canadian range / known or potential jurisdictions`,"(British Columbia|BC|Pacific)"))
  
  

