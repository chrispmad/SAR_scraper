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

# 4. COSEWIC spp candidate spceies
cosspp = read_csv("data/cosewic_spp_specialist_candidate_list.csv")

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
  # Make sure we fix this de-capitalization code!!
  # dplyr::mutate(`COSEWIC population` = str_replace_all(`COSEWIC population`,"[a-zA-Z] ",str_to_lower("\1"))) |> 
  dplyr::mutate(`Estimated re-assessment` = `COSEWIC last assessment date` + lubridate::years(10)) |> 
# Filter frr to just BC and Pacific.
  dplyr::filter(stringr::str_detect(Range, "(British Columbia|Pacific Ocean)")) |> 
  dplyr::filter(`Legal population` != `COSEWIC population` & !is.na(`Legal population`) & !is.na(`COSEWIC population`)) |> 
  dplyr::select(`Legal population`, `COSEWIC population`)
  # dplyr::mutate(coalesce_test = coalesce(`Legal population`,`COSEWIC population`)) |> 
  # dplyr::select(coalesce_test)
  # dplyr::filter(`Taxonomic group` %in% c("Amphibians","Molluscs","Fishes (freshwater)"))

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
                dplyr::everything()) |> 
  dplyr::select(-...1) |> 
  # Pull out the population from the common name field.
  dplyr::mutate(Population = str_extract_all(`Common name with population`,"(?<=\\().*(?=\\))")) |> 
  dplyr::mutate(Population = str_replace(Population, "Population", "population")) |> 
  dplyr::mutate(Population = ifelse(Population == 'character(0)', NA, Population))


# 3. COSEWIC candidate species
ccs_clean = ccs |> 
  # Filter for just British Columbia/BC/Pacific
  dplyr::filter(stringr::str_detect(`Canadian range / known or potential jurisdictions`,"(British Columbia|BC|Pacific)")) |>
  # Add year column pulled out of 'Category' column.
  dplyr::mutate(`Candidate List Year` = as.numeric(str_extract(Category,'^[0-9]{4}'))) |> 
  # dplyr::filter(`Taxonomic group` %in% c("Amphibians","Freshwater Fishes","Molluscs","Reptiles")) |> 
  dplyr::select(-...1) |> 
  dplyr::mutate(`Candidate List` = 'COSEWIC',
                Priority = "COSEWIC - Group 1")
  
# 4. COSEWIC specialist candidate species
cosspp_clean = cosspp |> 
  dplyr::filter(stringr::str_detect(Location,"(British Columbia|BC|Pacific)")) |>
  dplyr::mutate(`Candidate List Year` = NA,
                `Candidate List` = "SSC") |> 
  dplyr::rename(`Scientific name` = `Scientific Name`)
  # dplyr::filter(Group %in% c("Amphibians","Freshwater Fishes","Molluscs","Reptiles"))
  

# Merge tables!

merged_tbl = frr_clean |>
  dplyr::left_join(
    dplyr::bind_rows(
      ccs_clean |> dplyr::select(`Scientific name`,Priority,`Candidate List`,`Candidate List Year`,Rationale),
      cosspp_clean |> dplyr::select(`Scientific name`,Priority,`Candidate List`,`Candidate List Year`)
    )
  )
  # Bind together rows of the COSEWIC candidate species list and the SSC list into one table.
  
merged_tbl  |> 
  dplyr::full_join(
    csrp_clean |> dplyr::select(`Scientific name`,`Legal population` = Population,`Last assessment` = last_assessment_date,
                                `Stage of current assessment`,`Scheduled Assessment`)
  )

# merge_tb = frr_clean |> 
#   dplyr::select(`Common Name` = `Legal common name`, `Scientific name`,`Taxonomic group`,Domain,`COSEWIC status`,
#                 `COSEWIC common name`,`COSEWIC population`,`COSEWIC Assessed Date` = `COSEWIC last assessment date`,
#                 `Estimated COSEWIC Reassess` = `Estimated re-assessment`,`Schedule status`,dplyr::everything()) |> 
#   set_names(stringr::str_to_title)
# 
# csrp_clean = csrp_clean |> 
#   set_names(stringr::str_to_title)

# merge_tb |> 
#   dplyr::full_join(csrp_clean)
# Write files out to Teams folder
write.csv(frr_clean, "output/frr_clean.csv", row.names = F)
write.csv(csrp_clean, "output/csrp_clean.csv", row.names = F)
write.csv(ccs_clean, "output/ccs_clean.csv", row.names = F)
write.csv(cosspp_clean, "output/cos_spp_clean.csv", row.names = F)
