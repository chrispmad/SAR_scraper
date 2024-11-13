#%%
import pandas as pd
import functions.airtable_functions as airfuncs

risk_registry = pd.read_csv("data/risk_registry.csv", encoding='utf-8-sig')      
candidate_list = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')
status_report = pd.read_csv("data/cosewic_status_reports_prep.csv", encoding='ISO-8859-1')
species_tbl = pd.read_csv("data/candidate_species_tbl.csv", encoding='ISO-8859-1')
# %%
risk_registry['Domain'] = risk_registry.apply(airfuncs.determine_cosewic_domain, axis=1)
# %%

airfuncs.reassessment_date(risk_registry[:1])
# %%
