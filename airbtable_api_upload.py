#%%
import os
from pyairtable import Api
from airtable import Airtable
import requests
import pandas as pd
import csv
import time
import json

# %%
with open("login/airtable_key.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    token = lines[1].strip()
    print(f"USERNAME = {username}")


base_id = 'appiMHrjH9cntzZC7' # the base of the workspace - change as appropriate
table_id = 'tblaXWXnP0t9Ggdze' # change
view_id = 'viwcjEPJe70stwTwe?blocks=hide' # change

airtable_url = f"https://api.airtable.com/v0/{base_id}"

# %%
# Define the API endpoint and headers
url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

#with open("data/cosewic_spp_specialist_candidate_list.csv") as f:
#    csvFile = csv.reader(f)
#    for lines in csvFile:
#        print(lines)
          
fileToAdd = pd.read_csv("data/cosewic_spp_specialist_candidate_list.csv", encoding='ISO-8859-1')      


# %%
for index, row in fileToAdd.iterrows():
    data = {
        "fields":row.to_dict()
    }
    response = requests.post(url, headers=headers, data = json.dumps(data))
    
    if response.status_code != 200:
        print(f"Error uploading row {index}: {response.json()}")
    time.sleep(0.2)
    
    
# %%
