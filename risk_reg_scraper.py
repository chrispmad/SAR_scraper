# %%
# Import libraries
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re  # Regular expressions!
import time
import os
import glob
import shutil
from datetime import date

url = "https://species-registry.canada.ca/index-en.html#/species?sortBy=commonNameSort&sortDirection=asc&pageSize=100"

# %%
# Setup down above. Fire up the web driver.

# Create the WebDriver instance outside the loop
driver = webdriver.Chrome()

driver.get(url)

time.sleep(2)

# %%
# Click Download Button
#download_button = driver.find_element(By.CLASS_NAME, "sar-export-button")
download_button = driver.find_element(By.XPATH, '//*[@id="page-results"]/div[1]/div[4]/button')
download_button.click()
# %%
# Move downloaded file from Downloads folder to R data folder
time.sleep(5)
downloads_folder = os.path.expanduser("~/Downloads")
current_folder = os.getcwd()
current_data_folder = os.path.join(current_folder, "data")
file_pattern = "SAR Species - [0-9]+ results.csv"

all_downloads_in_folder = os.listdir(downloads_folder)
matching_file = [f for f in all_downloads_in_folder if re.search(file_pattern, f)]


todays_date = date.today().strftime('%Y-%m-%d')
if matching_file:
    downloaded_file = matching_file[0]

    # Copy file to data folder
    shutil.copyfile(
        os.path.join(downloads_folder, downloaded_file),
        current_data_folder + "\\risk_registry.csv",
    )
    
    # Copy file to data folder
    shutil.copyfile(
        os.path.join(downloads_folder, downloaded_file),
        current_data_folder + "\\risk_registry"+todays_date+".csv",
    )

    # Delete file in downloads folder
    os.remove(os.path.join(downloads_folder, downloaded_file))
else:
    print("No files found matching the pattern.")

#driver.quit()

# %%
