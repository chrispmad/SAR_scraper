# %%
# Import libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
from datetime import date

todays_date = date.today().strftime('%Y-%m-%d')


url = "https://www.cosewic.ca/index.php/en/reports/status-reports-preparation.html"

# %%
# Setup down above. Fire up the web driver.

# Create the WebDriver instance. This basically opens a robotically controlled internet browser!
driver = webdriver.Chrome()

# Navigate to the webpage with the robot browser.
driver.get(url)

# Wait for 2 seconds, to be respectful to the webpage's server and to give it time to load.
time.sleep(2)

# %%

# Wait for all h2 and table elements to load
WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.TAG_NAME, "h2"))
)
WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.TAG_NAME, "table"))
)

# Get all h2 and table elements
h2_elements = driver.find_elements(By.TAG_NAME, "h2")[1:]  # Skip the first h2
table_elements = driver.find_elements(By.TAG_NAME, "table")

# Ensure we only process as many tables as we have h2s
assert len(h2_elements) == len(table_elements), "Mismatch between h2s and tables"

all_dataframes = []

# Loop through each h2 and its corresponding table
for h2, table in zip(h2_elements, table_elements):
    group_label = h2.text.strip()  # e.g., "November 2025 (40) wildlife species"

    # Extract headers
    headers = []
    for row in table.find_elements(By.TAG_NAME, "tr"):
        ths = row.find_elements(By.TAG_NAME, "th")
        headers.append([th.text.strip() for th in ths])
    flat_headers = [item for sublist in headers for item in sublist]

    # Extract data rows
    data = []
    for row in table.find_elements(By.TAG_NAME, "tr"):
        tds = row.find_elements(By.TAG_NAME, "td")
        if tds:
            row_data = [td.text.strip() for td in tds]
            row_data.append(group_label)  # Assign group from h2
            data.append(row_data)

    # Add group column
    flat_headers.append("Group")

    # Create DataFrame
    df = pd.DataFrame(data, columns=flat_headers)
    if "Scientific name" in df.columns:
        df = df[df["Scientific name"].notna()]
    all_dataframes.append(df)

# Combine all tables into one
final_df = pd.concat(all_dataframes, ignore_index=True)

final_df = final_df[final_df["Scientific name"].notna()]
# %%

# Write out the scraped dataframe to our data folder.
final_df.to_csv("data/cosewic_status_reports_prep.csv")
todays_date = date.today().strftime('%Y-%m-%d')
final_df.to_csv("data/cosewic_status_reports_prep"+todays_date+".csv")
# Close the robotically controlled internet browser.
driver.quit()

# %%







