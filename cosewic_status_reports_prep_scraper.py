# %%
# Import libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

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

# Find the table on the webpage.
table = WebDriverWait(driver, 2).until(
    EC.presence_of_element_located((By.TAG_NAME, "table"))
)

# 4. Extract table headers (including nested ones if any)
headers = []
for row in table.find_elements(By.TAG_NAME, "tr"):
    ths = row.find_elements(By.TAG_NAME, "th")
    headers.append([th.text.strip() for th in ths])

# Flatten nested headers if necessary
flat_headers = [item for sublist in headers for item in sublist]

# Create blank container for data. This gets made into a dataframe.
data = []
current_group = None  # Store the most recent big grouping category

# Cycle through all table rows (denoted by the tag 'tr' on the webpage)
# to pull out the cell contents by row.
for row in table.find_elements(By.TAG_NAME, "tr"):
    tds = row.find_elements(By.TAG_NAME, "td")
    if tds:
        # Extract cell text for the current row
        row_data = [td.text.strip() for td in tds]

        # Check if this is a big grouping category row (all other cells are 'None')
        if all(cell == "None" for cell in row_data[1:]):
            current_group = row_data[0]  # Update current group with new section

        # Append the current group as a new column
        row_data.append(current_group)
        data.append(row_data)

# 6. Add the new column name to the headers
flat_headers.append("Group")

# 7. Create a DataFrame from the extracted data
df = pd.DataFrame(data, columns=flat_headers)

df = df[df["Scientific name"].notna()]
# %%

# Write out the scraped dataframe to our data folder.
df.to_csv("data/cosewic_status_reports_prep.csv")

# Close the robotically controlled internet browser.
driver.quit()
