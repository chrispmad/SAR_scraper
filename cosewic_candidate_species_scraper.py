# %%

import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import time

# Initialize WebDriver (ensure 'chromedriver' is in your PATH)
driver = webdriver.Chrome()

# Open the webpage
driver.get(
    "https://www.cosewic.ca/index.php/en/reports/candidate-wildlife-species.html"
)

# Wait for the page to load fully
time.sleep(10)

# %%

# Store the extracted data
data = []

# Find all <h3> elements (category headers)
h3_elements = driver.find_elements(By.TAG_NAME, "h3")

# Regular expression to match category labels like "2020 (7)"
category_pattern = r"[0-9]{4} \([0-9]+\)"

for h3 in h3_elements:
    # Extract the section name from the <h3> element
    section_name = h3.text.strip()

    # Process only valid category sections
    if re.match(category_pattern, section_name):
        print(f"Processing section: {section_name}")

        # Try to expand the section by clicking the <a> child element
        try:
            child_div = h3.find_element(By.TAG_NAME, "a")
            if child_div.get_attribute("class") == "text-info":
                ActionChains(driver).move_to_element(child_div).click().perform()
                time.sleep(2)  # Small delay to allow content to load
        except Exception as e:
            print(f"No clickable div found for section: {section_name}. Error: {e}")
            continue

        # Find the 'collapse' div containing the species entries
        try:
            collapse_div = h3.find_element(
                By.XPATH, "./following-sibling::div[contains(@class, 'collapse')]"
            )
            species_text = collapse_div.text  # Get all the text from the section
        except Exception as e:
            print(f"Error finding content div: {e}")
            continue

        # Split the text by newlines to get individual lines (key-value pairs or related data)
        lines = [
            line.strip() for line in species_text.split("\n") if line.strip()
        ]  # Remove any empty lines

        # Process every 6 lines as one species entry
        for i in range(0, len(lines), 6):
            species_data = lines[i : i + 6]  # Get the next 6 lines

            if len(species_data) == 6:  # Ensure it's a complete entry
                # Extract key-value pairs from the lines
                row = {
                    key_value.split(":", 1)[0]
                    .strip(): key_value.split(":", 1)[1]
                    .strip()
                    for key_value in species_data
                }

                # Add the category as a column
                row["Category"] = section_name

                # Add the row to the data list
                data.append(row)

# Convert the collected data into a DataFrame
df = pd.DataFrame(data)

# Print the DataFrame to verify
print(df)

# %%
df.to_csv("data/candidate_species_tbl.csv")
# %%

# Close the browser
driver.quit()
