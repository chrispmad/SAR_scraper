# %%


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import time
import csv
import re
from datetime import date

# Initialize WebDriver (ensure 'chromedriver' is in your PATH)
driver = webdriver.Chrome()

# Open the webpage
driver.get(
    "https://www.cosewic.ca/index.php/en/reports/candidate-wildlife-species.html"
)

# Wait for the page to load fully
time.sleep(5)

# %% 


# Store the extracted data
data = []

# Find all <h3> elements (category headers)
#h3_elements = driver.find_elements(By.TAG_NAME, "h3")



# Regular expression to match category labels like "2020 (7)"
category_pattern = r"[0-9]{4} \([0-9]+\)"

h3_elements = []
for the_year in range(2019,2030):
    try: 
        the_element = driver.find_element(By.CSS_SELECTOR,f"[href='#Y{the_year}']")
    except:
        the_element = None
    if the_element:
        print(" We found that year!")
        h3_elements.append(the_element)
#h3_elements = driver.find_element(By.CSS_SELECTOR,"[href='#Y2019']")


#%%

for h3 in h3_elements:
    # Extract the section name from the <h3> element
    section_name = h3.text.strip()

    # Process only valid category sections
    if re.match(category_pattern, section_name):
        print(f"Processing section: {section_name}")

        # Try to expand the section by clicking the <a> child element
        try:
            if h3.get_attribute("class") == "text-success":
                ActionChains(driver).move_to_element(h3).click().perform()
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
                # Safely extract key-value pairs
                row = {}
                for key_value in species_data:
                    parts = key_value.split(":", 1)
                    key = parts[0].strip()
                    value = parts[1].strip() if len(parts) > 1 else ""
                    row[key] = value

                # Add the category as a column
                row["Category"] = section_name

                # Add the row to the data list
                data.append(row)
                
# Convert the collected data into a DataFrame
df = pd.DataFrame(data)

# %%
todays_date = date.today().strftime('%Y-%m-%d')
df.to_csv("data/candidate_species_tbl"+todays_date+".csv")
df.to_csv("data/candidate_species_tbl.csv")
    
    
    
    
#%%
def scrape_ssc_tables(driver):
    rows = []
    for table in driver.find_elements(By.TAG_NAME, "table"):
        try:
            heading = table.find_element(By.XPATH, "preceding::h2[1]").text.strip()
        except NoSuchElementException:
            continue
        m = re.search(r"\((\d+)\)\s*$", heading)
        if not m:
            continue                      # skips non-taxon tables
        taxon = re.sub(r"\s*\(\d+\)\s*$", "", heading)
        expected = int(m.group(1))

        priority = None
        n_rows = 0
        for tr in table.find_elements(By.TAG_NAME, "tr"):
            cells = tr.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue
            texts = [c.text.strip() for c in cells]

            # priority label row: only the first cell has text
            if not any(texts[1:]):
                if texts[0] and "no candidates currently listed" not in texts[0].lower():
                    priority = texts[0]
                continue

            bold = bool(cells[0].find_elements(By.CSS_SELECTOR, "strong, b"))
            rows.append({
                "Group": taxon,
                "Priority": priority,
                "Common name": texts[0],
                "Scientific name": texts[1],
                "Location": texts[2],
                "Selected for assessment": bold,
            })
            n_rows += 1

        flag = "" if n_rows == expected else f"  <-- heading says {expected}"
        print(f"{taxon}: {n_rows} rows{flag}")

    return pd.DataFrame(rows)


df = scrape_ssc_tables(driver)

header = ['Group', 'Priority', 'Common name', 'Scientific name', 'Location',]

# with open('data/cosewic_spp_specialist_candidate_list.csv', 'w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerow(header)
#     writer.writerows(csv_data)
    
# with open('data/cosewic_spp_specialist_candidate_list'+todays_date+'.csv', 'w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerow(header)
#     writer.writerows(csv_data)




# %%
# Define the file path
file_path = "data/cosewic_spp_specialist_candidate_list.csv"
dated_file_path = f"data/cosewic_spp_specialist_candidate_list{todays_date}.csv"

# Save the DataFrame to CSV
df.to_csv(file_path, index=False)  # No index column
df.to_csv(dated_file_path, index=False)  # Save with today's date

# Close the browser
driver.quit()

# %%
