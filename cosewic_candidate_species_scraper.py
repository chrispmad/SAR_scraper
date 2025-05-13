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

# Print the DataFrame to verify
print(df)

# %%
todays_date = date.today().strftime('%Y-%m-%d')
df.to_csv("data/candidate_species_tbl"+todays_date+".csv")
df.to_csv("data/candidate_species_tbl.csv")
    
#%%
table_xpath = "//*[@id='ca-1529739248826']/main/div/div[2]/table"
table_element = driver.find_element(By.XPATH, table_xpath) 
table_data = []

 
 
 
 
def parse_table(table_element):
    table_data = []
    previous_h3 = ""
    previous_strong = ""

    for row in table_element.find_elements(By.TAG_NAME, "tr"):
        cells = row.find_elements(By.TAG_NAME, "td")

        if not cells:
            continue

        # Check if the row contains only one cell with an h3 or strong element
        if len(cells) == 1:
            single_cell = cells[0]
            h3_elements = single_cell.find_elements(By.TAG_NAME, "h3")
            strong_elements = single_cell.find_elements(By.TAG_NAME, "strong")

            if h3_elements:
                previous_h3 = h3_elements[0].text.strip()
                continue  # Skip to the next row
            elif strong_elements:
                previous_strong = strong_elements[0].text.strip()
                continue  # Skip to the next row

        # Process rows with multiple cells or rows without h3/strong in the first cell
        current_row_data = []
        for cell in cells:
            h3_text = ""
            strong_text = ""
            cell_text = cell.text.strip()

            h3_elements = cell.find_elements(By.TAG_NAME, "h3")
            if h3_elements:
                h3_text = h3_elements[0].text.strip()
            else:
                h3_text = previous_h3

            strong_elements = cell.find_elements(By.TAG_NAME, "strong")
            if strong_elements:
                strong_text = strong_elements[0].text.strip()
            else:
                strong_text = previous_strong

            current_row_data.append((h3_text, strong_text, cell_text))

        table_data.append(current_row_data)

    return table_data

table_data = parse_table(table_element)

# get rid of the numbers in front of the type of organism
def clean_category(category):
    return re.sub(r'\(\d+\)', '', category).strip()

#naming the items in the lists so they can be referred to. Species also includes location
def tuple_to_dict(tup):
    return {
        'category': clean_category(tup[0]),
        'priority': tup[1],
        'species': tup[2] if len(tup) > 2 else ''
    }
    
# Process the data and store in rows for CSV
csv_data = []
for sublist in table_data:
    dict_list = [tuple_to_dict(tup) for tup in sublist]
    
    # Organize each row in the desired order
    row = [
        dict_list[0]['category'],               # Cleaned Category
        dict_list[0]['priority'],               # Priority group
        *(d['species'] for d in dict_list)      # Species and location
    ]
    csv_data.append(row)
#header names for the csv file

# Convert list to DataFrame
df = pd.DataFrame(csv_data, columns=["col1", "group", "species", "scientific_name", "regions"])
# Identify rows that define a new category (category name is in column 2, others are NaN)
df["category"] = df["species"].where(df["scientific_name"].isna())  # Extract categories
df["category"] = df["category"].fillna(method="ffill")  # Forward-fill category to other rows
df = df.drop(columns=["col1"])

# Remove category-defining rows (rows where scientific_name is NaN)
df = df.dropna(subset=["scientific_name"])

df = df.rename(columns={"species": "Common name", "scientific_name": "Scientific name", "regions": "Location", "category": "Group"})

df["Group"] = df["Group"].str.replace(r"\s*\(.*?\)", "", regex=True)

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
