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

def is_text_bold(driver, element):
    script = """
        return window.getComputedStyle(arguments[0]).fontWeight === 'bold';
    """
    return driver.execute_script(script, element)

def get_row_text_size(row):
    cell_sizes = []
    for cell in row.find_elements(By.TAG_NAME, "td"):
        font_size = cell.value_of_css_property("font-size")
        cell_sizes.append(font_size)
    return cell_sizes


def parse_table(table_element):
    table_data = []
    current_h3 = None
    current_strong = None

    for row in table_element.find_elements(By.TAG_NAME, "tr"):
        
        cells = row.find_elements(By.TAG_NAME, "td")

        if not cells:
            continue  # Skip rows with no cells

        # Handle single-cell rows (e.g., headings)
        if len(cells) >= 1:
            try:
                h3_element = cells[0].find_element(By.TAG_NAME, "h3")
                if h3_element:
                    current_h3 = h3_element.text.strip()
            except NoSuchElementException:
                pass  # Handle cases where h3 is not found

        elif len(cells) == 3:
            current_strong = [strong.text.strip() for strong in cells]
                
        else:
            # Handle rows with colspan
            if cells[0].get_attribute("colspan") == "3":
                current_h3 = cells[0].text.strip()
            else:
                # Handle regular data rows
                row_data = []
                if current_h3:
                    row_data.append(current_h3)  # Add the current h3 to the beginning
                for cell in cells:
                    cell_text = cell.text.strip()
                    is_bold = cell.find_element(By.TAG_NAME, "b") is not None
                    row_data.append((cell_text, is_bold))
                table_data.append(row_data)

    return table_data
 
table_xpath = "//*[@id='ca-1529739248826']/main/div/div[2]/table"
table_element = driver.find_element(By.XPATH, table_xpath) 
 
parsed_data = parse_table(table_element)

print(parsed_data)     
#table_data.to_csv("data/table_three_species_specialist.csv")
    
    
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

def clean_category(category):
    return re.sub(r'\(\d+\)', '', category).strip()

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

header = ['Group', 'Priority', 'Common name', 'Scientific Name', 'Location']

with open('data/cosewic_spp_specialist_candidate_list.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    writer.writerows(csv_data)

# %%

# Close the browser
driver.quit()
