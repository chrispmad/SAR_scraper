#%%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import csv
import re
import os


#%%

driver = webdriver.Chrome()

    # Open airtable login page
driver.get('https://airtable.com/login')
print("Opened airtable login page")
time.sleep(5)    

user_login = driver.find_element(By.ID, "emailLogin")
continue_button = driver.find_element(By.XPATH, "//*[@id='sign-in-form-fields-root']/button")
with open("login/airtable_login.txt") as f:
    lines = f.readlines()
    username = lines[0].strip()
    password = lines[1].strip()
    print(f"USERNAME = {username}")

user_login.clear()
user_login.send_keys(username)
continue_button.click()
time.sleep(1)

user_pw = driver.find_element(By.ID, "passwordLogin")
sign_button = driver.find_element(By.XPATH, "//*[@id='sign-in-form-fields-root']/button")
user_pw.clear()
user_pw.send_keys(password)
sign_button.click()
time.sleep(1)


# %%
def import_new_csv(fileToImport):
    import_button = driver.find_elements(By.XPATH, "//*[@id='0d1e740adffe135550f6d36b1bfc6d80']/div[2]/div[2]/div[2]/p")
    import_button.click()



#%%   
with open("data/cosewic_spp_specialist_candidate_list.csv") as 
import_new_csv