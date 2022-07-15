# The purpose of this is to simulate how it would look to auto-vote for a contest online.
# For ethical reasons, this code was never run (and the contest is long-over by now).
# However, this code is a good template to use for similar web scraping in the future.


import csv
import time
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys

voteImage = '----FILL THIS IN-----'

def voteSubmitter(first,last,email,phone,zipCode):
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get('https://www.venturephotography.us/competition/potm-caldwell/')
    selectYourFavourite = driver.find_element(By.CSS_SELECTOR,'#nf-field-19')
    prepareSelect = Select(selectYourFavourite)
    prepareSelect.select_by_visible_text(voteImage)
    driver.find_element(By.CSS_SELECTOR,'#nf-field-20').send_keys(f'{first}')
    driver.find_element(By.CSS_SELECTOR,'#nf-field-21').send_keys(f'{last}')
    driver.find_element(By.CSS_SELECTOR,'#nf-field-22').send_keys(f'{email}')
    driver.find_element(By.CSS_SELECTOR,'#nf-field-23').send_keys(f'{phone}')
    driver.find_element(By.CSS_SELECTOR,'#nf-field-24').send_keys(f'{zipCode}')
    driver.find_element(By.CSS_SELECTOR,'#nf-label-class-field-31-1').click()
    driver.find_element(By.CSS_SELECTOR,'#nf-field-28').click()
    
def executeVoting():
    # READ PEOPLE DATA
    people = pd.read_csv('us-500.csv')
    firsts = list(people['first_name'])
    lasts = list(people['last_name'])
    emails = list(people['email'])
    phones = list(people['phone1'])
    zipCodes = list(people['zip'])
    isIterable = len(firsts) == len(lasts) == len(emails) == len(phones) == len(zipCodes)
    if isIterable:
        votes = []
        count = 0
        for i in range(len(firsts)):
            # EXTRACT FIELDS
            first = firsts[i]
            last = lasts[i]
            email = emails[i]
            phone = phones[i]
            zipCode = zipCodes[i]
            #VOTE HERE. DO NOT UNCOMMENT.
            #voteSumbitter(first,last,email,phone,zipCode)
            votes.append((first,last,email,phone,zipCode))
            count+=1
            print(f'VoteBot has voted {count} times (and counting!)')
            time.sleep(1)
    else:
        return "Error: The lengths of each data category differ somewhere)."

executeVoting()
