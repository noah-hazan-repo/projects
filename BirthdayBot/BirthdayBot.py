import time 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.expected_conditions import presence_of_element_located

def birthdayBot():
    username = '---FILL THIS IN---'
    password = '---FILL THIS IN---'
    # modify browser options to dissalow popups which break selenium
    option = Options()
    option.add_argument("--disable-infobars")
    option.add_argument("start-maximized")
    option.add_argument("--disable-extensions") 
    driv = ChromeDriverManager().install() 
    option.add_experimental_option(
        "prefs", {"profile.default_content_setting_values.notifications": 1})
    # instantiate browser
    browser = webdriver.Chrome(driv, chrome_options=option)
    browser.get('https://www.facebook.com/birthdays/')
    # login
    browser.find_element(By.CSS_SELECTOR,'#email').send_keys(f'{username}')
    browser.find_element(By.CSS_SELECTOR,'#pass').send_keys(f'{password}')
    browser.find_element(By.NAME,'login').click()
    # sleep since moving too quickly seems to break program
    time.sleep(5)
    # grab all names and birthday message boxes (lists)
    names_dates = browser.find_elements(By.CSS_SELECTOR,'.o8rfisnq .i1fnvgqd')
    birthdays = browser.find_elements(By.CSS_SELECTOR,'._1mj') 
    # clean names
    names = []
    for name in names_dates:
        names.append(name.text.split('\n')[0].split(' ')[0])
    names = names[:len(birthdays)]
    # assert names and birthdays are aligned
    # if false, it means bday messages would go to people with the wrong name in the message 
    # thus, we can't allow the below assertion to not be true
    assert len(names) == len(birthdays)
    # zip names and selenium objects for message boxes into dict
    people = dict(zip(names,birthdays))
    # iterate through people and send them a birthday message
    for name, birthday in people.items():
    # hard coded one person I actually wanted to wish a bday on the day of testing (jul 19) ;)
        if name == 'Leora': 
            birthday.send_keys(f'Happy Birthday {name}!')
    # sleep again since moving too quickly breaks program
            time.sleep(5)
            birthday.click()
            birthday.send_keys(Keys.RETURN)
            
# Execute            
birthdayBot()
