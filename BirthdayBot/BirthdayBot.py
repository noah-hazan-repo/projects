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

def createBrowser():
    option = Options()

    option.add_argument("--disable-infobars")

    option.add_argument("start-maximized")

    option.add_argument("--disable-extensions")

    driv = ChromeDriverManager().install() 

    option.add_experimental_option(
        "prefs", {"profile.default_content_setting_values.notifications": 1})

    browser = webdriver.Chrome(driv, chrome_options=option)
    
    return browser

def getBirthdays(username, password):
    
    browser = createBrowser()

    browser.get('https://www.facebook.com/birthdays/')

    browser.find_element(By.CSS_SELECTOR,'#email').send_keys(f'{username}')

    browser.find_element(By.CSS_SELECTOR,'#pass').send_keys(f'{password}')

    browser.find_element(By.NAME,'login').click()
    
    names_dates = browser.find_elements(By.CSS_SELECTOR,'.o8rfisnq .i1fnvgqd')
    
    birthdays = browser.find_elements(By.CSS_SELECTOR,'._1mj') #list
    
    names = []

    for name in names_dates:

        names.append(name.text.split('\n')[0].split(' ')[0])

    names = names[:len(birthdays)]
    
    print(names,birthdays)
    
    return names,birthdays


def birthdayBot(names,birthdays):
    
    assert len(names) == len(birthdays)

    for i in range(len(birthdays)):

        name = names[i].text.split('\n')[0].split(' ')[0]

        birthday = birthdays[i]

        birthday.send_keys(f'Happy Birthday, {name}!')

        birthday.click()

        birthday.send_keys(Keys.RETURN)

birthdayBot(getBirthdays(username,password))
