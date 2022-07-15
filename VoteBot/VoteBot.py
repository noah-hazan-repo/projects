{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "dfe7db2f",
   "metadata": {},
   "outputs": [],
   "source": [
    "import csv\n",
    "import time\n",
    "import pandas as pd\n",
    "from selenium import webdriver\n",
    "from webdriver_manager.chrome import ChromeDriverManager\n",
    "from selenium.webdriver.common.by import By\n",
    "from selenium.webdriver.support.ui import Select\n",
    "from selenium.webdriver.common.keys import Keys\n",
    "\n",
    "voteImage = '----FILL THIS IN-----'\n",
    "\n",
    "def voteSubmitter(first,last,email,phone,zipCode):\n",
    "    driver = webdriver.Chrome(ChromeDriverManager().install())\n",
    "    driver.get('https://www.venturephotography.us/competition/potm-caldwell/')\n",
    "    selectYourFavourite = driver.find_element(By.CSS_SELECTOR,'#nf-field-19')\n",
    "    prepareSelect = Select(selectYourFavourite)\n",
    "    prepareSelect.select_by_visible_text(voteImage)\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-field-20').send_keys(f'{first}')\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-field-21').send_keys(f'{last}')\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-field-22').send_keys(f'{email}')\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-field-23').send_keys(f'{phone}')\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-field-24').send_keys(f'{zipCode}')\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-label-class-field-31-1').click()\n",
    "    driver.find_element(By.CSS_SELECTOR,'#nf-field-28').click()\n",
    "    \n",
    "def executeVoting():\n",
    "    # READ PEOPLE DATA\n",
    "    people = pd.read_csv('us-500.csv')\n",
    "    firsts = list(people['first_name'])\n",
    "    lasts = list(people['last_name'])\n",
    "    emails = list(people['email'])\n",
    "    phones = list(people['phone1'])\n",
    "    zipCodes = list(people['zip'])\n",
    "    isIterable = len(firsts) == len(lasts) == len(emails) == len(phones) == len(zipCodes)\n",
    "    if isIterable:\n",
    "        votes = []\n",
    "        count = 0\n",
    "        for i in range(len(firsts)):\n",
    "            # EXTRACT FIELDS\n",
    "            first = firsts[i]\n",
    "            last = lasts[i]\n",
    "            email = emails[i]\n",
    "            phone = phones[i]\n",
    "            zipCode = zipCodes[i]\n",
    "            #VOTE HERE. DO NOT UNCOMMENT.\n",
    "            #voteSumbitter(first,last,email,phone,zipCode)\n",
    "            votes.append((first,last,email,phone,zipCode))\n",
    "            count+=1\n",
    "            print(f'VoteBot has voted {count} times (and counting!)')\n",
    "            time.sleep(1)\n",
    "    else:\n",
    "        return \"Error: The lengths of each data category differ somewhere).\"\n",
    "\n",
    "executeVoting()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "2e1eaed6",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
