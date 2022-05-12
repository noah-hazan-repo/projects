import os
import pandas as pd
import os
import sys
import json
import pandas as pd
import sqlalchemy
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import time 

from sqlalchemy import create_engine
os.chdir('/Users/Noah.Hazan/Downloads/')

# Flow:

# 1) Gather all downloaded statements
# 2) Generate single df from statements. 
#    Only transformations are dropping redundant cols and renaming so dfs can be concatenated.
#3) Upload to MySQL DB and dedup the data on load. Merge? Upsert? 


def gatherStatements():
    chase = []
    bofa = []
    for file in os.listdir():
        if 'chase' in file or 'Chase' in file:
            chase.append(file)
        elif 'stmt' in file:
            bofa.append(file)
    return chase, bofa

def generateDf(chase, bofa):
    dfs = []
    for path in bofa:
        df = pd.read_csv(path, skiprows = 6)
        df = df.drop(columns=['Running Bal.'])
        dfs.append(df)
    for path in chase:
        df = pd.read_csv(path)
        df = df.drop(columns=['Post Date', 'Category', 'Type', 'Memo'])
        df = df.rename(columns={'Transaction Date' : 'Date'})
        dfs.append(df)

    df = pd.concat(dfs)
    df = df.rename(columns={'Date':'date', 'Description':'description', 'Amount':'amount'})
    df.insert(loc = 0,column = 'dwh_insert_date',value = str(datetime.now()))
    df = df.drop_duplicates()
    df = df.dropna()
    return df

def execute_sql(sql, cursor):
    try:
        print("Executing SQL - {}: ".format(sql[:15]), end='')
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")

        
def get_cursor(password):
    connection = mysql.connector.connect(host='localhost',database='mint',user='root',password=password, autocommit=True)
    cursor = connection.cursor()
    print("MySQL cursor now connected")
    return connection,cursor 


chase,bofa = gatherStatements()
df = generateDf(chase,bofa)




engine = sqlalchemy.create_engine('mysql+pymysql://root:Babusafti33@localhost/mint') # connect to server
engine.execute("CREATE DATABASE IF NOT EXISTS mint") #create db
engine.execute("USE mint") # select new db
connection, cursor = get_cursor('Babusafti33') # cursor


DDLS = [ """CREATE SCHEMA IF NOT EXISTS mint;""",
        
"""CREATE TABLE IF NOT EXISTS mint.f_transactions_raw(
dwh_insert_date timestamp NOT NULL,
date varchar(1000) NOT NULL,
description varchar(512),
amount varchar(1000));""",
        
"""CREATE TABLE IF NOT EXISTS mint.f_transactions(
dwh_insert_date timestamp NOT NULL,
date varchar(1000) NOT NULL,
description varchar(512),
category varchar(100),
amount varchar(1000));""",
        
"""CREATE TABLE IF NOT EXISTS mint.job_logging(
job_dwh_insert_date timestamp NOT NULL
);"""]




for sql_task in DDLS:
    execute_sql(sql_task, cursor)
    

DAG = [
        "USE mint;", 
    
        "CREATE TABLE IF NOT EXISTS tmp_transactions_raw AS SELECT * FROM mint.f_transactions_raw;"
]

for sql_task in DAG:
    execute_sql(sql_task, cursor)
    
df.to_sql("tmp_transactions_raw", con = engine, if_exists='append', index = False)



    
DAG2 = [
        "USE mint;",

        """INSERT INTO mint.f_transactions_raw SELECT tmp_transactions_raw.*
        FROM   tmp_transactions_raw
        LEFT   JOIN f_transactions_raw USING ( date, description, amount)
        WHERE  f_transactions_raw.date IS NULL; """,

        "DROP TABLE tmp_transactions_raw;"
    
       
]


for sql_task in DAG2:
    execute_sql(sql_task, cursor)
    

# until now we're loading distinct records... just need logic ON TOP OF THIS to help out
    
#job_dwh_insert_date = max(df['dwh_insert_date'])

#job_dwh_insert_date = datetime.fromisoformat(job_dwh_insert_date) 


connection, cursor = get_cursor('Babusafti33') # cursor
cursor.execute("SELECT MAX(dwh_insert_date) FROM mint.f_transactions")

seen_max_dwh_insert_date = cursor.fetchall()[0][0] 

print(seen_max_dwh_insert_date)

cursor.execute(f"""SELECT 
                        dwh_insert_date, 
                        date, 
                        description,
                        CASE WHEN DESCRIPTION LIKE '%QUADPAY%' THEN 'INCOME' 
                            WHEN DESCRIPTION LIKE '%SAINT PETERS%' THEN 'INCOME'
                            WHEN DESCRIPTION LIKE '%RAINBOW CLEANERS%' THEN 'CLEANERS'
                            WHEN DESCRIPTION LIKE '%COFFEE%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%RITE AID%' THEN 'CONVENIENCE STORE'
                            WHEN DESCRIPTION LIKE '%CHINA%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%GOLDMAN SACHS%' THEN 'TRANSFER_TO_EXTERNAL_ACCOUNT'
                            WHEN DESCRIPTION LIKE '%PAYMENT%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%PAYROLL%' THEN 'INCOME'
                            WHEN DESCRIPTION LIKE '%HONDA%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%YAD ELIEZER%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%LOIS E. SHULM%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%TARGET%' THEN 'TARGET'
                            WHEN DESCRIPTION LIKE '%TOMCHEI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%OIL%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%SUNOCO%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%DUNKIN%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%STARBUCKS%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%SHEETZ%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%STOP & SHOP%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%INSTACART%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%SUNOCO%' THEN 'GAS'
                            WHEN DESCRIPTION LIKE '%PSEG%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%PIZZA%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%SUSHI%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%VERIZON%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%OPTIMUM%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%DEPT EDUCATION%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%ONLINE PMT%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%Amazon%' THEN 'AMAZON'
                            WHEN DESCRIPTION LIKE '%CHASE CREDIT%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%Trnsfr%' THEN 'TRANSFER_TO_EXTERNAL_ACCOUNT'
                            WHEN DESCRIPTION LIKE '%FLORA%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%Bill Pay%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%Check%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%transfer%' THEN 'TRANSFER_TO_EXTERNAL_ACCOUNT'
                            WHEN DESCRIPTION LIKE '%BLUESTONE%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%RWJ NEW%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%SPOTIFY%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%APPLE%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%ATM%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%Marcus Invest%' THEN 'TRANSFER_TO_EXTERNAL_ACCOUNT'
                            WHEN DESCRIPTION LIKE '%DISNEY%' THEN 'INCOME'
                            WHEN DESCRIPTION LIKE '%MTA%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%WELLS FARGO%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%AMZN%' THEN 'AMAZON'
                            WHEN DESCRIPTION LIKE '%BAKERY%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%WHOLEFDS%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%TRANSACTION FEE%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%MINI GOLF%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%NINTENDO%' THEN 'PERSONAL'
                            WHEN DESCRIPTION LIKE '%BRIDGE TURKISH%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%LYFT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%UBER%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%BED BATH%' THEN 'HOUSE'
                            WHEN DESCRIPTION LIKE '%CAFE%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%RACEWAY%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%CVS%' THEN 'CONVENIENCE STORE'
                            WHEN DESCRIPTION LIKE '%DERECHETZCH%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%STAUF%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%UNITED%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%CAR%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%FEDEX%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%DERECH%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%KOLLEL%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%WITHDRWL%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%NJT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%DOLLAR-A-DAY%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%CITRON%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%GREAT CLIPS%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%PENSTOCK%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%HOTEL%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%RESTAURANT%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%PAYPAL%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%WALGREENS%' THEN 'CONVENIENCE STORE'
                            WHEN DESCRIPTION LIKE '%NAILS%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%GIVING%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%AIRBNB%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%STOP &%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%BUDGET.COM%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%SPIRIT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%CLEANERS%' THEN 'CLEANERS'
                            WHEN DESCRIPTION LIKE '%DUANE READE%' THEN 'CONVENIENCE STORE'
                            WHEN DESCRIPTION LIKE '%GET AIR%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%VENDING%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%CONG.%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%7-ELEVEN%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%GLATT 27%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%BEXLEY MKT%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%TRADER JOE%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%HEADWAY%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%EXXON%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%CHAI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BUY BUY%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%HYATT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%KOSHER%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%HAVA JAVA%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%KOHL%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%SHOPRITE%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%OLD NAVY%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%AMC%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%SPEEDWAY%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%COFF%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%PHARMA%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%CABOKI%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%RITA%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%ROASTERY%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%KROGER%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%FINES AND COSTS%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%MUSIC%' THEN 'PERSONAL'
                            WHEN DESCRIPTION LIKE '%KITTIE%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%CHASDEI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BESTBUY%' THEN 'PERSONAL'
                            WHEN DESCRIPTION LIKE '%CHICKIES%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%SPA%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%PRIME%' THEN 'AMAZON'
                            WHEN DESCRIPTION LIKE '%ROCKNROLL%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%PINOT%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%WALMART%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%BOUTIQUE%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%ZAGE%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%SKI%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%QUICK CHEK%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%THEATRE%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%FIRESIDE%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%PARKING%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%YOLANDA%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%TJMA%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%KISSENA%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%PARTY%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%STAPLES%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%YERUSHALAYIM%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%LAWRENCE%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%LOFT%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%ANTHRO%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%Travel%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%VICTORIA%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%TAXI%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%TAVLIN%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%GIVING%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%SCHNITZ%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%PARK P%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%DELI KAS%' THEN 'RESTAURANT'
                            ELSE 'OTHER'
                        END AS category,
                        amount 
                    FROM 
                        mint.f_transactions_raw 
                    WHERE 
                        dwh_insert_date >= '{seen_max_dwh_insert_date}'""")

unprocessed_records = cursor.fetchall()

df = pd.DataFrame(unprocessed_records,columns =['dwh_insert_date', 'date', 'description', 'category', 'amount'])
df.to_sql("f_transactions", schema = 'mint' , con = engine, if_exists='append', index = False)   
    
    







