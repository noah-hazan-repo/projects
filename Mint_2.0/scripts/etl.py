from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from mysql.connector import Error
from pprint import pprint
import df2gspread as d2g
import mysql.connector
import pandas as pd
import sqlalchemy
import gspread
import json
import time
import os
import sys


os.chdir('/Users/Noah.Hazan/Downloads/') # Change directory to location of all bank statements.



def gatherStatements(): 
    # Current support only for Chase and Bank of America statements.
    chase = []
    bofa = []
    for file in os.listdir():
        if 'chase' in file or 'Chase' in file:
            chase.append(file)
        elif 'stmt' in file:
            bofa.append(file)
    return chase, bofa

def generateDf(chase, bofa): 
    # Read statements and drop/rename columns to prepare for DataFrame union.
    # Also insert 'dwh_insert_date' at position 0 for ETL purposes.
    # Finally, deduplicate and remove nulls.
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

def getConnection(password):
    # Creates DB connection and generates cursor. 
    # Must be run locally as DB is on my local machine.
    connection = mysql.connector.connect(host='localhost',database='mint',user='root',password=password, autocommit=True)
    return connection

def executeSql(sql, cursor):
    # Simply executes SQL and returns the cursor if wanted.
    # Requires cursor input.
    
    try:
        print("Executing SQL - {}: ".format(sql[:15]), end='') # Display header of SQL query for transparency.
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print("OK")
    return cursor

def dfToSql(df,password,database,schema,target,ifexists='append'):
    engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{password}@localhost/{schema}') # connect to server
    engine.execute(f'CREATE DATABASE IF NOT EXISTS {database}') #create db
    engine.execute(f'USE {schema}') # select new db
    df.to_sql(f'{target}', con = engine, if_exists=f'{ifexists}', index = False)
    print('DataFrame -> SQL Database Complete')
    
    
def transformLoadDf(sourceDf,transform_sql, targetTable):
    conn = getConnection('Babusafti33')
    cur = conn.cursor()
    for sql_task in createTempTable:
        executeSql(sql_task, cur)
    dfToSql(sourceDf,'Babusafti33','mint','mint','tmp_transactions_raw','append')
    for sql_task in insertTempToRaw:
        executeSql(sql_task, cur)
    seen_max_dwh_insert_date = executeSql("SELECT MAX(dwh_insert_date) FROM mint.f_transactions", cur)
    seen_max_dwh_insert_date = seen_max_dwh_insert_date.fetchall()
    transform_sql = transform_sql.format(seen_max_dwh_insert_date[0][0])
    dfCategories = executeSql(transform_sql, cur)
    unprocessed_records = dfCategories.fetchall()
    dfCategories = pd.DataFrame(unprocessed_records,columns =['dwh_insert_date', 'date', 'description', 'category', 'amount'])
    dfToSql(dfCategories, 'Babusafti33', 'mint', 'mint', f'{targetTable}', 'append')
    conn.close()
    
    
def mintTransactions():
    conn = getConnection('Babusafti33')
    cur = conn.cursor()
    data = executeSql("SELECT * FROM mint.f_transactions", cur)
    data = data.fetchall()
    conn.close()
    df = pd.DataFrame(data,columns =['dwh_insert_date', 'date', 'description', 'category', 'amount'])
    df = df.drop_duplicates() # there should be none in theory, if we've done our work well
    df = df.dropna() # there is one expected NULL in the 'amount' column which we initiated the table with
    return df

def dfToSheets(df):
    # Hard coding the values for now.
    os.chdir('/Users/Noah.Hazan/Downloads/')
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    gc = gspread.authorize(creds)
    worksheet = gc.open('MINT_2.0').worksheet('Sheet1')
    df['dwh_insert_date']=df['dwh_insert_date'].astype(str)
    worksheet.update('A1',[df.columns.values.tolist()] + df.values.tolist())

createTempTable = [ "USE mint;", 
       
       "CREATE TABLE IF NOT EXISTS tmp_transactions_raw AS SELECT * FROM mint.f_transactions_raw;"
      ]

insertTempToRaw = [ "USE mint;",
        
        """INSERT INTO mint.f_transactions_raw SELECT tmp_transactions_raw.*
        FROM   tmp_transactions_raw
        LEFT   JOIN f_transactions_raw USING ( date, description, amount)
        WHERE  f_transactions_raw.date IS NULL; """,

        "DROP TABLE tmp_transactions_raw;"
       ]

generateCategories = """SELECT 
                        dwh_insert_date as dwh_insert_date, 
                         cast(date as char(100)) as date,
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
                            WHEN DESCRIPTION LIKE '%MICHAELS%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%CIRCLE K%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%HUMBLE TOAST%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%ANN TAYLOR%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%BEDBATH%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%SAMMY%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%ZAGAFEN%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%COLOR ME%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%BETH JACOB%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%PARK DELI%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%RESTAU%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%NCSY%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%DEPOSIT%' THEN 'INCOME'
                            WHEN DESCRIPTION LIKE '%ZENNI%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%RWJ%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%Duane%' THEN 'CONVENIENCE STORE'
                            WHEN DESCRIPTION LIKE '%TAXRFD%' THEN 'INCOME'
                            WHEN DESCRIPTION LIKE '%MUNICI%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%MEOROT%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BBQ%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%BAKERIST%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%EXPEDIA%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%656 OCEAN%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%HILTON%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%REWARD%' THEN 'INCOME'
                            WHEN DESCRIPTION LIKE '%ROBINHOOD%' AND AMOUNT < 0  THEN 'TRANSFER_TO_EXTERNAL_ACCOUNT'
                            WHEN DESCRIPTION LIKE '%VENMO%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%NATIONWIDE%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%GRAETERS%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%WAL-MART%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%EDEN%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%PARK DENTAL%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%PARKCOLUMBUS%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%CRIMSON%' THEN 'COFFEE'
                            WHEN DESCRIPTION LIKE '%BARNES%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%Theater of%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%CLOTHING%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%JUDAICA%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%ROAD RUNNER%' THEN 'HEALTH'
                            WHEN DESCRIPTION LIKE '%KITCHEN%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%PIZZ%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%WEGMAN%' THEN 'GROCERIES'
                            WHEN DESCRIPTION LIKE '%AIRPORT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%BLOOMINGDAL%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%FEE%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%NJMVC%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%FUNDRA%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%CULVV%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%WOMENS%' THEN 'WIFE'
                            WHEN DESCRIPTION LIKE '%GRILL%' THEN 'RESTAURANT'
                            WHEN DESCRIPTION LIKE '%MACY%' THEN 'WIFE'
                            ELSE 'UNCLASSIFIED'
                        END AS category,
                        amount 
                    FROM 
                        mint.f_transactions_raw 
                    WHERE 
                        dwh_insert_date > '{}'"""


# STEP 1: 

# If first run, or developer wishes to start from scratch, run ddls_and_dmls.sql

# DROP TABLE IF EXISTS mint.f_transactions_raw;
# DROP TABLE IF EXISTS mint.f_transactions;
# DROP TABLE IF EXISTS mint.tmp_transactions_raw;
# DROP TABLE IF EXISTS mint.job_logging;
# 
# 
# CREATE TABLE IF NOT EXISTS mint.f_transactions_raw (
#     dwh_insert_date TIMESTAMP NOT NULL,
#     date VARCHAR(1000) NOT NULL,
#     description VARCHAR(512),
#     amount VARCHAR(1000)
# );
#         
# CREATE TABLE IF NOT EXISTS mint.f_transactions (
#     dwh_insert_date VARCHAR(100) NOT NULL,
#     date VARCHAR(1000) NOT NULL,
#     description VARCHAR(512),
#     category VARCHAR(100),
#     amount VARCHAR(1000)
# );
#         
# CREATE TABLE IF NOT EXISTS mint.job_logging (
#     job_dwh_insert_date TIMESTAMP NOT NULL
# );
# 
# INSERT INTO mint.f_transactions (dwh_insert_date, date, description, category, amount)
# VALUES ('1970-01-01 00:00:00.000000', '1970-01-01', NULL,NULL, NULL);

# STEP 2:

# Gather downloaded statements and generate unioned DataFrame.

chase,bofa = gatherStatements()

df = generateDf(chase,bofa)

transformLoadDf(df, generateCategories, 'f_transactions')

dataDf = mintTransactions()

dfToSheets(dataDf)