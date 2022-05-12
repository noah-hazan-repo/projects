import os
import pandas as pd
import os
import sys
import json
import pandas as pd
import sqlalchemy
import mysql.connector
from mysql.connector import Error
from datetime import datetime
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
amount varchar(1000));""",
        
"""CREATE TABLE IF NOT EXISTS mint.job_logging(
job_dwh_insert_date timestamp NOT NULL
);"""]



DAG = [
        "USE mint;", 
    
        "CREATE TABLE IF NOT EXISTS tmp_transactions_raw AS SELECT * FROM mint.f_transactions_raw;"
]



DAG2 = [
        "USE mint;",

        """INSERT INTO mint.f_transactions_raw SELECT tmp_transactions_raw.*
        FROM   tmp_transactions_raw
        LEFT   JOIN f_transactions_raw USING ( date, description, amount)
        WHERE  f_transactions_raw.date IS NULL; """,

        "DROP TABLE tmp_transactions_raw;"
    
       
]

connection, cursor = get_cursor('Babusafti33')

for sql_task in DDLS:
    execute_sql(sql_task, cursor)

for sql_task in DAG:
    execute_sql(sql_task, cursor)
    
df.to_sql("tmp_transactions_raw", con = engine, if_exists='append', index = False)

job_dwh_insert_date = max(df['dwh_insert_date'])

LOGGING = [
    
    f"INSERT INTO mint.job_logging (job_dwh_insert_date) VALUES ('{job_dwh_insert_date}') "
    
]

for sql_task in LOGGING:
    execute_sql(sql_task, cursor)

for sql_task in DAG2:
    execute_sql(sql_task, cursor)

    
#cursor.execute("SELECT MAX(job_dwh_insert_date) FROM mint.job_logging")
#
#
#seen_dwh_insert_date = cursor.fetchall()
#
#seen_dwh_insert_date[0][0].strftime('%Y-%m-%d %H:%M:%S')
#
#print(seen_dwh_insert_date)
#
# Upsert to table new transactions and include DWH INSERT DATE
# Read from table only unprocessed records and load to new table
# unprocessed = where transaction_date >= max(dwh_insert_date)
# process unprocessed transactions with categorization mapping logic
# append to new table




