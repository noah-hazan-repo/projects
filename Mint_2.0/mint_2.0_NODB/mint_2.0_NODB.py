from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from mysql.connector import Error
from pprint import pprint
import df2gspread as d2g
import pandasql as ps
import pandas as pd
import gspread
import time
import json
import sys
import os
os.chdir('/Users/Noah.Hazan/Downloads/') # Change directory to location of all bank statements.

def chaseDf():
    paths_to_chase_files = []
    chase_dfs = []
    for file in os.listdir():
        if 'chase' in file or 'Chase' in file:
            paths_to_chase_files.append(file)
    for path in paths_to_chase_files:
        df = pd.read_csv(path)
        
        if 'Details' in df.columns:
            df = df.drop(columns=['Details', 'Type', 'Balance', 'Check or Slip #'])
            df.rename(columns={'Posting Date':'Date'})
        else:    
            df = df.drop(columns=['Post Date', 'Type', 'Memo'])
            df = df.rename(columns={'Transaction Date' : 'Date', 'Category':'bank_category'})
        chase_dfs.append(df)
    df = pd.concat(chase_dfs)
    return df
    
def bofaDf():
    paths_to_bofa_files = []
    bofa_dfs = []
    for file in os.listdir():
        if 'stmt' in file:
            paths_to_bofa_files.append(file)
    for path in paths_to_bofa_files:
        df = pd.read_csv(path, skiprows = 6)
        df = df.drop(columns=['Running Bal.'])
        bofa_dfs.append(df)  
    df = pd.concat(bofa_dfs)
    df.insert(loc = 2,column = 'bank_category',value = 'None')
    return df

def banksDf(): 
    # Read statements and drop/rename columns to prepare for DataFrame union.
    # Also insert 'dwh_insert_date' at position 0 for ETL purposes.
    # Finally, deduplicate and remove nulls.
    chase_df = chaseDf()
    bofa_df = bofaDf()
    dfs = [chase_df, bofa_df]
    df = pd.concat(dfs)
    df = df.rename(columns={'Date':'date', 'Description':'description', 'Category':'bank_category', 'Amount':'amount'})
    df.insert(loc = 0,column = 'dwh_insert_date',value = str(datetime.now()))
    df = df.drop_duplicates()
    df = df.dropna()
    df['amount'] = df['amount'].astype('str').str.replace(',','').astype('float')
    return df


    


def dfToSheets(df):
    # Hard coding the values for now.
    os.chdir('/Users/Noah.Hazan/Downloads/')
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    gc = gspread.authorize(creds)
    worksheet = gc.open('MINT_2.0').worksheet('Sheet1')
    df['dwh_insert_date']=df['dwh_insert_date'].astype(str)
    worksheet.clear()
    worksheet.update('A1',[df.columns.values.tolist()] + df.values.tolist())


    

transform_query = """

with precise_categories as (

SELECT 
                        dwh_insert_date as dwh_insert_date, 
                         cast(date as char(100)) as date,
                        description,
                        cast(amount as float) as amount,
                        CASE 
                           WHEN amount > 0 AND (DESCRIPTION NOT LIKE '%GOLDMAN SACHS%' 
                           and DESCRIPTION NOT LIKE '%PAYMENT%' and DESCRIPTION NOT LIKE '%TRANSFER%') THEN 'INCOME' 
                          WHEN bank_category = 'Groceries' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%TICKPICK%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%SBUFEESDEPOSIT%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%STUDENT LN%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%SOHO NAILS%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%RAINBOW CLEANERS%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%COFFEE%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%RITE AID%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%CHINA%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%GOLDMAN SACHS%' THEN 'TRANSFER'
                            WHEN DESCRIPTION LIKE '%PAYMENT%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%VENTURE%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%STUDIOS%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%BANANARE%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%HONDA%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%YAD ELIEZER%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%LOIS E. SHULM%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%TARGET%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%TOMCHEI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%LUKOIL%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%SUNOCO%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%DUNKIN%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%STARBUCKS%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%SHEETZ%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%STOP & SHOP%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%INSTACART%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%SUNOCO%' THEN 'GAS'
                            WHEN DESCRIPTION LIKE '%PSEG%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%PIZZA%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%SUSHI%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%VERIZON%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%OPTIMUM%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%DEPT EDUCATION%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%ONLINE PMT%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%Amazon%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%CHASE CREDIT%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%Trnsfr%' THEN 'TRANSFER'
                            WHEN DESCRIPTION LIKE '%BAHAMAR%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%ANNUAL MEMBERSHIP FEE%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%ACME%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%WOODBRIDGE%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%BAHA BAY%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%CIBO%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%BERMAN BOOKS%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%HOME DEPOT%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%PEDIATRIC%' THEN 'MEDICAL AND GYM' 
                            WHEN DESCRIPTION LIKE '%MARKET%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%FIT2RUN%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%H&%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%FUEL%' THEN 'CAR' 
                            WHEN DESCRIPTION LIKE '%BABY%' THEN 'GROCERIES AND BABY' 
                            WHEN DESCRIPTION LIKE '%NORDRACK%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%BASKETEERS%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%BP%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%SHELL OIL%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%GROCERY%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%MILKY WAY%' THEN 'RESTAURANT/COFFEE' 
                            WHEN DESCRIPTION LIKE '%MALL%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%CONCESSIONS%' THEN 'WANTS' 
                            WHEN DESCRIPTION LIKE '%BREWERY%' THEN 'WANTS' 
                            WHEN DESCRIPTION LIKE '%WIFI%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%HUDSON ST%' THEN 'TRAVEL' 
                            WHEN DESCRIPTION LIKE '%CANONICA%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%OXYFRESH%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%FLORA%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%Bill Pay%' THEN 'BILL PAYMENT'
                            WHEN DESCRIPTION LIKE '%Check%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%transfer%' THEN 'TRANSFER'
                            WHEN DESCRIPTION LIKE '%BLUESTONE%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%RWJ NEW%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%SPOTIFY%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%APPLE%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%ATM%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%Marcus Invest%' THEN 'TRANSFER'
                        
                            WHEN DESCRIPTION LIKE '%MTA%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%WELLS FARGO%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%AMZN%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%BAKERY%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%WHOLEFDS%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%TRANSACTION FEE%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%MINI GOLF%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%NINTENDO%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%BRIDGE TURKISH%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%LYFT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%UBER%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%BED BATH%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%CAFE%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%RACEWAY%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%CVS%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%DERECHETZCH%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%STAUF%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%UNITED%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%CAR%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%FEDEX%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%DERECH%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%KOLLEL%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%WITHDRWL%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%NJT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%DOLLAR-A-DAY%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%CITRON%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%GREAT CLIPS%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%PENSTOCK%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%HOTEL%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%RESTAURANT%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PAYPAL%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%WALGREENS%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%NAILS%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%GIVING%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%AIRBNB%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%STOP &%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%BUDGET.COM%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%SPIRIT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%CLEANERS%' THEN 'HOUSE BILLS'
                            WHEN DESCRIPTION LIKE '%DUANE READE%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%GET AIR%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%VENDING%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%CONG.%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%7-ELEVEN%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%GLATT 27%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%BEXLEY MKT%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%TRADER JOE%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%HEADWAY%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%EXXON%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%CHAI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BUY BUY%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%HYATT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%KOSHER%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%HAVA JAVA%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%KOHL%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%SHOPRITE%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%OLD NAVY%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%AMC%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%SPEEDWAY%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%COFF%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PHARMA%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%CABOKI%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%RITA%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%ROASTERY%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%KROGER%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%FINES AND COSTS%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%MUSIC%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%KITTIE%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%CHASDEI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BESTBUY%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%CHICKIES%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%SPA%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%PRIME%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                            WHEN DESCRIPTION LIKE '%ROCKNROLL%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PINOT%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%WALMART%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%BOUTIQUE%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%ZAGE%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%SKI%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%QUICK CHEK%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%THEATRE%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%DONATI%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%FIRESIDE%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PARKING%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%YOLANDA%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%TJMA%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%KISSENA%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%PARTY%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%STAPLES%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%YERUSHALAYIM%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%LAWRENCE%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%LOFT%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%ANTHRO%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%GIVING%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%Travel%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%VICTORIA%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%PARKING%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%TAXI%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%TAVLIN%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%GIVING%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%SCHNITZ%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PARK P%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%DELI KAS%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%MICHAELS%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%CIRCLE K%' THEN 'CAR'
                            WHEN DESCRIPTION LIKE '%HUMBLE TOAST%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%ANN TAYLOR%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%BEDBATH%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%SAMMY%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%ZAGAFEN%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%COLOR ME%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%BETH JACOB%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%PARK DELI%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%RESTAU%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%NCSY%' THEN 'CHARITY'
                        
                            WHEN DESCRIPTION LIKE '%ZENNI%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%RWJ%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%Duane%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                       
                            WHEN DESCRIPTION LIKE '%MUNICI%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%MEOROT%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BBQ%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%BAKERIST%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%EXPEDIA%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%656 OCEAN%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%HILTON%' THEN 'TRAVEL'
                       
                            WHEN DESCRIPTION LIKE '%ROBINHOOD%' THEN 'TRANSFER'
                            WHEN DESCRIPTION LIKE '%VENMO%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%NATIONWIDE%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%GRAETERS%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%WAL-MART%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%EDEN%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PARK DENTAL%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%PARKCOLUMBUS%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%CRIMSON%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%BARNES%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%Theater of%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%CLOTHING%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%JUDAICA%' THEN 'GENERAL'
                            WHEN DESCRIPTION LIKE '%ROAD RUNNER%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%KITCHEN%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%PIZZ%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%WEGMAN%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%AIRPORT%' THEN 'TRAVEL'
                            WHEN DESCRIPTION LIKE '%BLOOMINGDAL%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%FEE%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%NJMVC%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%FUNDRA%' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%CULVV%' THEN 'MISC'
                            WHEN DESCRIPTION LIKE '%WOMENS%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%GRILL%' THEN 'RESTAURANT/COFFEE'
                            WHEN DESCRIPTION LIKE '%MACY%' THEN 'WANTS'
                            WHEN DESCRIPTION LIKE '%YU.EDU' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%BRAVO%' THEN 'GROCERIES AND BABY'
                            WHEN DESCRIPTION LIKE '%BRACHA' THEN 'CHARITY'
                            WHEN DESCRIPTION LIKE '%SURGERY%' THEN 'MEDICAL AND GYM'
                            WHEN DESCRIPTION LIKE '%VEORIDE%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%PILOT%' THEN 'CAR'
                         WHEN DESCRIPTION LIKE '%NORDSTROM%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%KEEPS%' THEN 'MEDICAL AND GYM'
                         WHEN DESCRIPTION LIKE '%MEDICAL%' THEN 'MEDICAL AND GYM'
                         WHEN DESCRIPTION LIKE '%SAM ASH%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%SCARF%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%STUDIOS' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%JJS%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%BANANARE' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%BAGEL%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%BRUCE SPRINGSTEEN%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%MENUCHA%' THEN 'CHARITY'
                         WHEN DESCRIPTION LIKE '%KOSH%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%TOPGOLF%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%BARBER SHOP%' THEN 'MEDICAL AND GYM'
                         WHEN DESCRIPTION LIKE '%VANGUARD%' THEN 'MISC'
                         WHEN DESCRIPTION LIKE '%DOLLAR CITY%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%IPA MAN%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%CAFFE YAHUDA HALEVI%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%GIFTS%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%AROMA%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%ZOL GADOL%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%TMOL%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%GAN SIPOR SAKER%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%BEGAL CAFE REHAVIA%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%COFFE%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%KAFE RIMON%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%SUPER PHARM%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%SHORASHIM%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%GRAFUS BEAM%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%AMI%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%L LEVY NADLAN%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%HOTEL%' THEN 'TRAVEL'
                         WHEN DESCRIPTION LIKE '%INFUSED JLM%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%HEADWAY%' THEN 'MEDICAL AND GYM'
                         
                         WHEN DESCRIPTION LIKE '%FACEBK%' THEN 'MISC'
                         WHEN DESCRIPTION LIKE '%PURCHASE NEW BRUNSWICK NJ%' THEN 'RESTAURANT/COFFEE'
                         WHEN DESCRIPTION LIKE '%ZARA USA%' THEN 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)'
                         WHEN DESCRIPTION LIKE '%TOP GOLF%' THEN 'WANTS'
                         WHEN DESCRIPTION LIKE '%AMERICAN FRIENDS OF%' THEN 'CHARITY'
                         
                         WHEN DESCRIPTION LIKE '%CHAILI%' THEN 'CHARITY'
                         WHEN DESCRIPTION LIKE '%TORAHT%' THEN 'CHARITY'
                         WHEN DESCRIPTION LIKE '%GIVING%' THEN 'CHARITY'
                         WHEN DESCRIPTION LIKE '%MR. C%' THEN 'TRAVEL'
                         WHEN DESCRIPTION LIKE '%AIRPOR%' THEN 'TRAVEL'
                         
                         
                         
                         
                         
                         
                            ELSE 'UNCLASSIFIED'
                        END AS category
                    FROM 
                        df) select *, 
                        
                           CASE WHEN category = 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)' THEN 'RANDOM STUFF WE NEEDED TO BUY'
                             WHEN category = 'GENERAL' THEN 'RANDOM STUFF WE NEEDED TO BUY'
                             WHEN category = 'RESTAURANT/COFFEE' THEN 'FOOD WE COULD HAVE MADE AT HOME'
                             WHEN category = 'WANTS' THEN 'WANTS'
                             WHEN category = 'MEDICAL AND GYM' THEN 'NEEDS'
                             WHEN category = 'HOUSE BILLS' THEN 'NEEDS'
                             WHEN category = 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)' THEN 'RANDOM STUFF WE NEEDED TO BUY'
                             WHEN CATEGORY = 'MISC' THEN 'RANDOM STUFF WE NEEDED TO BUY'
                             WHEN category = 'HOUSE BILLS' THEN 'RANDOM STUFF WE NEEDED TO BUY'
                             when category = 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)' THEN 'RANDOM STUFF WE NEEDED TO BUY'
                             WHEN category = 'RESTAURANT/COFFEE' THEN 'FOOD WE COULD HAVE MADE AT HOME'
                             WHEN category = 'CAR' THEN 'NEEDS'
                             WHEN category = 'GROCERIES AND BABY' THEN 'NEEDS'
                             WHEN category = 'TRAVEL' THEN 'TRAVEL'
                             ELSE category
                         END as general_categories,
                        
                        CASE WHEN category = 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)' THEN 'NEEDS'
                             WHEN category = 'GENERAL' THEN 'NEEDS'
                             WHEN category = 'RESTAURANT/COFFEE' THEN 'WANTS'
                             WHEN category = 'WANTS' THEN 'WANTS'
                             WHEN category = 'MEDICAL AND GYM' THEN 'NEEDS'
                             WHEN category = 'HOUSE BILLS' THEN 'NEEDS'
                             WHEN category = 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)' THEN 'NEEDS'
                             WHEN CATEGORY = 'MISC' THEN 'NEEDS'
                             WHEN category = 'HOUSE BILLS' THEN 'NEEDS'
                             when category = 'GENERAL NEEDS (TARGET, AMAZON, RITE AID, ETC)' THEN 'NEEDS'
                             WHEN category = 'RESTAURANT/COFFEE' THEN 'WANTS'
                             WHEN category = 'CAR' THEN 'NEEDS'
                             WHEN category = 'GROCERIES AND BABY' THEN 'NEEDS'
                             WHEN category = 'TRAVEL' THEN 'TRAVEL'
                             ELSE category
                         END as needs_vs_wants
                         
                         FROM precise_categories 
                        """

df = banksDf()
data = ps.sqldf(transform_query, locals())
dfToSheets(data)
