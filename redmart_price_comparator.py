##########################################################
# Redmart Price Comparator
# Holmusk Challenge
# 2015.1.24
# Bhargav.V
# Version 5
##########################################################

import json
import urllib.request as ur
from bs4 import BeautifulSoup
import datetime

today_time = datetime.datetime.today()

#---------------------------------------Scrape Product info from internal API---------------------------------------

#url links to product info of all redmart products
url = 'https://ssapi.redmart.com/v1.5.6/catalog/search?q=*&pageSize=4000&sort=1&session=abcd&apiConsumerId=1234'
urlRes = ur.urlopen(url)
soup = BeautifulSoup(urlRes, "html.parser")
soupChr = str(soup)

#---------------------------------------JSON File To be Parsed---------------------------------------

#Parsing the JSON file
data = json.loads(soupChr)
chk = data['products']
dataForOneSC = []
#Looping through all the products
for i in range(len(chk)):
    oneRow = []
    temp = chk[i]
    #Appending required information for each product
    oneRow.append(temp['title'])
    oneRow.append(temp['sku'])
    oneRow.append(', '.join(temp['category_tags']) + ',')
    #Certain products do not have brand information
    try:
        oneRow.append(temp['filters']['brand_name'])
    except KeyError:
        oneRow.append('Unknown')
        continue
    oneRow.append(temp['filters']['mfr_name'])
    oneRow.append(temp['filters']['vendor_name'])
    #Certain products do not have country information
    try:
        oneRow.append(temp['filters']['country_of_origin'])
    except KeyError:
        oneRow.append('Unknown')
        continue
    oneRow.append(temp['pricing']['price'])
    #Saving the link of the image in our database
    oneRow.append("http://s3-ap-southeast-1.amazonaws.com/media.redmart.com/newmedia/150x"+temp['img']['name'])
    oneRow.append(today_time)
    dataForOneSC.append(oneRow)

#---------------------------------------Store Product Data in local database---------------------------------------

#Converting to a dataframe
#Please change db_path to local path used
import pandas as pd
df = pd.DataFrame(dataForOneSC, columns=['Title','SKU','Categories','brand','mfr','vendor','ctry_origin','Price','ImageLink', 'Run_date'])

#Storing the data in a database
import sqlite3
#Create a database locally
#Change path before execution
db_path = "/Users/bhargav/Desktop/Holmusk/redMart_all_products_data.db"
conn = sqlite3.connect(db_path)
c = conn.cursor()
df.to_sql('LATEST_DATA', conn, if_exists='replace', index=True)
conn.commit()

#---------------------------------------Data loaded into local database---------------------------------------

#Creating a table which holds historical data for all the weeks
c.execute('''
   CREATE TABLE IF NOT EXISTS RM_WEEKLY_DATA(ROW_NUM INTEGER, TITLE TEXT, SKU TEXT,
                      CATEGORIES TEXT, BRAND TEXT, MFR TEXT , VENDOR TEXT,
                      CTRY_ORIGIN TEXT, PRICE REAL , IMAGELINK TEXT, RUN_DATE TIMESTAMP)
          ''')
conn.commit()

c.execute('''
    INSERT INTO RM_WEEKLY_DATA SELECT * FROM LATEST_DATA
          ''')
conn.commit()

#---------------------------------------Manipulate data to compare weekly prices---------------------------------------

#Ranks the weeks in descending order based on run_date
c.execute('''
    DROP TABLE IF EXISTS RANKED_TABLE
          ''')
conn.commit()

c.execute('''
    CREATE TABLE RANKED_TABLE AS
    SELECT A.*,B.RANK
    FROM RM_WEEKLY_DATA A
    LEFT JOIN
    (
    SELECT A.ROW_NUM,A.RUN_DATE,
    COUNT(DISTINCT B.RUN_DATE) RANK
    FROM RM_WEEKLY_DATA A, RM_WEEKLY_DATA B
    WHERE A.RUN_DATE < B.RUN_DATE
      OR
      (A.RUN_DATE=B.RUN_DATE AND A.ROW_NUM = B.ROW_NUM)
    GROUP BY  A.ROW_NUM, A.RUN_DATE
    ORDER BY A.RUN_DATE DESC, RANK DESC
	)B
	ON A.ROW_NUM = B.ROW_NUM AND
	   A.RUN_DATE = B.RUN_DATE
	   ''')
conn.commit()

#Compares the 2 latest weeks and compares product price
c.execute('''
    DROP TABLE IF EXISTS PRICE_COMPARATOR
          ''')
conn.commit()

c.execute('''
	CREATE TABLE PRICE_COMPARATOR AS
	SELECT A.TITLE AS TITLE, A.SKU AS SKU, A.CATEGORIES, A.PRICE AS LATEST_PRICE, B.PRICE AS OLD_PRICE,
	A.IMAGELINK AS IMAGE,
	CASE WHEN A.PRICE != B.PRICE THEN 1 ELSE 0 END AS PRICE_CHANGE_FLAG
	FROM RANKED_TABLE A, RANKED_TABLE B
	ON A.SKU = B.SKU
	WHERE A.RANK = 1 AND B.RANK = 2
	''')
conn.commit()

#---------------------------------------Store price comparisons in a dataframe---------------------------------------

#Creating a dataframe containing all the latest price changes
subdata = pd.read_sql_query("SELECT * FROM PRICE_COMPARATOR", conn)
conn.commit()
conn.close()

#---------------------------------------Create a CSV with price comparison in local directory---------------------------------------

#Creates a csv output with price comparison information
out_path = "/Users/bhargav/Desktop/Holmusk/Price_Comparator_Output.csv"
subdata.to_csv(out_path, encoding='utf-8')

#---------------------------------------------------------------------------------------------------------------------
