# DataSet2.py - script to extract data from its source and load into ADLS.
import requests
import pickle
import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
#setting my config settings for spark
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))

uri = "abfss://newsandsentiments@storagesma.dfs.core.windows.net/"


spark = SparkSession.builder.appName("NewsAndSentiments").getOrCreate()

import pickle
import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
#setting my config settings for spark
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))

uri = "abfss://newsandsentiments@storagesma.dfs.core.windows.net/"
with open('av.config') as f:
    api_key = f.readline()
    f.close()
#open and read in a list of tickers that was saved by Tickers.py
with open('tickers.pkl', 'rb') as f:
    tickers = pickle.load(f) 
    f.close()
#creating a list for tickers that don't return news 
#['GOOGL', 'AJG', 'BRK.B', 'BF.B', 'CE', 'CHD', 'CMCSA', 'EQR', 'EL', 'FOXA', 'HBAN', 'LOW', 'MMC', 'MKC', 'MRK', 'MPWR', 'NDAQ', 'NWSA', 'NWS', 'TROW', 'USB']
tickers_without_news = []
counter = 0
#iterating through each of the tickers and pausing every 30 api calls and waiting a minute 
#this is done because the api key that i purchased only allows for 30 api calls a minute
for ticker in tickers:
    counter = counter + 1
    if(counter == 1):
        print("SLEEPING")
        time.sleep(60)
    if(counter %30 == 0):
        print("SLEEPING")
        time.sleep(60)
    try:
        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&limit=10&apikey={api_key}'
        print(ticker)
        print('--------------')
        r = requests.get(url)
        data = r.json()
        feed_data = data.get('feed', [])
        df = spark.createDataFrame(feed_data)

        # Select the first 10 articles with associated data.
        df = df.limit(10)
        # saving in a parquet file format since there are nested structures inside of columns and Csv cant handle that.
        df.repartition(1).write.mode('overwrite').parquet(uri+f"{ticker}")
    except Exception as e:
        tickers_without_news.append(ticker)
        print(e)

#Displays the tickers without news
print("tickers without news")
print(tickers_without_news)

