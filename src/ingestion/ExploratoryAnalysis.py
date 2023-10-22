# ExploratoryAnalysis.py - script to do EDA (exploratory data analysis) on the two primary data sets for the project. 

# Note - alternative approaches can be used besides local Python code.  Large datasets may require Databricks.  
# You can also use ChatGPT or similar language model for this step. 
import pickle
import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
#setting my config settings for spark
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
uri = "abfss://quarterlyfinancialreports@storagesma.dfs.core.windows.net/"

#open and read in a list of tickers that was saved by Tickers.py
with open('tickers.pkl', 'rb') as f:
    tickers = pickle.load(f) 
    f.close()
#EDA : SEC - EDGAR Database
max_gross_profit = 0
max_gross_profit_ticker = ""
df = spark.read.parquet(uri + f'ABBV/part*')
display(df)
# for ticker in tickers:
#     # try:
#     df = spark.read.parquet(uri + f'{ticker}/part*')
#         #for analysis, I will be using the most recent quarterly report (row 1)
#     print(ticker)
#     for col in df.columns:
#         print(col)
#     print('-------------------')

#     #     if int(gross_profit) > max_gross_profit:
#     #         max_gross_profit = gross_profit
#     #         max_gross_profit_ticker = ticker    
#     #     #for analysis, I will be using the most recent quarterly report (row 1)
#     #     gross_profit = df.select("Revenues").first()["Revenues"]
#     #     if int(gross_profit) > max_gross_profit:
#     #         max_gross_profit = gross_profit
#     #         max_gross_profit_ticker = ticker
#     # except:
#     #     print(f"skipping {ticker}")

# print(max_gross_profit)
# print(max_gross_profit_ticker)
#EDA : Alpha Vantage - News and Sentiment