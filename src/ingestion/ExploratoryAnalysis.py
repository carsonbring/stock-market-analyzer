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
#EDA : Yahoo Finance - Financial Reports
max_gross_profit = 0
max_gross_profit_ticker = ""
max_net_income = 0
max_net_income_ticker = ""

for ticker in tickers:
    try:
        df = spark.read.parquet(uri + f'{ticker}/part*')
        #for analysis, I will be using the most recent quarterly report (row 1)
        gross_profit = df.select("Total Revenue").first()["Total Revenue"]
        net_income = df.select("Net Income").first()["Net Income"]
        if int(gross_profit) > max_gross_profit:
            max_gross_profit = gross_profit
            max_gross_profit_ticker = ticker
        if int(net_income) > max_net_income:
            max_net_income = gross_profit
            max_net_income_ticker = ticker
    except:
        print(f"skipping report for {ticker}")

#WMT (Walmart)
print(f'Company with the highest revenue last quarterly report is {max_gross_profit_ticker} with {max_gross_profit} ')

print(f'Company with the highest net income in the last quarterly report is {max_net_income} with {max_net_income_ticker}')

#EDA : Alpha Vantage - News and Sentiment

spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
uri = "abfss://newsandsentiments@storagesma.dfs.core.windows.net/"



highest_overall_sentiment_score = 0
highest_overall_sentiment_score_ticker = ""

for ticker in tickers:
    try:
        df = spark.read.parquet(uri + f'{ticker}/part*')
        #casting as a float so I can sum
        df = df.withColumn('overall_sentiment_score_float', col('overall_sentiment_score').cast("float"))
        #filtering out null values
        df = df.filter(col('overall_sentiment_score_float').isNotNull())
        #summing the column and storing the result
        sum_result = df.agg({'overall_sentiment_score_float': 'sum'}).collect()[0]["sum(overall_sentiment_score_float)"]
        #setting the max if the sum_result is higher than the highest_overall_sentiment_score
        if sum_result > highest_overall_sentiment_score:
            highest_overall_sentiment_score = sum_result
            highest_overall_sentiment_score_ticker = ticker
    except:
        print(f'Skipping news analysis for {ticker}')
#HAS
print(f'The company with the highest overall sentiment score is {highest_overall_sentiment_score_ticker} with an average score across 10 articles being {highest_overall_sentiment_score/10}')


