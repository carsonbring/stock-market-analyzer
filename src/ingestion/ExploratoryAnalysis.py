# ExploratoryAnalysis.py - script to do EDA (exploratory data analysis) on the two primary data sets for the project. 

# Note - alternative approaches can be used besides local Python code.  Large datasets may require Databricks.  
# You can also use ChatGPT or similar language model for this step. 
import pickle
import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import matplotlib.pyplot as plt


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

# Pandas Profiling
# ------------------------------

#grabbing an example of a financial report - AAPL
df = spark.read.parquet(uri + f'CB/part*')
print(df.describe())
display(df)

# # Doing some operations on the data just for practice
# # ------------------------------------------
# max_gross_profit = 0
# max_gross_profit_ticker = ""
# max_net_income = 0
# max_net_income_ticker = ""
# top_gross_profit_tickers = {}
# top_net_income_tickers = {}
# for ticker in tickers:
#     try:
#         df = spark.read.parquet(uri + f'{ticker}/part*')
#         #for analysis, I will be using the most recent quarterly report (row 1)
#         gross_profit = df.select("Total Revenue").first()["Total Revenue"]
#         net_income = df.select("Net Income").first()["Net Income"]
#         #putting ticker and values inside of dictionaries to sort for results
#         top_gross_profit_tickers[ticker] = int(gross_profit)
#         top_net_income_tickers[ticker] = int(net_income)
#     except Exception as e:
#         print(f"skipping report for {ticker}")
#         print(e)

# #creating bar chart for top 5 gross profit companies
# sorted_gross_profit = sorted(top_gross_profit_tickers.items(), key=lambda x: x[1], reverse=True)[:5]
# plt.bar([item[0] for item in sorted_gross_profit], [item[1] for item in sorted_gross_profit])
# plt.title('Top 5 Companies with the highest revenue')
# plt.xlabel('Company Ticker')
# plt.ylabel('Total Revenue')
# plt.show()

# #creating bar chart for top 5 net income companies
# sorted_net_income = sorted(top_net_income_tickers.items(), key=lambda x: x[1], reverse=True)[:5]
# plt.bar([item[0] for item in sorted_net_income], [item[1] for item in sorted_net_income])
# plt.title('Top 5 Companies with the highest net income')
# plt.xlabel('Company Ticker')
# plt.ylabel('Net Income')
# plt.show()

# #EDA : Alpha Vantage - News and Sentiment

# spark.conf.set(
#     "fs.azure.account.key.storagesma.dfs.core.windows.net",
#     dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
# uri = "abfss://newsandsentiments@storagesma.dfs.core.windows.net/"

# # Pandas Profiling
# # ------------------------------


# df = spark.read.parquet(uri + f'AAPL/part*')

# display(df)




# # Doing some operations on the data just for practice
# # ------------------------------------------
# highest_overall_sentiment_score = 0
# highest_overall_sentiment_score_ticker = ""
# top_sentiment_score_tickers = {}
# for ticker in tickers:
#     try:
#         df = spark.read.parquet(uri + f'{ticker}/part*')
#         #casting as a float so I can sum
#         df = df.withColumn('overall_sentiment_score_float', col('overall_sentiment_score').cast("float"))
#         #filtering out null values
#         df = df.filter(col('overall_sentiment_score_float').isNotNull())
#         #summing the column and storing the result
#         sum_result = df.agg({'overall_sentiment_score_float': 'sum'}).collect()[0]["sum(overall_sentiment_score_float)"]
#         #storing in dictionary to sort later
#         top_sentiment_score_tickers[ticker] = sum_result

#     except Exception as e:
#         print(f'Skipping news analysis for {ticker}')
#         print(e)

# #printing top 5 sentiment score tickers
# print("\nTop 5 Companies with the highest overall sentiment score:")
# for ticker, value in sorted(top_sentiment_score_tickers.items(), key=lambda x: x[1], reverse=True)[:5]:
#     print(f"{ticker}: {value/10}")
# sorted_sentiment_scores = sorted(top_sentiment_score_tickers.items(), key=lambda x: x[1], reverse=True)[:5]
# plt.bar([item[0] for item in sorted_sentiment_scores], [item[1] for item in sorted_sentiment_scores])
# plt.title('Top 5 Companies with the sentiment scores')
# plt.xlabel('Company Ticker')
# plt.ylabel('Sentiment Score')
# plt.show()

