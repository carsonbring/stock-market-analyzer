from pyspark.sql import SparkSession
import pandas as pd
import pickle
import yfinance as yf
# Create a Spark session
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
uri = "abfss://quarterlyfinancialreports@storagesma.dfs.core.windows.net/"

spark = SparkSession.builder.appName("FinancialReports").getOrCreate()

# loading tickers from pickle file
with open('tickers.pkl', 'rb') as f:
    tickers = pickle.load(f) 
    f.close()

#downloading the financial data using yfinance
no_reports = []
for ticker in tickers:
    try:
        financials = yf.Ticker(ticker).quarterly_financials
        #creating pandas dataframe that transposes the dataframe to make the financial indicators the column names and then resets the index to the default (datetime of financial report)
        financials_df = spark.createDataFrame(pd.DataFrame(financials).T.reset_index())
        financials_df.repartition(1).write.mode('overwrite').parquet(uri+f"{ticker}")
    except:
        #if there is an issue, we append to the no reports list 
        no_reports.append(ticker)
        print(f"couldnt obtain report for {ticker}")

print("list of failed report retrieval")
#['BRK.B', 'BF.B', 'VLTO']
print(no_reports)
 
    