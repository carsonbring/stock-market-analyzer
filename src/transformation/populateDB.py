# populateDB.py - Populates the SQL database
# Carson Bring

import pickle
import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import matplotlib.pyplot as plt
import yfinance as yf
import pymssql

#getting the connnection string for my db
password = dbutils.secrets.get(scope="ScopeDB", key="password")

#setting up the database connection with pymssql
conn = pymssql.connect('serversma.database.windows.net', 'carson.bring', password, 'databasesma')

#open and read in a list of tickers that was saved by Tickers.py
with open('../ingestion/tickers.pkl', 'rb') as f:
    tickers = pickle.load(f) 
    f.close()


#using this to store tickers without any data present from news and financial reports
skipped_tickers = []
#setting spark config settings to get acces to newsandsentiments blob storage
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
uri = "abfss://newsandsentiments@storagesma.dfs.core.windows.net/"


# Storing average sentiment scores inside of a dictionary that has the ticker and average sentiment score
# ------------------------------------------

average_sentiment_scores = {}
for ticker in tickers:
    try:
        df = spark.read.parquet(uri + f'{ticker}/part*')
        #casting as a float so I can sum
        df = df.withColumn('overall_sentiment_score_float', col('overall_sentiment_score').cast("float"))
        #filtering out null values
        df = df.filter(col('overall_sentiment_score_float').isNotNull())
        #summing the column and storing the result
        sum_result = df.agg({'overall_sentiment_score_float': 'sum'}).collect()[0]["sum(overall_sentiment_score_float)"]
        #storing in dictionary to insert into DB later (the reason why dividing by 10 is because there are 10 articles with their own scores in each parquet)
        average_sentiment_scores[ticker] = sum_result/10

    except Exception as e:
        #storing skipped tickers for data cleanup later
        print(f'Skipping news analysis for {ticker}')
        skipped_tickers.append(ticker)
        print(e)

#switching config to financial reports in order to collect the tickers without any financial reports given before inserting anything into DB
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
uri = "abfss://quarterlyfinancialreports@storagesma.dfs.core.windows.net/"
for ticker in tickers:
    try:
        df = spark.read.parquet(uri + f'{ticker}/part*')
        
    except Exception as e:
        print(f"skipping report for {ticker}")
        skipped_tickers.append(ticker)
        print(e)


#list to store the ticker information as tuple for insert
ticker_data = []

# Iterate over tickers and fetch company name using yfinance
for ticker in tickers:
    if ticker not in skipped_tickers:
        try:
            #ENRICHING database information using yahoo finance comapny name for the ticker
            stock_info = yf.Ticker(ticker)
            company_name = stock_info.info.get("longName", "N/A")
            #fetching sentiment score from the dictionary
            sentiment_score = average_sentiment_scores.get(ticker, 0)

            ticker_data.append((ticker, company_name, sentiment_score))
        
        except Exception as e:
            print(f"Error fetching info for {ticker}: {e}")

#inserting data into the DIM_Ticker table
try:
    cursor = conn.cursor()
    #iterating over ticker_data and inserting into the table
    for ticker, company_name, sentiment_score in ticker_data:
        query = "INSERT INTO [dbo].[DIM_Ticker] (Ticker, CompanyName, Average_Sentiment) VALUES (%s, %s, %s)"
        values = (ticker, company_name, sentiment_score)
        cursor.execute(query, values)

    
    #committing the changes
    conn.commit()
    
    print("Data for DIM_Ticker inserted successfully.")

except pymssql.Error as e:
    print("Error executing query:", e)

finally:
    cursor.close()

#switching back over to quarterly financial reports
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))
uri = "abfss://quarterlyfinancialreports@storagesma.dfs.core.windows.net/"

#storing tickers with missing data (Total Expenses missing in a few reports) to delete DIM_Tickers of these tickers
tickers_with_missing_data = []
#inserting data into the DIM_Report and FACT_Analysis tables
try:
    #iterating over tickers and fetching financial report information
    for ticker in tickers:
        if ticker not in skipped_tickers:
            try:
                cursor = conn.cursor()
                df = spark.read.parquet(uri + f'{ticker}/part*')
                #iterating over rows in the DataFrame and insert into DIM_Report and FACT_Analysis
                for row in df.collect():
                    
                    #inserting into DIM_Report
                    query_dim_report = "INSERT INTO [dbo].[DIM_Report] (Ticker, ReportDate, TotalRevenue, NetIncome, TotalExpenses, GrossProfit, EPS) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    values_dim_report = (
                        ticker,
                        row['index'],
                        row['Total Revenue'],
                        row['Net Income'],
                        row['Total Expenses'],
                        row['Total Revenue'] - row['Total Expenses'],
                        row['Basic EPS']
                    )
                    cursor.execute(query_dim_report, values_dim_report)

                    #inserting into FACT_Analysis
                    query_fact_analysis = "INSERT INTO [dbo].[FACT_Analysis] (Ticker, ReportDate) VALUES (%s, %s)"
                    values_fact_analysis = (
                        ticker,
                        row['index']
                    )
                    cursor.execute(query_fact_analysis, values_fact_analysis)

            except Exception as e:
                print(f"Error inserting data for {ticker}: {e}")
                tickers_with_missing_data.append(ticker)


    #committing the changes
    conn.commit()

    print("Data inserted into DIM_Report and FACT_Analysis successfully.")

except pymssql.Error as e:
    print("Error executing query:", e)

finally:
    cursor.close()

try:
    cursor = conn.cursor()
    #deleting records in DIM_Ticker based on tickers_with_missing_data
    for ticker in tickers_with_missing_data:
        delete_query = f"DELETE FROM [dbo].[DIM_Ticker] WHERE Ticker = '{ticker}'"
        cursor.execute(delete_query)

    #committing the deletion changes
    conn.commit()

    print("DIM_Ticker records deleted successfully.")

except pymssql.Error as e:
    print("Error executing query:", e)

finally:
    cursor.close()
    conn.close()

tickers_not_included = set(skipped_tickers + tickers_with_missing_data)
#printing tickers that weren't included
print("Tickers that were not included: ")
print(tickers_not_included)