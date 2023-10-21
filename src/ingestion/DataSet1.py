import requests
import pandas as pd
import pickle
import datetime 
from pyspark.sql import SparkSession
from sec_api import XbrlApi, QueryApi
#open the config file where the api_key is for sec edgar api
with open('edgar.config') as f:
    api_key = f.readline()
#open and read in a list of tickers that was saved by Tickers.py
with open('tickers.pkl', 'rb') as f:
    tickers = pickle.load(f) 
    f.close()
#setting my config settings for spark
spark.conf.set(
    "fs.azure.account.key.storagesma.dfs.core.windows.net",
    dbutils.secrets.get(scope="Databricks-scope", key="databricks-key"))

uri = "abfss://financialreports@storagesma.dfs.core.windows.net/"


#initializing xbrlApi with the API key 
xbrlApi = XbrlApi(api_key)


#function that obtains the url for the most recent 10-Q financial report for a ticker that is passed in as an argument
def get_xbrl_url(ticker):
    try:
        queryApi = QueryApi(api_key=api_key)
        #creating the query
        query = {
        "query": { "query_string": {
            "query": f"ticker:{ticker} AND formType:\"10-Q\""
            } },
        "from": "0",
        "size": "1",
        "sort": [{ "filedAt": { "order": "desc" } }]
        }

        filings = queryApi.get_filings(query)
        most_recent_filing = filings['filings'][0]
    
        # the link to the html that I can pass into an xbrl function to get the json to parse in another funciton
        link_to_html = most_recent_filing.get('linkToHtml')
    except:
        link_to_html = None
    return link_to_html    
# parses the xbrl json response to create a dataframe in pandas and returns that dataframe
def get_income_statement(xbrl_json):
    income_statement_store = {}
    try:
        for usGaapItem in xbrl_json['StatementsOfIncome']:
            values = []
            indicies = []
            for fact in xbrl_json['StatementsOfIncome'][usGaapItem]:
                if 'segment' not in fact:
                    index = fact['period']['startDate'] + '-' + fact['period']['endDate']
                    # ensuring no index dupes are made
                    if index not in indicies:
                        values.append(fact['value'])
                        indicies.append(index)
            income_statement_store[usGaapItem] = pd.Series(values,index=indicies)
        income_statement = pd.DataFrame(income_statement_store)
        return income_statement.T
    except:
        return pd.DataFrame()
    
    
    

#prints out the ticker list just to make sure that the pickle file has the right info saved
print(tickers)
#initializing a SparkSession app that transforms the pandas dataframe into an apache spark dataframe
spark = SparkSession.builder.appName("pandas_to_spark").getOrCreate()

#iterating through each of the tickers in the list
for ticker in tickers:
    # getting the xbrl url using the function above
    url_10k = get_xbrl_url(ticker)
    if not url_10k:
        print(f"Couldn't obtain xbrl URL for {ticker}")
        continue
    #obtaining the json from xbrl
    xbrl_json = xbrlApi.xbrl_to_json(htm_url=url_10k)
    
    #passing json into my get_income_statement function to receive a pandas dataframe
    income_statement = get_income_statement(xbrl_json)
    print(ticker)
    if not income_statement.empty:
        # using pandas dataframe to make a spark dataframe
        spark_income_statement = spark.createDataFrame(income_statement)
        print('--------------')
        #writing the dataframe to a csv in a folder with the respective ticker name
        spark_income_statement.repartition(1).write.mode('overwrite').csv(uri+f"{ticker}")
    else:
        print('--------------')
        print(f"ERROR for {ticker}")

    
