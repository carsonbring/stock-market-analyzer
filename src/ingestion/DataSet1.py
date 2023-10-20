import requests
import pandas as pd
import pickle
import datetime 

from sec_api import XbrlApi, QueryApi
with open('edgar.config') as f:
    api_key = f.readline()
with open('tickers.pkl', 'rb') as f:
    tickers = pickle.load(f) 
    f.close()
#first getting the list of tickers 
xbrlApi = XbrlApi(api_key)

# Get the S&P 500 tickers

def get_xbrl_url(ticker):
    
    queryApi = QueryApi(api_key=api_key)
    
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
    
    # Access the 'linkToHtml' value
    link_to_html = most_recent_filing.get('linkToHtml')
    return link_to_html    

def get_income_statement(xbrl_json):
    income_statement_store = {}
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
    #switching columns and rows so that US GAAP items are rows and each column header represents a date range
    return income_statement.T


print(tickers)
for ticker in tickers:
    url_10k = get_xbrl_url(ticker)
    print(url_10k)
    xbrl_json = xbrlApi.xbrl_to_json(htm_url=url_10k)

    income_statement = get_income_statement(xbrl_json)

    print(ticker)
    print('--------------')
    print(income_statement)
