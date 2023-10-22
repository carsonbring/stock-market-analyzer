[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12453677&assignment_repo_type=AssignmentRepo)
# CSCI 422 Project - Stock Market Analyzer - Carson Bring
This project's goal is to provide a free to access website that has important financial, news and sentiment data regarding all of the stocks in the S&P 500. 
Each ticker will have their own page on the website that serves transformed data regarding each of these metrics in a simple and intuitive way. 
There will also be a live stock ticker price displayed on the page along with the metrics above, and article links to important stories. 


## Ingestion

The two datasources that I used for ingestion are Alpha Vantage and Yahoo Finance.
I used Yahoo finance (yfinance python API library) to read in quarterly financial reports for all companies in the S&P 500. (except ['BRK.B', 'BF.B', 'VLTO'] )
These financial reports are stored in my "StockMarketAnalyzer" resource group with the "storagesma" storage account. In the storage account, all the files are stored in parquet format in the "financialquarterlyreports" Blob container.

I used the Alpha Vantage API to obtain important articles pertaining to each of the stocks in the S&P 500 besides the stocks listed below, since there were no recent news stories about them. In the future, this list may be reduced if there are important publications metnioning them.
['AJG', 'BRK.B', 'BF.B', 'CE', 'CHD', 'CMCSA', 'EQR', 'EL', 'FOXA', 'HBAN', 'LOW', 'MMC', 'MKC', 'MRK', 'MPWR', 'NDAQ', 'NWSA', 'NWS', 'TROW', 'USB']'
Using this API involved the usage of REST API calls through the python "requests" library. 
The Alpha Vantage news and sentiment data is stored in my "StockMarketAnalyzer" resource group with the "storagesma" storage account. In the storage account, all the files are stored in parquet format in the "newsandsentiments" Blob container.

Both of my DataSet ingestion python files are written in databricks, and can be scheduled to update whenever necessary. 
