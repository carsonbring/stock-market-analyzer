[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12453677&assignment_repo_type=AssignmentRepo)
# CSCI 422 Project - Stock Market Analyzer - Carson Bring
This project's goal is to provide a way to access Power BI file that gives updated information regarding stock market news sentiment per sector along with financial data for analysis purposes.

Each ticker has their metrics shown in a simple and intuitive way through filtering by sectors. 
The outcome of my analysis was sort of incomplete, since I wasn't able to find a way to fully link the sentiment scores wiht the financial data, but high level analysis should be able to be performed with this tool nonetheless.
## Architecture
For this project I used Alpha Vantage and Yahoo finance as my two datasources (GENERATION). I then used databricks for my ingestion into Azure blob storage where parquet files are located for every stock in the S&P 500 for recent news sentiment with cited articles along with quarterly financial reports. For my transformation, I used databricks to move my data from parquet into a SQL database hosted in Azure. I then linked this Azure database to a Power Bi file for my Serving. 
## Ingestion

The two datasources that I used for ingestion are Alpha Vantage and Yahoo Finance.

I used Yahoo finance (yfinance python API library) to read in quarterly financial reports for all companies in the S&P 500. (except ['BRK.B', 'BF.B', 'VLTO'] )

These financial reports are stored in my "StockMarketAnalyzer" resource group with the "storagesma" storage account. In the storage account, all the files are stored in parquet format in the "financialquarterlyreports" Blob container.

--------------------------

I used the Alpha Vantage API to obtain important articles pertaining to each of the stocks in the S&P 500 besides the stocks listed below, since there were no recent news stories about them. In the future, this list may be reduced if there are important publications metnioning them.

['AJG', 'BRK.B', 'BF.B', 'CE', 'CHD', 'CMCSA', 'EQR', 'EL', 'FOXA', 'HBAN', 'LOW', 'MMC', 'MKC', 'MRK', 'MPWR', 'NDAQ', 'NWSA', 'NWS', 'TROW', 'USB']'

Using this API involved the usage of REST API calls through the python "requests" library. 

The Alpha Vantage news and sentiment data is stored in my "StockMarketAnalyzer" resource group with the "storagesma" storage account. In the storage account, all the files are stored in parquet format in the "newsandsentiments" Blob container.


Both of my DataSet ingestion python files are written in databricks, and can be scheduled to update whenever necessary. 

## Transformation
I handle my transformation in databricks, similar to my ingestion. I take the data that is in the parquet files that are created when the ingestion files are ran periodically, and convert both of these data sets into a an SQL database that is hosted through Azure. To do this, I use the PyMSSQL python library.

In the transformation process, my code automatically handles scenarios where incomplete data is given for a specific ticker, and removes all instances of that ticker from the database in order to preserve the integrite of the data in the database. 

The data is inserted into a star schema with my tbales being DIM_Ticker, DIM_Report, and FACT_Analysis.

## Serving 
My Serving is handled through a Power BI file that displays the Sentiment Scores by Sector, The Gross Profit per Quarter and Net Income per Quarter on their respective PowerBI pages. 
Both of the financial views are able to be sorted by Sectors, which makes the data a lot easier to read. 

## STEPS FOR REPLICATING
1. Create a resource group
2. Create a keyvault in said resource group to store secrets that will allow you to access blob storage and your database through Databricks.
3. Create a storage account with two blob storage containers to store news sentiment parquets and quarterly financial reports
4. Create a SQL server and database inside of your resource group
5. Create a databricks instance
6. In your key vault, create a key for your storage account access along with a password key for your sql database
7. Import this repository inside of your databricks instance
8. Change the creditials being used for connecting to the blob storage and creata  file named av.config with an alpha vantage api key in the file
9. Run the Ingestion files
10. Create the tables in your SQL database by using the .sql file in the transformation repo directory
11. Run the transformation files
12. View in power BI
13. Schedule the ingestion/transformation python files to run periodically in databricks if you so please (otherwise just run the ingestion files again and then the transformation files to update data)

The code is structured in this repo pretty intuitively with a directory existing for each big step in the data pipeline. 

---------------------------------
Let me know if you have any questions or concerns regarding th ecode in this repo!
