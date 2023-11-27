CREATE TABLE DIM_Ticker (
    Ticker STRING PRIMARY KEY,
    CompanyName STRING
);
CREATE TABLE DIM_Report (
    Ticker STRING,
    ReportDate DATE,
    TotalRevenue DOUBLE,
    NetIncome DOUBLE,
    GrossProfit DOUBLE,
    EPS DOUBLE,
    NormalizedEBITDA DOUBLE,

    PRIMARY KEY (Ticker, ReportDate),
    FOREIGN KEY (Ticker) REFERENCES DIM_Ticker (Ticker)
    
);
CREATE TABLE DIM_Article (
    Article_ID INT PRIMARY KEY identity(1,1),
    Ticker STRING,
    Article_Title STRING,
    Article_Source STRING,
    Article_Sentiment DOUBLE,
    Article_Date DATE,
    FOREIGN KEY (Ticker) REFERENCES DIM_Ticker (Ticker)
    
);
CREATE TABLE FACT_Analysis (
    Article_ID INT,
    Ticker STRING,
    ReportDate DATE,
    SentimentScore DOUBLE,
    TotalRevenue DOUBLE,
    TotalProfit DOUBLE,
    EPS DOUBLE,
    PRIMARY KEY (Article_ID, ReportDate),
    FOREIGN KEY (Article_ID) REFERENCES DIM_Article (Article_ID),
    FOREIGN KEY (Ticker, ReportDate) REFERENCES DIM_Report (Ticker, ReportDate)
    
);
