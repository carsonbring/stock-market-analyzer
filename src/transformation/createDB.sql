CREATE TABLE DIM_Ticker (
    Ticker STRING PRIMARY KEY,
    CompanyName STRING,
    Average_Sentiment DOUBLE
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
CREATE TABLE FACT_Analysis (
    FACT_ID INT PRIMARY KEY identity(1,1),
    Ticker STRING,
    ReportDate DATE,
    FOREIGN KEY (Ticker) REFERENCES DIM_Ticker (Ticker),
    FOREIGN KEY (Ticker, ReportDate) REFERENCES DIM_Report (Ticker, ReportDate)
    
);
