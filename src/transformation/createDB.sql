CREATE TABLE DIM_Ticker (
    Ticker VARCHAR(255) PRIMARY KEY,
    CompanyName VARCHAR(255),
    Average_Sentiment FLOAT
);

CREATE TABLE DIM_Report (
    Ticker VARCHAR(255),
    ReportDate DATE,
    TotalRevenue FLOAT,
    NetIncome FLOAT,
    TotalExpenses FLOAT,
    GrossProfit FLOAT,
    EPS FLOAT,
    PRIMARY KEY (Ticker, ReportDate)
);

CREATE TABLE FACT_Analysis (
    FACT_ID INT PRIMARY KEY IDENTITY(1,1),
    Ticker VARCHAR(255),
    ReportDate DATE,
    FOREIGN KEY (Ticker) REFERENCES DIM_Ticker (Ticker),
    FOREIGN KEY (Ticker, ReportDate) REFERENCES DIM_Report (Ticker, ReportDate)
);

