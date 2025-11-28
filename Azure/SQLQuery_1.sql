CREATE TABLE price_history (
    symbol       VARCHAR(20) NOT NULL,
    price_date   DATE        NOT NULL,
    open_price   FLOAT,
    high_price   FLOAT,
    low_price    FLOAT,
    close_price  FLOAT,
    adj_close    FLOAT,
    volume       BIGINT,
    ingested_at  DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_price_history PRIMARY KEY (symbol, price_date)
);

ALTER TABLE price_history
DROP COLUMN adj_close;

SELECT TOP (1000) * FROM [dbo].[price_history]

DROP TABLE price_history;

CREATE TABLE price_history (
    symbol       VARCHAR(20) NOT NULL,
    price_date   DATE        NOT NULL,
    open_price   FLOAT,
    high_price   FLOAT,
    low_price    FLOAT,
    close_price  FLOAT,
    volume       BIGINT,
    ingested_at  DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_price_history PRIMARY KEY (symbol, price_date)
);

SELECT symbol, MAX(price_date) AS LastDate
FROM price_history
GROUP BY symbol;

SELECT TOP 1 *
FROM price_history;

SELECT symbol, MAX(price_date) AS last_date, COUNT(*) AS rows
FROM price_history
GROUP BY symbol
ORDER BY symbol;


SELECT TOP 5 *
FROM price_history
ORDER BY price_date DESC, symbol;
