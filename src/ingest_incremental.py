import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

# ----------------------------
# CONFIG
# ----------------------------
TICKERS = ["^GSPC", "^IXIC", "AAPL", "MSFT", "NVDA", "AMZN"]

SERVER = "YOURS_SERVER_NAME"
DATABASE = "YOUR_DB_NAME"
USERNAME = "YOUR_CREDENTIALS"
PASSWORD = "YOUR_PASSWORD_HERE"
conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server=tcp:{SERVER},1433;"
    f"Database={DATABASE};"
    f"Uid={USERNAME};"
    f"Pwd={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ----------------------------
# 1. Get last loaded date per symbol from SQL
# ----------------------------
query = """
SELECT symbol, MAX(price_date) AS LastDate
FROM price_history
GROUP BY symbol;
"""

df_last = pd.read_sql(query, conn)
last_dates = df_last.set_index("symbol")["LastDate"].to_dict()

# ----------------------------
# 2. Download ONLY new data for each ticker
# ----------------------------
all_rows = []
default_start = datetime(2015, 1, 1)

for ticker in TICKERS:
    print(f"Processing {ticker}...")

    last_date_for_symbol = last_dates.get(ticker, default_start)
    # Next day after the last loaded date
    start_date = (last_date_for_symbol + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  -> Start date for incremental load: {start_date}")

    # Download daily OHLCV from Yahoo
    df = yf.download(ticker, start=start_date, interval="1d", progress=False)

    if df.empty:
        print(f"  -> No new data for {ticker}")
        continue

    # If yfinance returns a MultiIndex (e.g., ('Open','^GSPC')), flatten it
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # Make Date a normal column
    df.reset_index(inplace=True)

    # Keep and rename columns to match SQL schema
    df_renamed = df.rename(
        columns={
            "Date": "price_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Volume": "volume",
        }
    )[["price_date", "open_price", "high_price", "low_price", "close_price", "volume"]]

    # Add symbol column
    df_renamed["symbol"] = ticker

    all_rows.append(df_renamed)

# ----------------------------
# 3. Combine all new rows
# ----------------------------
if not all_rows:
    print("No new data for any ticker. Exiting.")
    conn.close()
    raise SystemExit()

final_df = pd.concat(all_rows, ignore_index=True)

print("Columns in final_df:", final_df.columns.tolist())
print("Number of new rows:", len(final_df))

# ----------------------------
# 4. UPSERT (MERGE) into SQL – avoid duplicates
# ----------------------------
for _, row in final_df.iterrows():
    cursor.execute(
        """
        MERGE price_history AS target
        USING (SELECT ? AS symbol, ? AS price_date) AS src
        ON target.symbol = src.symbol AND target.price_date = src.price_date
        WHEN MATCHED THEN UPDATE SET
            open_price  = ?,
            high_price  = ?,
            low_price   = ?,
            close_price = ?,
            volume      = ?,
            ingested_at = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (symbol, price_date, open_price, high_price, low_price,
                    close_price, volume, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE());
        """,
        # match key
        row["symbol"],
        row["price_date"],
        # update values
        row["open_price"],
        row["high_price"],
        row["low_price"],
        row["close_price"],
        row["volume"],
        # insert values
        row["symbol"],
        row["price_date"],
        row["open_price"],
        row["high_price"],
        row["low_price"],
        row["close_price"],
        row["volume"],
    )

conn.commit()
conn.close()

print("\nIncremental update complete! 🚀")
