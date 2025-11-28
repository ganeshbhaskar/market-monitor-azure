import os
from datetime import datetime
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
TICKERS = ["^GSPC", "^IXIC", "AAPL", "MSFT", "NVDA", "AMZN"]

START_DATE = "2015-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# This will be created in your project:  ../data/market_data_clean.csv
OUTPUT_PATH = os.path.join("..", "data", "market_data_clean.csv")


def fetch_one_ticker(ticker: str) -> pd.DataFrame:
    """Download one ticker and return it in 7-column tidy format."""
    print(f"Downloading data for: {ticker}")

    # IMPORTANT: download ONE ticker at a time (this avoids the wide format)
    df = yf.download(
        ticker,
        start=START_DATE,
        end=END_DATE,
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        print(f"⚠ No data for {ticker}, skipping.")
        return pd.DataFrame()

    # Move Date index to a column
    df = df.reset_index()

    # Keep only columns we care about
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()

    # Rename to our final schema
    df.rename(
        columns={
            "Date": "price_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Volume": "volume",
        },
        inplace=True,
    )

    # Add ticker symbol column
    df["symbol"] = ticker

    # Reorder columns
    df = df[
        [
            "symbol",
            "price_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
        ]
    ]

    return df


def main():
    print("Starting local data ingestion (clean 7-column format)...")

    frames = []
    for ticker in TICKERS:
        one = fetch_one_ticker(ticker)
        if not one.empty:
            frames.append(one)

    if not frames:
        print("❌ No data downloaded for any ticker.")
        return

    final_df = pd.concat(frames, ignore_index=True)

    # Sort nicely
    final_df["price_date"] = pd.to_datetime(final_df["price_date"]).dt.date
    final_df = final_df.sort_values(["symbol", "price_date"]).reset_index(drop=True)

    # Ensure /data exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Save
    final_df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Done.")
    print(f"Saved {len(final_df)} rows to {OUTPUT_PATH}")
    print(final_df.head())


if __name__ == "__main__":
    main()
