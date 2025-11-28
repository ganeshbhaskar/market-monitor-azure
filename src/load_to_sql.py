import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def get_engine():
    load_dotenv()

    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")

    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )

    return create_engine(connection_string)

def transform(df):
    # Rename columns
    df = df.rename(columns={
        "Date": "price_date",
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Symbol": "symbol"
    })

    # 🟢 Convert DD-MM-YYYY or MM/DD/YYYY to real datetime
    df["price_date"] = pd.to_datetime(df["price_date"], errors="coerce")

    # Drop rows where date is invalid (None)
    df = df.dropna(subset=["price_date"])

    # Only keep needed columns
    df = df[[
        "symbol",
        "price_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume"
    ]]

    return df


def main():
    print("Loading local CSV → Azure SQL (without Adj Close)…")

    # Load local CSV file
    df = pd.read_csv("data/market_data_raw.csv")

    # Transform to match SQL schema
    df = transform(df)

    engine = get_engine()

    # Write to SQL table
    df.to_sql(
        name="price_history",
        con=engine,
        if_exists="append",
        index=False
    )

    print("DONE — Data successfully loaded into Azure SQL.")

if __name__ == "__main__":
    main()
