import yfinance as yf
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# List of 50 final stock tickers (replace with your actual list)
stocks = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "ICICIBANK.NS", "HDFCBANK.NS",
    "KOTAKBANK.NS", "LT.NS", "SBIN.NS", "AXISBANK.NS", "BAJFINANCE.NS",
    "TATAMOTORS.NS", "TATASTEEL.NS", "HINDUNILVR.NS", "ITC.NS", "WIPRO.NS",
    "TECHM.NS", "NTPC.NS", "POWERGRID.NS", "ONGC.NS", "COALINDIA.NS",
    "BHARTIARTL.NS", "ADANIPORTS.NS", "ULTRACEMCO.NS", "GRASIM.NS", "NESTLEIND.NS",
    "MARUTI.NS", "ASIANPAINT.NS", "SUNPHARMA.NS", "CIPLA.NS", "DRREDDY.NS",
    "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS", "HCLTECH.NS", "DIVISLAB.NS",
    "SBILIFE.NS", "HDFCLIFE.NS", "ICICIPRULI.NS", "BRITANNIA.NS", "SHREECEM.NS",
    "AMARAJABAT.NS", "TRENT.NS", "TATAELXSI.NS", "JSWENERGY.NS", "EXIDEIND.NS",
    "TVSMOTOR.NS", "ZOMATO.NS", "NHPC.NS", "RVNL.NS", "DIXON.NS"
]

# Date range
end_date = datetime.today()
start_date = end_date - timedelta(days=730)

# Create SQLite DB
conn = sqlite3.connect("stock_data.db")
cursor = conn.cursor()

# Create table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_prices (
        stock TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER,
        PRIMARY KEY (stock, date)
    )
''')

# Fetch and insert data
for stock in stocks:
    print(f"Fetching {stock}...")
    df = yf.download(stock, start=start_date, end=end_date)
    df.reset_index(inplace=True)
    df["stock"] = stock
    df = df[["stock", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
    df.columns = ["stock", "date", "open", "high", "low", "close", "adj_close", "volume"]
    df.to_sql("stock_prices", conn, if_exists="append", index=False)

conn.commit()
conn.close()
print("Data fetching complete and stored in SQLite.")
