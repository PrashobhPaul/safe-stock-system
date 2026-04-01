"""
data_fetch.py — StockSage India
================================
Fetches 2 years of daily OHLCV data from Yahoo Finance
and stores it in a local SQLite database (stock_data.db).

Run this first, then run analyze.py.
No API key needed — uses free Yahoo Finance via yfinance.
"""

import yfinance as yf
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("data_fetch")

# ── Stock universe — your original 50 + expanded list ──
STOCKS = [
    # Your original 50
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "ICICIBANK.NS", "HDFCBANK.NS",
    "KOTAKBANK.NS", "LT.NS", "SBIN.NS", "AXISBANK.NS", "BAJFINANCE.NS",
    "TATAMOTORS.NS", "TATASTEEL.NS", "HINDUNILVR.NS", "ITC.NS", "WIPRO.NS",
    "TECHM.NS", "NTPC.NS", "POWERGRID.NS", "ONGC.NS", "COALINDIA.NS",
    "BHARTIARTL.NS", "ADANIPORTS.NS", "ULTRACEMCO.NS", "GRASIM.NS", "NESTLEIND.NS",
    "MARUTI.NS", "ASIANPAINT.NS", "SUNPHARMA.NS", "CIPLA.NS", "DRREDDY.NS",
    "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS", "HCLTECH.NS", "DIVISLAB.NS",
    "SBILIFE.NS", "HDFCLIFE.NS", "ICICIPRULI.NS", "BRITANNIA.NS", "SHREECEM.NS",
    "TRENT.NS", "TATAELXSI.NS", "JSWENERGY.NS", "TVSMOTOR.NS",
    "ZOMATO.NS", "NHPC.NS", "DIXON.NS",
    # Additional blue-chips
    "TITAN.NS", "BAJAJFINSV.NS", "M&M.NS", "APOLLOHOSP.NS",
    "TATACONSUM.NS", "JSWSTEEL.NS", "BPCL.NS", "INDUSINDBK.NS",
    "HINDALCO.NS", "VEDL.NS", "PFC.NS", "RECLTD.NS",
    "HAL.NS", "BEL.NS", "IRCTC.NS", "NAUKRI.NS",
    "PIDILITIND.NS", "HAVELLS.NS", "LTIM.NS", "PERSISTENT.NS",
    "COFORGE.NS", "POLYCAB.NS", "MUTHOOTFIN.NS", "CHOLAFIN.NS",
    "BANKBARODA.NS", "FEDERALBNK.NS", "IDFCFIRSTB.NS", "DABUR.NS",
    "MARICO.NS", "GODREJCP.NS", "COLPAL.NS", "AMBUJACEM.NS",
    "ACC.NS", "BERGEPAINT.NS", "AUROPHARMA.NS", "LUPIN.NS",
]

# Remove duplicates while preserving order
seen = set()
STOCKS = [s for s in STOCKS if not (s in seen or seen.add(s))]

# Date range — 2 years for solid 200 SMA calculation
END_DATE = datetime.today()
START_DATE = END_DATE - timedelta(days=730)


def init_db(conn: sqlite3.Connection):
    """Create the stock_prices table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            stock     TEXT,
            date      TEXT,
            open      REAL,
            high      REAL,
            low       REAL,
            close     REAL,
            volume    INTEGER,
            PRIMARY KEY (stock, date)
        )
    """)
    conn.commit()


def fetch_stock(ticker: str, conn: sqlite3.Connection) -> int:
    """
    Downloads OHLCV data and upserts into SQLite.
    Returns number of rows inserted/updated.
    """
    try:
        df = yf.download(
            ticker,
            start=START_DATE,
            end=END_DATE,
            progress=False,
            auto_adjust=True,   # Adjusts for splits/dividends automatically
        )

        if df is None or df.empty:
            log.warning(f"  {ticker}: No data returned")
            return 0

        # Flatten multi-level columns (yfinance sometimes returns these)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        required = ["Open", "High", "Low", "Close", "Volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            log.warning(f"  {ticker}: Missing columns {missing}")
            return 0

        df = df[required].copy()
        df.index = pd.to_datetime(df.index)
        df.dropna(subset=["Close"], inplace=True)
        df.reset_index(inplace=True)

        rows_inserted = 0
        for _, row in df.iterrows():
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO stock_prices
                       (stock, date, open, high, low, close, volume)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ticker,
                        row["Date"].strftime("%Y-%m-%d"),
                        float(row["Open"]),
                        float(row["High"]),
                        float(row["Low"]),
                        float(row["Close"]),
                        int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
                    ),
                )
                rows_inserted += 1
            except Exception:
                pass

        conn.commit()
        return rows_inserted

    except Exception as e:
        log.warning(f"  {ticker}: Error — {str(e)[:80]}")
        return 0


def main():
    log.info("=" * 55)
    log.info("  StockSage India — Data Fetch")
    log.info(f"  Stocks : {len(STOCKS)}")
    log.info(f"  Range  : {START_DATE.date()} → {END_DATE.date()}")
    log.info("=" * 55)

    conn = sqlite3.connect("stock_data.db")
    init_db(conn)

    total_rows = 0
    success = 0

    for i, ticker in enumerate(STOCKS, 1):
        log.info(f"  [{i:>3}/{len(STOCKS)}] {ticker:<22} ...", )
        rows = fetch_stock(ticker, conn)
        if rows > 0:
            log.info(f"  [{i:>3}/{len(STOCKS)}] {ticker:<22} ✓ {rows} rows")
            total_rows += rows
            success += 1
        # Polite delay to avoid rate limiting
        time.sleep(0.3)

    conn.close()
    log.info(f"\n✅ Done — {success}/{len(STOCKS)} stocks, {total_rows:,} rows stored in stock_data.db")


if __name__ == "__main__":
    main()
