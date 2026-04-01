# 📈 Safe-Bet Stock Recommendation System

> **Migrated from GPT-4 to a deterministic rule-based engine.**
> No OpenAI API key. No Streamlit. Just free Yahoo Finance data + 20+ technical indicators + a static GitHub Pages dashboard.

**[🌐 Live Dashboard →](https://yourusername.github.io/safe-bet)**

---

## What Changed (vs Original)

| | Before | Now |
|--|--------|-----|
| **Analysis** | GPT-4 (`gpt_analyze.py`) | Rule-based engine (`analyze.py`) |
| **Frontend** | Streamlit (`streamlit_app.py`) | Static HTML (`index.html`) — GitHub Pages |
| **API Key** | OpenAI required | None needed |
| **Cost** | ~$0.10–0.50/day | $0 forever |
| **Hosting** | Needs a server for Streamlit | GitHub Pages (free static) |
| **Stocks** | 50 | ~85 (Nifty 50 + Next 50 + Midcap) |
| **Indicators** | 3 (price/growth metrics) | 20+ technical indicators |

`gpt_recommendation.json` is still written by `analyze.py` so any downstream tooling you had continues to work.

---

## How It Works

```
[GitHub Actions — daily at 4:15 PM IST]
         │
         ▼
   data_fetch.py          ← Downloads OHLCV from Yahoo Finance → stock_data.db
         │
         ▼
   analyze.py             ← Scores each stock 0–100 using 5 rule categories
         │
         ▼
   predictions.json       ← Top picks, watchlist, avoid list, sector momentum
   gpt_recommendation.json← Legacy format (top 5 picks, same schema as before)
         │
         ▼
   [Git push → GitHub Pages auto-deploys]
         │
         ▼
   index.html             ← Static dashboard reads predictions.json
```

---

## Scoring Methodology

Each stock is scored **0–100** across 5 weighted categories:

| Category | Max Points | Signals |
|----------|-----------|---------|
| **Trend Analysis** | 30 | SMA 20/50/200 alignment · EMA 12/26 crossover · ADX strength · DI+/DI- |
| **Momentum** | 25 | RSI zone (50–65 = ideal buy zone) · MACD line/signal · Stochastic %K/%D · ROC |
| **Volume** | 20 | Volume vs 20-day avg ratio · OBV slope · Price-volume confirmation |
| **Breakout** | 15 | Bollinger Band position/squeeze · 20-day high breakout · 52W high breakout |
| **Price Action** | 10 | 52-week high proximity · 5-day momentum · Gap-up · Candle direction |

**Penalties:** Overbought RSI (>78) · Death cross (SMA50 < SMA200) · Price below SMA200 · Volume divergence

**Signal thresholds:**
- 🟢 STRONG BUY ≥ 75 · 🟩 BUY 60–74 · 🟡 WATCH 45–59 · 🔴 AVOID < 45

---

## Setup

### Deploy on GitHub Pages (Recommended)

```bash
# 1. Fork this repo, then clone
git clone https://github.com/yourusername/safe-bet.git
cd safe-bet

# 2. In GitHub: Settings → Pages → Deploy from main branch → root folder
#    Your site: https://yourusername.github.io/safe-bet

# 3. GitHub Actions runs automatically every weekday at 4:15 PM IST
#    No secrets or API keys needed — everything is free
```

### Run Locally

```bash
pip install -r requirements.txt

# Step 1: Fetch market data (creates stock_data.db)
python data_fetch.py

# Step 2: Score stocks and generate predictions
python analyze.py

# Open index.html in your browser to view results
# (or serve it: python -m http.server 8080)
```

---

## File Structure

```
safe-bet/
├── index.html                     # Static dashboard (replaces streamlit_app.py)
├── predictions.json               # Daily generated — top picks (read by index.html)
├── gpt_recommendation.json        # Legacy format — top 5 (backward compat)
├── stock_data.db                  # SQLite — 2 years OHLCV (gitignored)
├── data_fetch.py                  # Downloads stock data → stock_data.db
├── analyze.py                     # Rule-based scoring → predictions.json
├── gpt_analyze.py                 # DEPRECATED — kept for reference only
├── streamlit_app.py               # DEPRECATED — replaced by index.html
├── requirements.txt               # Python deps (no openai/streamlit)
└── .github/
    └── workflows/
        └── daily.yml              # Runs data_fetch.py + analyze.py daily
```

---

## Stock Universe (~85 stocks)

**Nifty 50:** RELIANCE, TCS, INFY, ICICIBANK, HDFCBANK, KOTAKBANK, LT, SBIN, AXISBANK, BAJFINANCE, TATAMOTORS, TATASTEEL, HINDUNILVR, ITC, WIPRO, TECHM, NTPC, POWERGRID, ONGC, COALINDIA, BHARTIARTL, ADANIPORTS, ULTRACEMCO, GRASIM, NESTLEIND, MARUTI, ASIANPAINT, SUNPHARMA, CIPLA, DRREDDY, BAJAJ-AUTO, HEROMOTOCO, EICHERMOT, HCLTECH, DIVISLAB, SBILIFE, HDFCLIFE, ICICIPRULI, BRITANNIA, SHREECEM, TITAN, BAJAJFINSV, M&M, APOLLOHOSP, TATACONSUM, JSWSTEEL, BPCL, INDUSINDBK, HINDALCO...

**Nifty Next 50 / Midcap selection:** TRENT, TATAELXSI, JSWENERGY, TVSMOTOR, ZOMATO, NHPC, DIXON, VEDL, PFC, RECLTD, HAL, BEL, IRCTC, NAUKRI, PIDILITIND, HAVELLS, LTIM, PERSISTENT, COFORGE, POLYCAB, MUTHOOTFIN, CHOLAFIN, DABUR, MARICO, GODREJCP, COLPAL, AMBUJACEM, ACC, BERGEPAINT, AUROPHARMA, LUPIN...

---

## ⚠️ Disclaimer

This tool is for **educational and informational purposes only** — NOT financial advice. Technical analysis signals do not guarantee future returns. Always do your own research and consult a SEBI-registered advisor before investing.

---

## License

MIT — free to use, modify, distribute.
