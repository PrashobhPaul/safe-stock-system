"""
quotes_daily.py — StockSage/ProfitPilot daily pipeline
=======================================================
Fetches prices, scores stocks, pulls RSS news, produces three-horizon forecasts,
per-stock advisor narratives, and a dramatically richer daily brief.
Writes predictions.json and news_cache.json.

Called by .github/workflows/daily.yml.
Zero LLM. Zero paid services. 100% deterministic.
"""

import json
import os
import sys
import time
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, date
import pytz

# Add parent dir to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_fetcher import fetch_all_news
from news_analyzer import analyze, aggregate_ticker_sentiment, aggregate_macro_sentiment, news_for_ticker
from forecast_engine import forecast_stock
from advisor_narrator import build_narrative

IST = pytz.timezone("Asia/Kolkata")


# ───────────────────────────────────────────────────────────────────────────
# RESILIENT YAHOO FETCH
# ───────────────────────────────────────────────────────────────────────────
# Yahoo tightened bot detection in 2025. A plain requests session (or older
# yfinance versions) now frequently returns empty JSON bodies and the parser
# raises "Expecting value: line 1 column 1 (char 0)". The fix is two-fold:
#   1. Use curl_cffi to impersonate a real Chrome TLS fingerprint.
#   2. Never trust a single batch call — fall back to per-ticker retries so
#      one bad ticker doesn't zero out the whole run.
def _build_session():
    """Return a curl_cffi Chrome-impersonating session, or None if unavailable."""
    try:
        from curl_cffi import requests as curl_requests
        return curl_requests.Session(impersonate="chrome")
    except Exception as e:
        print(f"  ⚠ curl_cffi unavailable ({e}); falling back to default yfinance session")
        return None


def fetch_prices_resilient(tickers, period="1y", interval="1d",
                           batch_retries=2, per_ticker_retries=2, sleep_between=1.0):
    """
    Robust price fetcher.
    Strategy:
      A. Try batch download with a hardened session, up to batch_retries times.
      B. For any ticker missing or empty after batch, retry per-ticker.
      C. Return a MultiIndex DataFrame shaped like yf.download(group_by='ticker').
    Never raises — returns whatever it could fetch. Empty DataFrame means
    everything failed; caller must handle that gracefully.
    """
    session = _build_session()
    got = {}  # ticker -> single-ticker DataFrame

    # --- Phase A: batch attempts ---
    for attempt in range(1, batch_retries + 1):
        try:
            kw = dict(
                tickers=tickers, period=period, interval=interval,
                progress=False, auto_adjust=True, group_by="ticker",
                threads=True,
            )
            if session is not None:
                kw["session"] = session
            df_all = yf.download(**kw)
            if df_all is not None and not df_all.empty:
                if isinstance(df_all.columns, pd.MultiIndex):
                    available = set(df_all.columns.get_level_values(0))
                    for t in tickers:
                        if t in available:
                            sub = df_all[t].dropna(subset=["Close"])
                            if not sub.empty:
                                got[t] = sub
                else:
                    # Single-ticker shape (unlikely with multiple tickers, but defensive)
                    sub = df_all.dropna(subset=["Close"])
                    if not sub.empty and len(tickers) == 1:
                        got[tickers[0]] = sub
            print(f"      ↳ batch attempt {attempt}: {len(got)}/{len(tickers)} tickers usable")
            if len(got) >= int(0.8 * len(tickers)):
                break  # good enough, skip remaining batch retries
        except Exception as e:
            print(f"      ↳ batch attempt {attempt} failed: {e}")
        time.sleep(sleep_between)

    # --- Phase B: per-ticker retry for missing ones ---
    missing = [t for t in tickers if t not in got]
    if missing:
        print(f"      ↳ retrying {len(missing)} missing tickers individually...")
        for t in missing:
            for attempt in range(1, per_ticker_retries + 1):
                try:
                    kw = dict(period=period, interval=interval, auto_adjust=True, progress=False)
                    if session is not None:
                        kw["session"] = session
                    tk = yf.Ticker(t, session=session) if session is not None else yf.Ticker(t)
                    df = tk.history(period=period, interval=interval, auto_adjust=True)
                    if df is not None and not df.empty and "Close" in df.columns:
                        got[t] = df.dropna(subset=["Close"])
                        break
                except Exception as e:
                    if attempt == per_ticker_retries:
                        print(f"        ✗ {t}: {e}")
                time.sleep(0.3)

    # --- Phase C: reshape back into a MultiIndex DataFrame like yf.download returns ---
    if not got:
        print("      ✗ All fetches failed. Returning empty frame.")
        return pd.DataFrame()
    frames = {t: df for t, df in got.items()}
    combined = pd.concat(frames, axis=1)  # MultiIndex: (ticker, OHLCV)
    print(f"      ✓ final usable tickers: {len(got)}/{len(tickers)}")
    return combined


def load_previous_predictions():
    """Load the last-good predictions.json from disk, or None if absent/corrupt."""
    try:
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "predictions.json",
        )
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            prev = json.load(f)
        # Only treat as "good" if it has real picks
        if prev.get("stocks_analyzed", 0) > 0:
            return prev
        return None
    except Exception as e:
        print(f"  ⚠ could not load previous predictions.json: {e}")
        return None

# ───────────────────────────────────────────────────────────────────────────
# STOCK UNIVERSE
# ───────────────────────────────────────────────────────────────────────────
STOCKS = {
    "RELIANCE.NS":  ("Reliance Industries",   "Conglomerate"),
    "TCS.NS":       ("Tata Consultancy Svc",  "IT"),
    "HDFCBANK.NS":  ("HDFC Bank",             "Banking"),
    "INFY.NS":      ("Infosys",               "IT"),
    "ICICIBANK.NS": ("ICICI Bank",            "Banking"),
    "WIPRO.NS":     ("Wipro",                 "IT"),
    "HCLTECH.NS":   ("HCL Technologies",      "IT"),
    "BAJFINANCE.NS":("Bajaj Finance",         "NBFC"),
    "TITAN.NS":     ("Titan Company",         "Consumer Disc."),
    "SUNPHARMA.NS": ("Sun Pharmaceutical",    "Pharma"),
    "MARUTI.NS":    ("Maruti Suzuki",         "Automobiles"),
    "BHARTIARTL.NS":("Bharti Airtel",         "Telecom"),
    "AXISBANK.NS":  ("Axis Bank",             "Banking"),
    "KOTAKBANK.NS": ("Kotak Mahindra Bank",   "Banking"),
    "LT.NS":        ("Larsen & Toubro",       "Infrastructure"),
    "SBIN.NS":      ("State Bank of India",   "Banking"),
    "NTPC.NS":      ("NTPC",                  "Power"),
    "APOLLOHOSP.NS":("Apollo Hospitals",      "Healthcare"),
    "TRENT.NS":     ("Trent",                 "Retail"),
    "LTIM.NS":      ("LTIMindtree",           "IT"),
    "ADANIPORTS.NS":("Adani Ports",           "Infrastructure"),
    "HINDUNILVR.NS":("Hindustan Unilever",    "FMCG"),
    "ITC.NS":       ("ITC",                   "FMCG"),
    "BAJAJFINSV.NS":("Bajaj Finserv",         "NBFC"),
    "TATAMOTORS.NS":("Tata Motors",           "Automobiles"),
    "TATASTEEL.NS": ("Tata Steel",            "Metals"),
    "ASIANPAINT.NS":("Asian Paints",          "Paints"),
    "DRREDDY.NS":   ("Dr. Reddy's",           "Pharma"),
    "CIPLA.NS":     ("Cipla",                 "Pharma"),
    "EICHERMOT.NS": ("Eicher Motors",         "Automobiles"),
    "HEROMOTOCO.NS":("Hero MotoCorp",         "Automobiles"),
    "BAJAJ-AUTO.NS":("Bajaj Auto",            "Automobiles"),
    "BRITANNIA.NS": ("Britannia Industries",  "FMCG"),
    "NESTLEIND.NS": ("Nestle India",          "FMCG"),
    "POWERGRID.NS": ("Power Grid Corp",       "Power"),
    "ONGC.NS":      ("ONGC",                  "Oil & Gas"),
    "HAL.NS":       ("Hindustan Aeronautics", "Defence"),
    "BEL.NS":       ("Bharat Electronics",    "Defence"),
    "PERSISTENT.NS":("Persistent Systems",    "IT"),
    "COFORGE.NS":   ("Coforge",               "IT"),
    "POLYCAB.NS":   ("Polycab India",         "Electricals"),
    "DIXON.NS":     ("Dixon Technologies",    "Electronics"),
    "PFC.NS":       ("Power Finance Corp",    "NBFC"),
    "RECLTD.NS":    ("REC Ltd",               "NBFC"),
    "TATACONSUM.NS":("Tata Consumer Prods",   "FMCG"),
    "INDUSINDBK.NS":("IndusInd Bank",         "Banking"),
    "HINDALCO.NS":  ("Hindalco Industries",   "Metals"),
    "JSWSTEEL.NS":  ("JSW Steel",             "Metals"),
    "DIVISLAB.NS":  ("Divi's Laboratories",   "Pharma"),
    "COALINDIA.NS": ("Coal India",            "Mining"),
}


# ───────────────────────────────────────────────────────────────────────────
# TECHNICAL SCORING (same rule engine, unchanged for backward-compat)
# ───────────────────────────────────────────────────────────────────────────
def score_stock(ticker, df):
    try:
        c  = df["Close"]; h = df["High"]; lo = df["Low"]; v = df["Volume"]; op = df["Open"]
        close = float(c.iloc[-1])
        prev  = float(c.iloc[-2]) if len(c) > 1 else close
        chg   = round((close - prev) / prev * 100, 2) if prev else 0

        sma20  = float(c.rolling(20).mean().iloc[-1])
        sma50  = float(c.rolling(50).mean().iloc[-1])
        sma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None

        ema12 = float(c.ewm(span=12, adjust=False).mean().iloc[-1])
        ema26 = float(c.ewm(span=26, adjust=False).mean().iloc[-1])

        d = c.diff()
        g = d.where(d > 0, 0.0).rolling(14).mean()
        l = (-d.where(d < 0, 0.0)).rolling(14).mean()
        rsi = float((100 - 100 / (1 + g / l.replace(0, np.nan))).iloc[-1])
        if np.isnan(rsi): rsi = 50.0

        ml = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        sl_ = ml.ewm(span=9, adjust=False).mean()
        hist = ml - sl_
        macd_b = float(ml.iloc[-1]) > float(sl_.iloc[-1])
        hist_ex = float(hist.iloc[-1]) > float(hist.iloc[-2]) if len(hist) > 1 else False

        vavg = float(v.rolling(20).mean().iloc[-1])
        vr = round(float(v.iloc[-1]) / vavg, 2) if vavg > 0 else 1.0

        bb_m = c.rolling(20).mean()
        bb_s = c.rolling(20).std()
        bb_pos = float(((c - (bb_m - 2*bb_s)) / (4*bb_s.replace(0, np.nan))).iloc[-1])
        if np.isnan(bb_pos): bb_pos = 0.5

        obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
        obv_up = float(obv.iloc[-1] - obv.iloc[-6]) > 0 if len(obv) >= 6 else False

        hl = h - lo
        hc = (h - c.shift()).abs()
        lc = (lo - c.shift()).abs()
        atr = float(pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean().iloc[-1])
        if np.isnan(atr): atr = close * 0.018

        w52h = float(h.iloc[-252:].max()) if len(h) >= 252 else float(h.max())
        w52_pct = round((w52h - close) / w52h * 100, 1)

        reasons = []
        trend = 0
        if sma200:
            if close > sma20 > sma50 > sma200:
                trend += 12; reasons.append("Full bullish SMA alignment")
            elif close > sma50 > sma200:
                trend += 8; reasons.append("Bullish SMA alignment")
            elif close > sma200:
                trend += 4; reasons.append("Above 200 SMA")
            pct200 = (close - sma200) / sma200 * 100
            if 5 <= pct200 <= 25: trend += 4
            elif 0 < pct200 < 5: trend += 2
        else:
            if close > sma20 > sma50: trend += 8
            elif close > sma50: trend += 4
        if ema12 > ema26: trend += 3; reasons.append("EMA 12/26 bullish")
        if close > sma20: trend += 3
        trend = min(trend, 30)

        mom = 0
        if 50 <= rsi <= 65: mom += 12; reasons.append(f"RSI ideal zone ({rsi:.1f})")
        elif 45 <= rsi < 50: mom += 8; reasons.append(f"RSI building ({rsi:.1f})")
        elif 65 < rsi <= 72: mom += 6; reasons.append(f"RSI strong ({rsi:.1f})")
        elif 40 <= rsi < 45: mom += 4
        elif 30 <= rsi < 40: mom += 2
        if macd_b: mom += 5; reasons.append("MACD bullish")
        if hist_ex: mom += 4; reasons.append("MACD histogram expanding")
        mom = min(mom, 25)

        vol = 0
        if vr >= 2.5: vol += 10; reasons.append(f"Volume surge {vr:.1f}x")
        elif vr >= 2.0: vol += 8; reasons.append(f"High volume {vr:.1f}x")
        elif vr >= 1.5: vol += 6; reasons.append(f"Above avg vol {vr:.1f}x")
        elif vr >= 1.2: vol += 4
        elif vr >= 0.9: vol += 2
        if obv_up: vol += 4; reasons.append("OBV trending up")
        vol = min(max(vol, 0), 20)

        brk = 0
        if bb_pos >= 0.75: brk += 5; reasons.append("Near upper BB")
        elif bb_pos >= 0.55: brk += 3
        if len(h) >= 21 and close > float(h.iloc[-21:-1].max()):
            brk += 5; reasons.append("Breaking 20-day high!")
        elif len(h) >= 11 and close > float(h.iloc[-11:-1].max()):
            brk += 2
        if len(h) >= 252 and close > float(h.iloc[-252:-1].max()):
            brk += 5; reasons.append("New 52-week high!")
        brk = min(max(brk, 0), 15)

        pa = 0
        if w52_pct <= 3: pa += 5; reasons.append(f"Near 52W high ({w52_pct:.1f}%)")
        elif w52_pct <= 8: pa += 3
        elif w52_pct <= 15: pa += 2
        elif w52_pct <= 25: pa += 1
        if len(c) >= 6:
            r5 = (close - float(c.iloc[-6])) / float(c.iloc[-6]) * 100
            if r5 >= 4: pa += 3
            elif r5 >= 2: pa += 2
            elif r5 >= 0: pa += 1
        if close > float(op.iloc[-1]): pa += 1
        pa = min(pa, 10)

        penalty = 0
        if rsi > 78: penalty += 10
        if sma200 and close < sma200: penalty += 5
        if sma200 and sma50 < sma200: penalty += 6
        if chg < -2: penalty += 3

        total = max(0, min(100, trend + mom + vol + brk + pa - penalty))

        if total >= 72: sig, conf = "STRONG BUY", "HIGH"
        elif total >= 55: sig, conf = "BUY", "HIGH" if total >= 63 else "MEDIUM"
        elif total >= 40: sig, conf = "WATCH", "MEDIUM"
        else: sig, conf = "AVOID", "LOW"

        # Category assignment
        short_pts = medium_pts = long_pts = 0
        if brk >= 8: short_pts += 4
        if vr >= 1.5: short_pts += 3
        if 50 <= rsi <= 67: short_pts += 3
        if hist_ex: short_pts += 3
        if w52_pct <= 8: short_pts += 3
        if chg > 0.5: short_pts += 2
        if trend >= 15: medium_pts += 4
        if macd_b: medium_pts += 3
        if 45 <= rsi <= 65: medium_pts += 3
        if vr >= 1.2: medium_pts += 2
        if 8 < w52_pct <= 25: medium_pts += 3
        if sma200 and close > sma20 > sma50 > sma200: long_pts += 5
        if trend >= 20: long_pts += 3
        if rsi < 60: long_pts += 2
        if w52_pct > 20: long_pts += 3
        if obv_up: long_pts += 2

        if short_pts >= medium_pts and short_pts >= long_pts: cat = "short"
        elif medium_pts >= long_pts: cat = "medium"
        else: cat = "long"

        # Targets
        if cat == "short":
            pullback = atr * 0.28; window = "09:15–09:45 AM IST"
            strategy = "Place limit order before 9:14 AM"
            t1 = round(close + atr * 1.8, 2); t2 = round(close + atr * 2.8, 2); t3 = round(close + atr * 4.0, 2)
            hold_dur = "5–15 trading days"
            sell_trig = f"Exit when RSI > 72 or price hits ₹{t2:,.2f}"
        elif cat == "medium":
            pullback = atr * 0.50; window = "09:15–10:15 AM IST"
            strategy = "Patient limit order"
            t1 = round(close + atr * 3.5, 2); t2 = round(close + atr * 5.5, 2); t3 = round(close + atr * 8.0, 2)
            hold_dur = "4–12 weeks"
            sell_trig = f"Exit on weekly MACD bearish cross or ₹{t2:,.2f}"
        else:
            pullback = atr * 0.75; window = "09:15 AM IST (GTC)"
            strategy = "GTC limit — no urgency"
            t1 = round(close * 1.25, 2); t2 = round(close * 1.45, 2); t3 = round(close * 1.65, 2)
            hold_dur = "6–18 months"
            sell_trig = f"Exit if fundamentals deteriorate or ₹{t2:,.2f} hit"

        entry_price = round(close - pullback, 2)
        limit_ord = round(entry_price * 0.9993, 2)
        entry_max = round(close * (1.003 if cat == "short" else 1.005 if cat == "medium" else 1.010), 2)
        u1 = round((t1 - entry_price) / entry_price * 100, 1)
        u2 = round((t2 - entry_price) / entry_price * 100, 1)
        u3 = round((t3 - entry_price) / entry_price * 100, 1)

        macd_str = "Bullish" if macd_b else "Bearish"
        if sma200:
            if close > sma20 > sma50 > sma200: sma_str = "Full Bull (>20>50>200)"
            elif close > sma50 > sma200: sma_str = "Bull (>50>200)"
            elif close > sma200: sma_str = "Above 200 SMA"
            else: sma_str = "Bearish"
        else:
            sma_str = "Bullish" if close > sma50 else "Bearish"

        trend_label = "Uptrend" if trend >= 18 else "Downtrend" if trend < 8 else "Sideways"

        return {
            "symbol": ticker.replace(".NS", ""),
            "score": int(total),
            "signal": sig,
            "confidence": conf,
            "current_price": round(close, 2),
            "change_pct": chg,
            "change": round(close - prev, 2),
            "target_price": t2,
            "stop_loss": round(close * 0.94, 2),
            "risk_reward": round((t2 - entry_price) / max(close - close * 0.94, 1), 1),
            "holding_category": cat,
            "sector": "Various",
            "penalty": int(penalty),
            "scores": {"trend": int(trend), "momentum": int(mom), "volume": int(vol), "breakout": int(brk), "price_action": int(pa)},
            "indicators": {
                "rsi": round(rsi, 1), "macd_signal": macd_str, "sma_alignment": sma_str,
                "volume_ratio": vr, "week52_pct": w52_pct, "bb_position": round(bb_pos, 2), "adx": None,
            },
            # Extra fields needed by forecast/narrator
            "_tech_meta": {
                "score": int(total), "rsi": round(rsi, 1), "trend_label": sma_str,
                "volume_ratio": vr, "change_pct": chg,
            },
            "_atr": round(atr, 2),
            "_entry_ceiling": entry_max,
            "reasons": reasons[:6],
            "trade_plan": {
                "category": cat, "cat_score": int(max(short_pts, medium_pts, long_pts)),
                "atr": round(atr, 2), "atr_pct": round(atr / close * 100, 2),
                "entry": {
                    "ideal_price": entry_price, "limit_order": limit_ord, "acceptable_max": entry_max,
                    "entry_window": window, "order_strategy": strategy,
                    "note": f"Prev close ₹{close:,.2f} | Dip ~₹{round(pullback, 2)}",
                },
                "exit": {
                    "target_conservative": t1, "target_ideal": t2, "target_stretch": t3,
                    "upside_conservative": u1, "upside_ideal": u2, "upside_stretch": u3,
                    "hold_min_days": 5 if cat == "short" else 30 if cat == "medium" else 180,
                    "hold_max_days": 15 if cat == "short" else 90 if cat == "medium" else 540,
                    "hold_duration": hold_dur, "sell_trigger": sell_trig,
                    "hold_note": "Review daily" if cat == "short" else "Review weekly" if cat == "medium" else "Review monthly",
                },
            },
        }
    except Exception as e:
        print(f"  ✗ score_stock error for {ticker}: {e}")
        return None


# ───────────────────────────────────────────────────────────────────────────
# REGIME DETECTION
# ───────────────────────────────────────────────────────────────────────────
def detect_regime(results: list) -> dict:
    if not results:
        return {"label": "UNKNOWN", "breadth_pct": 0, "momentum_pct": 0, "volatility": 0, "notes": "no data"}
    n = len(results)
    bullish = sum(1 for r in results
                  if (r.get("indicators", {}).get("sma_alignment", "") or "").startswith(("Full Bull", "Bull", "Above")))
    breadth = round(bullish / n * 100, 1)
    median_chg = float(np.median([r.get("change_pct", 0) for r in results]))
    median_atr_pct = float(np.median([r.get("trade_plan", {}).get("atr_pct", 0) for r in results]))

    if breadth >= 60 and median_chg > 0:
        label = "BULL"
    elif breadth <= 40 or median_chg < -0.5:
        label = "BEAR"
    else:
        label = "SIDEWAYS"

    return {
        "label": label,
        "breadth_pct": breadth,
        "momentum_pct": round(median_chg, 2),
        "volatility": round(median_atr_pct, 2),
        "notes": f"{n} stocks analyzed",
    }


# ───────────────────────────────────────────────────────────────────────────
# VERDICT LABEL MAPPING
# ───────────────────────────────────────────────────────────────────────────
def verdict_from_signal(signal: str, score: int, news_label: str) -> tuple[str, str]:
    """Returns (action, verdict)."""
    if signal == "STRONG BUY":
        if news_label == "BEARISH":
            return "WAIT_FOR_DIP", "ACCUMULATE ON DIP"
        return "ADD", "STRONG BUY"
    if signal == "BUY":
        if news_label == "BEARISH":
            return "WAIT_FOR_DIP", "ACCUMULATE ON DIP"
        return "ADD", "BUY"
    if signal == "WATCH":
        return "HOLD", "HOLD"
    if signal == "AVOID":
        if news_label == "BEARISH":
            return "EXIT", "EXIT"
        return "HOLD", "HOLD"
    return "HOLD", "HOLD"


# ───────────────────────────────────────────────────────────────────────────
# RICH DAILY BRIEF BUILDER
# ───────────────────────────────────────────────────────────────────────────
def build_rich_daily_brief(results: list, regime: dict, news_items: list,
                           macro_sentiment: dict, sector_momentum: list) -> dict:
    try:
        now = datetime.now(IST)

        # ── Headline composition ──
        adds = sum(1 for r in results if r.get("_action") == "ADD")
        holds = sum(1 for r in results if r.get("_action") == "HOLD")
        exits = sum(1 for r in results if r.get("_action") == "EXIT")
        wfds = sum(1 for r in results if r.get("_action") == "WAIT_FOR_DIP")

        label = regime.get("label", "UNKNOWN")
        breadth = regime.get("breadth_pct", 0)
        macro_label = macro_sentiment.get("label", "NEUTRAL")

        if exits >= 5:
            headline = f"{label} regime — {exits} exits flagged, defensive day; macro tone {macro_label.replace('_', ' ').lower()}"
        elif adds >= 6 and breadth >= 55:
            headline = f"{label} regime with {breadth:.0f}% breadth — {adds} fresh BUY setups; macro {macro_label.replace('_', ' ').lower()}"
        elif adds >= 3:
            headline = f"{label} tape — {adds} ADDs, {exits + wfds} cautious; {macro_sentiment.get('count', 0)} macro items in focus"
        elif label == "BEAR":
            headline = f"Risk-off: {breadth:.0f}% breadth, tighten stops; macro {macro_label.replace('_', ' ').lower()}"
        else:
            headline = f"{label} regime — {breadth:.0f}% breadth, selective stance; macro {macro_label.replace('_', ' ').lower()}"

        # ── Key insight ──
        insight_parts = []
        if label == "BULL" and breadth >= 60:
            insight_parts.append(
                f"Broad-based strength with {breadth:.0f}% of names above their 50-day average and median move "
                f"{regime.get('momentum_pct', 0):+.1f}% — the kind of tape where adding quality on 2-3% dips usually pays."
            )
        elif label == "BEAR":
            insight_parts.append(
                f"Risk-off regime with {breadth:.0f}% breadth and median momentum {regime.get('momentum_pct', 0):+.1f}%. "
                f"Raise cash, honour stops, resist bottom-fishing until breadth recovers above 55%."
            )
        elif label == "SIDEWAYS":
            insight_parts.append(
                f"Range-bound tape with {breadth:.0f}% breadth. No directional edge — let existing positions work, "
                f"avoid fresh breakout trades, save dry powder for the next regime shift."
            )
        else:
            insight_parts.append(f"{label} regime with mixed signals. Lean on stock-specific setups.")

        if macro_sentiment.get("count", 0) >= 3:
            insight_parts.append(
                f"Macro flow is {macro_label.replace('_', ' ').lower()} with {macro_sentiment['count']} "
                f"policy/global items in focus — factor this into position sizing."
            )

        if sector_momentum and len(sector_momentum) >= 3:
            top = sector_momentum[0]
            bot = sector_momentum[-1]
            spread = top["score"] - bot["score"]
            if spread >= 15:
                insight_parts.append(
                    f"Sector dispersion is wide: {top['sector']} leads at {top['score']} while {bot['sector']} "
                    f"lags at {bot['score']} — a stock-picker's tape."
                )

        key_insight = " ".join(insight_parts)

        # ── Narrative bullets ──
        narrative = [
            f"Regime: {label} · breadth {breadth:.0f}% · median momentum {regime.get('momentum_pct', 0):+.1f}% · ATR {regime.get('volatility', 0):.1f}%",
            f"Book: {adds} ADD · {wfds} ACCUMULATE · {holds} HOLD · {exits} EXIT",
            f"Macro tone: {macro_label.replace('_', ' ')} from {macro_sentiment.get('count', 0)} items across {len(set(i.get('source', '') for i in news_items))} sources",
        ]
        if sector_momentum:
            top = sector_momentum[0]
            narrative.append(f"Strongest sector: {top['sector']} (avg score {top['score']})")

        # ── Conviction board with rich narratives ──
        conviction = []
        candidates = sorted(
            [r for r in results if r.get("_action") in ("ADD", "WAIT_FOR_DIP", "HOLD")],
            key=lambda r: -r.get("score", 0)
        )[:6]
        for r in candidates:
            conviction.append({
                "symbol": r["symbol"], "name": r.get("name", ""), "sector": r.get("sector", ""),
                "action": r.get("_action"), "verdict": r.get("_verdict", ""),
                "score": r.get("score", 0), "rsi": r.get("indicators", {}).get("rsi"),
                "trend": r.get("indicators", {}).get("sma_alignment"),
                "reason": r.get("reasons", ["Strong setup"])[0] if r.get("reasons") else "Strong setup",
                "narrative": r.get("_narrative", ""),
                "forecasts": r.get("_forecasts", {}),
                "news_info": r.get("_news_info", {}),
            })

        # ── Risk watchlist ──
        risk_watchlist = []
        for r in results:
            flags = []
            rsi = r.get("indicators", {}).get("rsi")
            if rsi is not None and rsi >= 78: flags.append(f"RSI {rsi:.0f} overbought")
            if rsi is not None and rsi <= 30: flags.append(f"RSI {rsi:.0f} oversold")
            if r.get("_action") in ("EXIT", "TRIM"): flags.append(f"rule → {r['_action']}")
            news_info = r.get("_news_info", {})
            if news_info.get("label") == "BEARISH":
                flags.append(f"negative news ({news_info.get('count', 0)})")
            if flags:
                risk_watchlist.append({
                    "symbol": r["symbol"], "name": r.get("name", ""),
                    "action": r.get("_action"), "flags": flags,
                    "verdict": r.get("_verdict", "HOLD"),
                })
        risk_watchlist = risk_watchlist[:8]

        # ── Breakout watch ──
        breakout_watch = []
        for r in results:
            w52 = r.get("indicators", {}).get("week52_pct")
            if w52 is not None and w52 <= 3.5 and r.get("score", 0) >= 55:
                breakout_watch.append({
                    "symbol": r["symbol"], "name": r.get("name", ""),
                    "pct_from_52h": round(w52, 1), "score": r.get("score", 0),
                    "action": r.get("_action"),
                })
        breakout_watch.sort(key=lambda x: (x["pct_from_52h"], -x["score"]))
        breakout_watch = breakout_watch[:6]

        # ── Action plan ──
        action_plan = []
        for r in results:
            if r.get("_action") == "EXIT":
                action_plan.append({
                    "priority": 1, "type": "EXIT", "symbol": r["symbol"],
                    "name": r.get("name", ""), "price": r.get("current_price"),
                    "instruction": "Market order at open — rule ladder triggered.",
                    "reason": r.get("reasons", ["Exit signal"])[0] if r.get("reasons") else "Exit signal",
                    "confidence": r.get("confidence", "LOW"), "verdict": "EXIT",
                })
        for r in results:
            if r.get("_action") == "WAIT_FOR_DIP":
                entry = r.get("_entry_ceiling") or r.get("current_price", 0)
                action_plan.append({
                    "priority": 2, "type": "ACCUMULATE", "symbol": r["symbol"],
                    "name": r.get("name", ""), "price": r.get("current_price"),
                    "instruction": f"GTC limit ≤ ₹{entry:,.0f} — accumulate on dip",
                    "reason": r.get("reasons", ["Good setup, stretched entry"])[0] if r.get("reasons") else "Good setup",
                    "confidence": r.get("confidence", "MEDIUM"), "verdict": "ACCUMULATE",
                })
        adds_sorted = sorted([r for r in results if r.get("_action") == "ADD"],
                             key=lambda r: -r.get("score", 0))[:5]
        for r in adds_sorted:
            entry = r.get("_entry_ceiling") or r.get("current_price", 0)
            action_plan.append({
                "priority": 3, "type": "ADD", "symbol": r["symbol"],
                "name": r.get("name", ""), "price": r.get("current_price"),
                "instruction": f"GTC limit ≤ ₹{entry:,.0f}",
                "reason": r.get("reasons", ["Strong setup"])[0] if r.get("reasons") else "Strong setup",
                "confidence": r.get("confidence", "HIGH"), "verdict": r.get("_verdict", "BUY"),
            })
        action_plan = action_plan[:15]

        # ── Sector heatmap with news tags ──
        sector_heatmap = []
        sec_scores: dict = {}
        for r in results:
            sec = r.get("sector", "Other")
            sec_scores.setdefault(sec, []).append(r.get("score", 0))
        for sec, scores in sec_scores.items():
            if not scores:
                continue
            avg = sum(scores) / len(scores)
            # News attributed to this sector
            sec_news = [n for n in news_items if sec in (n.get("matched_sectors") or [])]
            sector_heatmap.append({
                "sector": sec, "avg_score": round(avg, 1), "count": len(scores),
                "trend": "up" if avg >= 58 else "down" if avg < 45 else "flat",
                "news_count": len(sec_news),
                "news_label": "BULLISH" if sum(n.get("sentiment_score", 0) for n in sec_news) > 2
                              else "BEARISH" if sum(n.get("sentiment_score", 0) for n in sec_news) < -2
                              else "NEUTRAL",
            })
        sector_heatmap.sort(key=lambda x: -x["avg_score"])

        # ── Movers ──
        movers_up = sorted(results, key=lambda r: -(r.get("change_pct", 0)))[:5]
        movers_down = sorted(results, key=lambda r: r.get("change_pct", 0))[:5]

        def mover_dict(r):
            return {
                "symbol": r["symbol"], "name": r.get("name", ""),
                "change_pct": round(r.get("change_pct", 0), 2),
                "price": r.get("current_price"), "action": r.get("_action"),
                "score": r.get("score", 0),
            }

        # ── News highlights (top-weighted items) ──
        news_highlights = []
        scored_news = sorted(news_items,
                             key=lambda n: -abs(n.get("sentiment_score", 0)) * n.get("weight", 1))[:12]
        for n in scored_news:
            news_highlights.append({
                "title":     n.get("title"),
                "source":    n.get("source"),
                "category":  n.get("category"),
                "sentiment": n.get("sentiment"),
                "tickers":   n.get("matched_tickers", []),
                "sectors":   n.get("matched_sectors", []),
                "published": n.get("published_iso"),
                "link":      n.get("link"),
            })

        return {
            "generated_at": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "source": "rules_engine_v4_news_fusion",
            "headline": headline,
            "regime": {
                "label": label, "breadth_pct": breadth,
                "momentum_pct": regime.get("momentum_pct", 0),
                "volatility": regime.get("volatility", 0),
                "notes": regime.get("notes", ""),
            },
            "macro_context": {
                "label": macro_label,
                "score": macro_sentiment.get("score", 0),
                "count": macro_sentiment.get("count", 0),
                "highlights": macro_sentiment.get("highlights", []),
            },
            "key_insight": key_insight,
            "narrative": narrative,
            "market_pulse": {
                "total_analyzed": len(results),
                "strong_buys": sum(1 for r in results if r.get("signal") == "STRONG BUY"),
                "actionable_buys": adds + wfds,
                "holds": holds,
                "risk_alerts": exits,
                "earnings_blackouts": 0,
                "do_not_trade": 0,
            },
            "top_movers_up": [mover_dict(r) for r in movers_up],
            "top_movers_down": [mover_dict(r) for r in movers_down],
            "conviction_board": conviction,
            "risk_watchlist": risk_watchlist,
            "breakout_watch": breakout_watch,
            "sector_heatmap": sector_heatmap[:12],
            "action_plan": action_plan,
            "news_highlights": news_highlights,
            "news_stats": {
                "total_items": len(news_items),
                "sources": len(set(n.get("source", "") for n in news_items)),
                "with_tickers": sum(1 for n in news_items if n.get("matched_tickers")),
                "bullish": sum(1 for n in news_items if n.get("sentiment") in ("BULLISH", "MILDLY_BULLISH")),
                "bearish": sum(1 for n in news_items if n.get("sentiment") in ("BEARISH", "MILDLY_BEARISH")),
            },
        }
    except Exception as e:
        import traceback
        print(f"✗ build_rich_daily_brief failed: {e}")
        traceback.print_exc()
        return {
            "generated_at": datetime.now(IST).isoformat(),
            "source": "degraded",
            "headline": "End-of-day brief degraded",
            "regime": regime, "narrative": [], "key_insight": str(e)[:200],
            "market_pulse": {"total_analyzed": len(results)},
            "conviction_board": [], "risk_watchlist": [], "breakout_watch": [],
            "sector_heatmap": [], "action_plan": [], "news_highlights": [],
            "top_movers_up": [], "top_movers_down": [],
            "macro_context": macro_sentiment or {},
        }


# ───────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ───────────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(IST)
    print(f"═══════════════════════════════════════════════════════════")
    print(f"  StockSage/ProfitPilot Daily Pipeline v5")
    print(f"  {now.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"═══════════════════════════════════════════════════════════")

    # ── STEP 1: Fetch RSS news in parallel ──
    print("\n[1/5] Fetching RSS news...")
    t0 = time.time()
    raw_news = fetch_all_news(max_workers=10)
    news_items = analyze(raw_news)
    print(f"      → {len(raw_news)} raw items, {len(news_items)} after dedupe ({time.time()-t0:.1f}s)")

    # Save news_cache.json (commit by workflow)
    try:
        out_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cache_path = os.path.join(out_dir, "news_cache.json")
        # Keep only recent items, max 300
        cache_items = sorted(news_items, key=lambda x: -x.get("published_ts", 0))[:300]
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": now.isoformat(),
                "count": len(cache_items),
                "items": cache_items,
            }, f, indent=2, default=str)
        print(f"      → news_cache.json saved ({len(cache_items)} items)")
    except Exception as e:
        print(f"      ⚠ news_cache.json write failed: {e}")

    macro_sentiment = aggregate_macro_sentiment(news_items)
    print(f"      → macro tone: {macro_sentiment.get('label')}")

    # ── STEP 2: Download price data ──
    print(f"\n[2/5] Downloading price data for {len(STOCKS)} stocks...")
    df_all = fetch_prices_resilient(list(STOCKS.keys()))

    # ── STEP 3: Score stocks ──
    print(f"\n[3/5] Scoring stocks...")
    results = []
    if df_all is not None and not df_all.empty:
        for ticker, (name, sector) in STOCKS.items():
            try:
                if isinstance(df_all.columns, pd.MultiIndex):
                    if ticker not in df_all.columns.get_level_values(0):
                        continue
                    df = df_all[ticker].dropna(subset=["Close"]).copy()
                else:
                    df = df_all.dropna(subset=["Close"]).copy()
                if len(df) < 30:
                    continue
                r = score_stock(ticker, df)
                if r:
                    r["name"] = name
                    r["sector"] = sector
                    results.append(r)
            except Exception as e:
                print(f"  ✗ {ticker}: {e}")

    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"      → {len(results)} stocks scored")

    # ── PRESERVE-LAST-GOOD: if this run produced zero usable stocks,
    #    do NOT overwrite predictions.json with a degraded payload.
    #    Instead, refresh the existing file's news + timestamps and mark _stale.
    if not results:
        prev = load_previous_predictions()
        if prev is not None:
            print("      ⚠ 0 stocks scored — preserving last-good predictions.json with refreshed news.")
            prev["_stale"] = True
            prev["_stale_reason"] = "Yahoo Finance fetch returned no usable data in this run"
            prev["_last_refresh_attempt"] = now.isoformat()
            # Refresh only the parts that don't depend on prices
            if "daily_brief" in prev and isinstance(prev["daily_brief"], dict):
                prev["daily_brief"]["macro_context"] = macro_sentiment
                prev["daily_brief"]["_last_refresh_attempt"] = now.isoformat()
                prev["daily_brief"]["_stale"] = True
                prev["daily_brief"]["_previous_market_date"] = prev.get("market_date")
            out_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "predictions.json",
            )
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(prev, f, indent=2, default=str)
            print(f"✅ predictions.json preserved (last good: {prev.get('market_date')})")
            return
        else:
            print("      ⚠ 0 stocks scored AND no prior predictions.json — writing degraded payload.")
            # Fall through to normal empty-brief path below

    # ── STEP 4: Detect regime, enrich with news + forecasts + narratives ──
    print(f"\n[4/5] Regime detection + news fusion + forecasts + narratives...")
    regime = detect_regime(results)
    print(f"      → regime: {regime['label']} ({regime['breadth_pct']}% breadth)")

    # Sector momentum (needed by narrator)
    sec_b: dict = {}
    for r in results:
        sec_b.setdefault(r["sector"], []).append(r["score"])
    sector_momentum = sorted([
        {"sector": s, "score": int(sum(v) / len(v)),
         "trend": "up" if sum(v) / len(v) >= 60 else "down" if sum(v) / len(v) < 45 else "neutral"}
        for s, v in sec_b.items()
    ], key=lambda x: -x["score"])

    for r in results:
        # News aggregation for this ticker
        news_info = aggregate_ticker_sentiment(news_items, r["symbol"])
        r["_news_info"] = news_info

        # Apply news modifier to score (bounded)
        original_score = r["score"]
        r["score"] = max(0, min(100, r["score"] + news_info.get("modifier", 0)))
        r["_score_pre_news"] = original_score

        # Re-derive signal if news changed the score band
        if r["score"] >= 72: r["signal"], r["confidence"] = "STRONG BUY", "HIGH"
        elif r["score"] >= 55: r["signal"], r["confidence"] = "BUY", "HIGH" if r["score"] >= 63 else "MEDIUM"
        elif r["score"] >= 40: r["signal"], r["confidence"] = "WATCH", "MEDIUM"
        else: r["signal"], r["confidence"] = "AVOID", "LOW"

        # Verdict + action
        action, verdict = verdict_from_signal(r["signal"], r["score"], news_info.get("label", "NEUTRAL"))
        r["_action"] = action
        r["_verdict"] = verdict
        r["verdict"] = verdict

        # Three-horizon forecasts
        tech_meta = r.get("_tech_meta", {})
        tech_meta["score"] = r["score"]  # use post-news score
        forecasts = forecast_stock(
            tech=tech_meta, price=r["current_price"], atr=r.get("_atr", 0),
            news_info=news_info, regime=regime, sector=r["sector"]
        )
        r["_forecasts"] = forecasts
        r["forecasts"] = forecasts  # expose in JSON

        # Advisor narrative
        narrative = build_narrative(
            symbol=r["symbol"], name=r.get("name", ""), sector=r["sector"],
            tech=tech_meta, price=r["current_price"], change_pct=r.get("change_pct", 0),
            action=action, verdict=verdict,
            news_info=news_info, regime=regime, sector_momentum=sector_momentum,
            forecasts=forecasts, entry_ceiling=r.get("_entry_ceiling", r["current_price"]),
        )
        r["_narrative"] = narrative
        r["narrative"] = narrative  # expose in JSON

    # Re-sort after news adjustment
    results.sort(key=lambda x: x["score"], reverse=True)

    # ── Force top 5 per category ──
    used = set()
    def top5(cat):
        picks = [r for r in results
                 if r["holding_category"] == cat and r["signal"] in ("STRONG BUY", "BUY")]
        used.update(r["symbol"] for r in picks[:5])
        if len(picks) < 5:
            extras = [r for r in results
                      if r["symbol"] not in used and r["signal"] in ("STRONG BUY", "BUY", "WATCH")]
            for r in extras:
                if len(picks) >= 5:
                    break
                rc = dict(r); rc["holding_category"] = cat
                if cat == "short":
                    rc["trade_plan"]["entry"]["entry_window"] = "09:15–09:45 AM IST"
                    rc["trade_plan"]["exit"]["hold_duration"] = "5–15 trading days"
                elif cat == "medium":
                    rc["trade_plan"]["entry"]["entry_window"] = "09:15–10:15 AM IST"
                    rc["trade_plan"]["exit"]["hold_duration"] = "4–12 weeks"
                else:
                    rc["trade_plan"]["entry"]["entry_window"] = "09:15 AM IST (GTC)"
                    rc["trade_plan"]["exit"]["hold_duration"] = "6–18 months"
                picks.append(rc); used.add(rc["symbol"])
        for i, r in enumerate(picks[:5]):
            r["rank"] = i + 1
        return picks[:5]

    short_picks = top5("short")
    medium_picks = top5("medium")
    long_picks = top5("long")
    top_picks = results[:15]
    for i, r in enumerate(top_picks):
        r["rank"] = i + 1

    # ── STEP 5: Build rich daily brief ──
    print(f"\n[5/5] Building rich daily brief...")
    if not results:
        print("      ⚠ No stocks scored — emitting minimal brief, frontend will show 'not ready'")
        daily_brief = {
            "generated_at": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "source": "rules_engine_v5_degraded",
            "headline": "Data fetch failed — no picks available",
            "regime": regime,
            "macro_context": macro_sentiment,
            "key_insight": (
                "Yahoo Finance did not return usable price data for any of the "
                f"{len(STOCKS)} tracked stocks in this run. The frontend will show "
                "a 'brief not ready' state until the next successful workflow run."
            ),
            "narrative": [
                f"Attempted to score {len(STOCKS)} stocks, got 0 results",
                f"News layer OK: {len(news_items)} items ingested",
                "Retry: GitHub → Actions → Daily Stock Analysis → Run workflow",
            ],
            "market_pulse": {
                "total_analyzed": 0, "strong_buys": 0, "actionable_buys": 0,
                "holds": 0, "risk_alerts": 0, "earnings_blackouts": 0, "do_not_trade": 0,
            },
            "top_movers_up": [], "top_movers_down": [],
            "conviction_board": [], "risk_watchlist": [],
            "breakout_watch": [], "sector_heatmap": [], "action_plan": [],
            "news_highlights": [],
        }
    else:
        daily_brief = build_rich_daily_brief(results, regime, news_items, macro_sentiment, sector_momentum)
    print(f"      → headline: {daily_brief.get('headline', '')[:80]}")
    print(f"      → conviction: {len(daily_brief.get('conviction_board', []))}")
    print(f"      → risk watch: {len(daily_brief.get('risk_watchlist', []))}")
    print(f"      → news highlights: {len(daily_brief.get('news_highlights', []))}")

    # Market breadth
    adv = sum(1 for r in results if r["change_pct"] > 0.25)
    dec = sum(1 for r in results if r["change_pct"] < -0.25)
    k = 3500 / max(len(results), 1)

    # Strip internal underscore fields before writing JSON to keep payload lean
    def clean_result(r):
        return {k: v for k, v in r.items() if not k.startswith("_")}

    output = {
        "generated_at": now.isoformat(),
        "market_date": now.strftime("%Y-%m-%d"),
        "analysis_version": "5.0-rules+news+forecast+narrative",
        "engine": "rules_engine_v5_news_fusion",
        "stocks_analyzed": len(results),
        "regime": regime,
        "indices": {
            "NIFTY50": {"value": 0, "change": 0, "change_pct": 0},
            "SENSEX": {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_BANK": {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_IT": {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_MIDCAP100": {"value": 0, "change": 0, "change_pct": 0},
        },
        "market_breadth": {
            "advances": int(adv * k), "declines": int(dec * k),
            "unchanged": int((len(results) - adv - dec) * k),
            "new_52w_high": 0, "new_52w_low": 0,
        },
        "short_term": {"label": "Short Term (5–15 days)", "picks": [clean_result(r) for r in short_picks]},
        "medium_term": {"label": "Medium Term (4–12 weeks)", "picks": [clean_result(r) for r in medium_picks]},
        "long_term": {"label": "Long Term (6–18 months)", "picks": [clean_result(r) for r in long_picks]},
        "top_picks": [clean_result(r) for r in top_picks],
        "watchlist": [{"symbol": r["symbol"], "name": r.get("name", ""), "sector": r.get("sector", ""),
                       "current_price": r.get("current_price"), "score": r.get("score", 0),
                       "signal": "WATCH",
                       "reason": r.get("reasons", ["Monitor"])[0] if r.get("reasons") else "Monitor"}
                      for r in results if r["signal"] == "WATCH"][:5],
        "avoid": [{"symbol": r["symbol"], "name": r.get("name", ""), "sector": r.get("sector", ""),
                   "current_price": r.get("current_price"), "score": r.get("score", 0),
                   "reason": r.get("reasons", ["Bearish"])[0] if r.get("reasons") else "Bearish"}
                  for r in results if r["signal"] == "AVOID"][:3],
        "sector_momentum": sector_momentum[:12],
        "daily_brief": daily_brief,
    }

    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "predictions.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n✅ predictions.json saved")
    print(f"   Short term : {len(short_picks)} picks")
    print(f"   Medium term: {len(medium_picks)} picks")
    print(f"   Long term  : {len(long_picks)} picks")
    print(f"   News items : {len(news_items)} · Macro: {macro_sentiment.get('label')}")


if __name__ == "__main__":
    main()
