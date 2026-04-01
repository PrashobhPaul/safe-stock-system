"""
analyze.py — StockSage India
==============================
Rule-based stock scoring engine. Replaces gpt_analyze.py.
Reads OHLCV data from stock_data.db (built by data_fetch.py),
applies 20+ technical indicators across 5 scoring categories,
and outputs predictions.json for the static HTML dashboard.

Scoring categories (total 0–100):
  Trend Analysis  : 0–30 pts
  Momentum        : 0–25 pts
  Volume          : 0–20 pts
  Breakout        : 0–15 pts
  Price Action    : 0–10 pts

No OpenAI API key needed. Zero paid services.
"""

import json
import sqlite3
import logging
from datetime import datetime, date
import pytz

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("analyze")

IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────────────────────
# METADATA — Names and Sectors
# ─────────────────────────────────────────────────────────────

STOCK_META = {
    "RELIANCE.NS":   ("Reliance Industries Ltd",        "Conglomerate"),
    "TCS.NS":        ("Tata Consultancy Services",       "IT"),
    "INFY.NS":       ("Infosys Ltd",                     "IT"),
    "ICICIBANK.NS":  ("ICICI Bank Ltd",                  "Banking"),
    "HDFCBANK.NS":   ("HDFC Bank Ltd",                   "Banking"),
    "KOTAKBANK.NS":  ("Kotak Mahindra Bank",             "Banking"),
    "LT.NS":         ("Larsen & Toubro Ltd",             "Infrastructure"),
    "SBIN.NS":       ("State Bank of India",             "Banking"),
    "AXISBANK.NS":   ("Axis Bank Ltd",                   "Banking"),
    "BAJFINANCE.NS": ("Bajaj Finance Ltd",               "NBFC"),
    "TATAMOTORS.NS": ("Tata Motors Ltd",                 "Automobiles"),
    "TATASTEEL.NS":  ("Tata Steel Ltd",                  "Metals"),
    "HINDUNILVR.NS": ("Hindustan Unilever Ltd",          "FMCG"),
    "ITC.NS":        ("ITC Ltd",                         "FMCG"),
    "WIPRO.NS":      ("Wipro Ltd",                       "IT"),
    "TECHM.NS":      ("Tech Mahindra Ltd",               "IT"),
    "NTPC.NS":       ("NTPC Ltd",                        "Power"),
    "POWERGRID.NS":  ("Power Grid Corp",                 "Power"),
    "ONGC.NS":       ("Oil & Natural Gas Corp",          "Oil & Gas"),
    "COALINDIA.NS":  ("Coal India Ltd",                  "Mining"),
    "BHARTIARTL.NS": ("Bharti Airtel Ltd",               "Telecom"),
    "ADANIPORTS.NS": ("Adani Ports & SEZ",               "Infrastructure"),
    "ULTRACEMCO.NS": ("UltraTech Cement",                "Cement"),
    "GRASIM.NS":     ("Grasim Industries",               "Cement"),
    "NESTLEIND.NS":  ("Nestle India Ltd",                "FMCG"),
    "MARUTI.NS":     ("Maruti Suzuki India",             "Automobiles"),
    "ASIANPAINT.NS": ("Asian Paints Ltd",                "Paints"),
    "SUNPHARMA.NS":  ("Sun Pharmaceutical Industries",   "Pharma"),
    "CIPLA.NS":      ("Cipla Ltd",                       "Pharma"),
    "DRREDDY.NS":    ("Dr. Reddy's Laboratories",        "Pharma"),
    "BAJAJ-AUTO.NS": ("Bajaj Auto Ltd",                  "Automobiles"),
    "HEROMOTOCO.NS": ("Hero MotoCorp Ltd",               "Automobiles"),
    "EICHERMOT.NS":  ("Eicher Motors Ltd",               "Automobiles"),
    "HCLTECH.NS":    ("HCL Technologies Ltd",            "IT"),
    "DIVISLAB.NS":   ("Divi's Laboratories",             "Pharma"),
    "SBILIFE.NS":    ("SBI Life Insurance",              "Insurance"),
    "HDFCLIFE.NS":   ("HDFC Life Insurance",             "Insurance"),
    "ICICIPRULI.NS": ("ICICI Prudential Life",           "Insurance"),
    "BRITANNIA.NS":  ("Britannia Industries",            "FMCG"),
    "SHREECEM.NS":   ("Shree Cement Ltd",                "Cement"),
    "TRENT.NS":      ("Trent Ltd",                       "Retail"),
    "TATAELXSI.NS":  ("Tata Elxsi Ltd",                  "IT"),
    "JSWENERGY.NS":  ("JSW Energy Ltd",                  "Power"),
    "TVSMOTOR.NS":   ("TVS Motor Company",               "Automobiles"),
    "ZOMATO.NS":     ("Zomato Ltd",                      "Internet"),
    "NHPC.NS":       ("NHPC Ltd",                        "Power"),
    "DIXON.NS":      ("Dixon Technologies",              "Electronics"),
    "TITAN.NS":      ("Titan Company Ltd",               "Consumer Disc."),
    "BAJAJFINSV.NS": ("Bajaj Finserv Ltd",               "NBFC"),
    "M&M.NS":        ("Mahindra & Mahindra",             "Automobiles"),
    "APOLLOHOSP.NS": ("Apollo Hospitals Enterprise",     "Healthcare"),
    "TATACONSUM.NS": ("Tata Consumer Products",          "FMCG"),
    "JSWSTEEL.NS":   ("JSW Steel Ltd",                   "Metals"),
    "BPCL.NS":       ("Bharat Petroleum Corp",           "Oil & Gas"),
    "INDUSINDBK.NS": ("IndusInd Bank Ltd",               "Banking"),
    "HINDALCO.NS":   ("Hindalco Industries",             "Metals"),
    "VEDL.NS":       ("Vedanta Ltd",                     "Metals"),
    "PFC.NS":        ("Power Finance Corp",              "NBFC"),
    "RECLTD.NS":     ("REC Ltd",                         "NBFC"),
    "HAL.NS":        ("Hindustan Aeronautics",           "Defence"),
    "BEL.NS":        ("Bharat Electronics",              "Defence"),
    "IRCTC.NS":      ("Indian Railway Catering",         "Travel"),
    "NAUKRI.NS":     ("Info Edge India",                 "Internet"),
    "PIDILITIND.NS": ("Pidilite Industries",             "Chemicals"),
    "HAVELLS.NS":    ("Havells India",                   "Electricals"),
    "LTIM.NS":       ("LTIMindtree Ltd",                 "IT"),
    "PERSISTENT.NS": ("Persistent Systems",              "IT"),
    "COFORGE.NS":    ("Coforge Ltd",                     "IT"),
    "POLYCAB.NS":    ("Polycab India",                   "Electricals"),
    "MUTHOOTFIN.NS": ("Muthoot Finance",                 "NBFC"),
    "CHOLAFIN.NS":   ("Cholamandalam Investment",        "NBFC"),
    "BANKBARODA.NS": ("Bank of Baroda",                  "Banking"),
    "FEDERALBNK.NS": ("Federal Bank Ltd",               "Banking"),
    "IDFCFIRSTB.NS": ("IDFC First Bank",                 "Banking"),
    "DABUR.NS":      ("Dabur India Ltd",                 "FMCG"),
    "MARICO.NS":     ("Marico Ltd",                      "FMCG"),
    "GODREJCP.NS":   ("Godrej Consumer Products",        "FMCG"),
    "COLPAL.NS":     ("Colgate-Palmolive India",         "FMCG"),
    "AMBUJACEM.NS":  ("Ambuja Cements",                  "Cement"),
    "ACC.NS":        ("ACC Ltd",                         "Cement"),
    "BERGEPAINT.NS": ("Berger Paints India",             "Paints"),
    "AUROPHARMA.NS": ("Aurobindo Pharma",                "Pharma"),
    "LUPIN.NS":      ("Lupin Ltd",                       "Pharma"),
}


# ─────────────────────────────────────────────────────────────
# TECHNICAL INDICATORS
# ─────────────────────────────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calc_bollinger(close: pd.Series, period=20, std_dev=2):
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    width = (upper - lower) / sma.replace(0, np.nan)
    position = (close - lower) / (upper - lower).replace(0, np.nan)
    return sma, upper, lower, width, position


def calc_stochastic(df: pd.DataFrame, k_period=14, d_period=3):
    low_min = df["low"].rolling(k_period).min()
    high_max = df["high"].rolling(k_period).max()
    denom = (high_max - low_min).replace(0, np.nan)
    k = 100 * (df["close"] - low_min) / denom
    d = k.rolling(d_period).mean()
    return k, d


def calc_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def calc_atr(df: pd.DataFrame, period=14) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def calc_adx(df: pd.DataFrame, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
    hl = high - low
    hc = (high - close.shift()).abs()
    lc = (low - close.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr.replace(0, np.nan))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.rolling(period).mean()
    return adx, plus_di, minus_di


def calc_roc(close: pd.Series, period=10) -> pd.Series:
    return ((close - close.shift(period)) / close.shift(period).replace(0, np.nan)) * 100


# ─────────────────────────────────────────────────────────────
# SCORING ENGINE
# ─────────────────────────────────────────────────────────────

def score_stock(ticker: str, df: pd.DataFrame) -> dict | None:
    """
    Core scoring function. Applies 5-category rule-based scoring.
    df columns expected: date, open, high, low, close, volume (lowercase)
    """
    if len(df) < 60:
        return None

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    open_ = df["open"]

    reasons = []

    # ── Moving Averages ──
    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean() if len(df) >= 200 else pd.Series([np.nan] * len(df))
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    # ── Indicators ──
    rsi = calc_rsi(close)
    macd_line, signal_line, histogram = calc_macd(close)
    _, bb_upper, bb_lower, bb_width, bb_pos = calc_bollinger(close)
    stoch_k, stoch_d = calc_stochastic(df)
    obv = calc_obv(close, volume)
    atr = calc_atr(df)
    adx, plus_di, minus_di = calc_adx(df)
    roc = calc_roc(close)

    # Current values
    c         = close.iloc[-1]
    c_open    = open_.iloc[-1]
    c_sma20   = sma20.iloc[-1]
    c_sma50   = sma50.iloc[-1]
    c_sma200  = sma200.iloc[-1]
    c_ema12   = ema12.iloc[-1]
    c_ema26   = ema26.iloc[-1]
    c_ema50   = ema50.iloc[-1]
    c_rsi     = rsi.iloc[-1]
    c_macd    = macd_line.iloc[-1]
    c_signal  = signal_line.iloc[-1]
    c_hist    = histogram.iloc[-1]
    c_hist_p  = histogram.iloc[-2] if len(histogram) > 1 else 0
    c_bb_pos  = bb_pos.iloc[-1]
    c_bb_w    = bb_width.iloc[-1]
    c_stk_k   = stoch_k.iloc[-1]
    c_stk_d   = stoch_d.iloc[-1]
    c_adx     = adx.iloc[-1] if not np.isnan(adx.iloc[-1]) else 15.0
    c_roc     = roc.iloc[-1]
    c_atr     = atr.iloc[-1]

    vol_avg = volume.rolling(20).mean().iloc[-1]
    vol_ratio = (volume.iloc[-1] / vol_avg) if vol_avg > 0 else 1.0

    # ── 1. TREND (0–30) ──
    trend = 0
    has_200 = not np.isnan(c_sma200)

    if has_200:
        if c > c_sma20 and c_sma20 > c_sma50 and c_sma50 > c_sma200:
            trend += 12
            reasons.append("Full bullish SMA alignment (price > 20 > 50 > 200 SMA)")
        elif c > c_sma50 and c_sma50 > c_sma200:
            trend += 8
            reasons.append("Bullish SMA alignment (price > 50 > 200 SMA)")
        elif c > c_sma200:
            trend += 4

        pct_above_200 = ((c - c_sma200) / c_sma200) * 100
        if 5 <= pct_above_200 <= 25:
            trend += 4
        elif 0 < pct_above_200 < 5:
            trend += 2
    else:
        if c > c_sma20 and c_sma20 > c_sma50:
            trend += 8

    if c_ema12 > c_ema26:
        # Check fresh crossover within last 5 sessions
        fresh = any(
            ema12.iloc[-(i+1)] <= ema26.iloc[-(i+1)]
            for i in range(1, min(6, len(ema12)))
        )
        if fresh:
            trend += 6
            reasons.append("Fresh EMA 12/26 bullish crossover")
        else:
            trend += 3

    if c > c_ema50:
        trend += 2

    if not np.isnan(c_adx):
        if c_adx >= 35:   trend += 5; reasons.append(f"Strong trend (ADX {c_adx:.0f})")
        elif c_adx >= 25: trend += 3
        elif c_adx >= 20: trend += 1

    if not np.isnan(plus_di.iloc[-1]) and plus_di.iloc[-1] > minus_di.iloc[-1]:
        trend += 2

    trend = min(trend, 30)

    # ── 2. MOMENTUM (0–25) ──
    mom = 0
    if not np.isnan(c_rsi):
        if   50 <= c_rsi <= 65: mom += 12; reasons.append(f"RSI at optimal buy zone ({c_rsi:.1f})")
        elif 45 <= c_rsi < 50:  mom += 7
        elif 65 < c_rsi <= 70:  mom += 5
        elif 40 <= c_rsi < 45:  mom += 3
        elif 30 <= c_rsi < 40:  mom += 2

    if c_macd > c_signal:
        mom += 5
        # Zero-line crossover (strong signal)
        prev_macd = macd_line.iloc[-2] if len(macd_line) > 1 else c_macd
        if c_macd > 0 and prev_macd <= 0:
            mom += 4
            reasons.append("MACD crossed zero line — strong bull signal")
        else:
            reasons.append("MACD bullish (above signal line)")

    if c_hist > 0 and c_hist > c_hist_p:
        mom += 4
        reasons.append("MACD histogram expanding")
    elif c_hist > 0:
        mom += 1

    if not np.isnan(c_stk_k) and 40 <= c_stk_k <= 80 and c_stk_k > c_stk_d:
        mom += 3
        if c_stk_k < 55:
            mom += 1

    if not np.isnan(c_roc) and c_roc > 3:
        mom += 2

    mom = min(mom, 25)

    # ── 3. VOLUME (0–20) ──
    vol = 0
    if   vol_ratio >= 2.5: vol += 10; reasons.append(f"Volume surge {vol_ratio:.1f}x avg — institutional activity")
    elif vol_ratio >= 2.0: vol += 8;  reasons.append(f"High volume {vol_ratio:.1f}x avg")
    elif vol_ratio >= 1.5: vol += 6;  reasons.append(f"Above-average volume {vol_ratio:.1f}x avg")
    elif vol_ratio >= 1.2: vol += 3
    elif vol_ratio < 0.7:  vol -= 2

    if len(obv) >= 10:
        obv_slope = (obv.iloc[-1] - obv.iloc[-6]) / 5
        obv_prev  = (obv.iloc[-6] - obv.iloc[-11]) / 5
        if obv_slope > 0:
            vol += 4
            reasons.append("OBV trending up — accumulation detected")
        if obv_slope > obv_prev:
            vol += 2

    if len(close) >= 6 and len(volume) >= 6:
        price_up = close.iloc[-1] > close.iloc[-4]
        vol_up   = volume.iloc[-3:].mean() > volume.iloc[-6:-3].mean()
        if price_up and vol_up:
            vol += 4
            reasons.append("Price and volume both rising over 3 days")

    vol = min(max(vol, 0), 20)

    # ── 4. BREAKOUT (0–15) ──
    brk = 0
    if not np.isnan(c_bb_pos):
        if   c_bb_pos >= 0.8: brk += 5; reasons.append("Price near upper Bollinger Band")
        elif c_bb_pos >= 0.6: brk += 3
        elif c_bb_pos < 0.2:  brk -= 2

    if len(bb_width) >= 25:
        avg_w = bb_width.rolling(20).mean().iloc[-1]
        if not np.isnan(avg_w) and c_bb_w < avg_w * 0.80:
            brk += 4
            reasons.append("Bollinger Band squeeze — breakout likely")

    if len(high) >= 21:
        high_20 = high.iloc[-21:-1].max()
        if c > high_20:
            brk += 5
            reasons.append("Breaking above 20-day high")
        elif len(high) >= 11 and c > high.iloc[-11:-1].max():
            brk += 2

    if len(high) >= 252 and c > high.iloc[-252:-1].max():
        brk += 5
        reasons.append("New 52-week high!")

    brk = min(max(brk, 0), 15)

    # ── 5. PRICE ACTION (0–10) ──
    pa = 0
    if len(close) >= 252:
        w52h = high.iloc[-252:].max()
        pct_from_high = ((w52h - c) / w52h) * 100
        if   pct_from_high <= 3:  pa += 5; reasons.append(f"Near 52-week high ({pct_from_high:.1f}% away)")
        elif pct_from_high <= 8:  pa += 3
        elif pct_from_high <= 15: pa += 1

    if len(close) >= 6:
        ret5 = ((c - close.iloc[-6]) / close.iloc[-6]) * 100
        if   ret5 >= 5:  pa += 3; reasons.append(f"Strong 5-day momentum (+{ret5:.1f}%)")
        elif ret5 >= 2:  pa += 2
        elif ret5 >= 0:  pa += 1

    if c > c_open:
        pa += 2 if ((c - c_open) / c_open) * 100 >= 1.5 else 1

    if len(open_) >= 2 and c_open > close.iloc[-2] * 1.01:
        pa += 2
        reasons.append("Gap-up opening today")

    pa = min(pa, 10)

    # ── PENALTIES ──
    penalty = 0
    if not np.isnan(c_rsi) and c_rsi > 78:
        penalty += 12
    if has_200 and c_sma50 < c_sma200:
        penalty += 8
    if has_200 and c < c_sma200:
        penalty += 6
    if len(close) >= 10:
        price_up_5 = c > close.iloc[-6]
        vol_down_5 = volume.iloc[-5:].mean() < volume.iloc[-10:-5].mean() * 0.85
        if price_up_5 and vol_down_5:
            penalty += 4

    # ── TOTAL ──
    total = max(0, min(100, trend + mom + vol + brk + pa - penalty))

    # Signal thresholds
    if   total >= 75: signal, confidence = "STRONG BUY", "HIGH"
    elif total >= 60: signal, confidence = "BUY",         "HIGH" if total >= 67 else "MEDIUM"
    elif total >= 45: signal, confidence = "WATCH",       "MEDIUM"
    else:             signal, confidence = "AVOID",       "LOW"

    # Target / Stop via ATR
    if not np.isnan(c_atr) and c_atr > 0:
        target    = round(c + c_atr * 2.5, 2)
        stop_loss = round(c - c_atr * 1.2, 2)
        risk      = c - stop_loss
        reward    = target - c
        rr        = round(reward / risk, 1) if risk > 0 else 0.0
    else:
        pct = 0.07 if total >= 75 else 0.05
        target    = round(c * (1 + pct), 2)
        stop_loss = round(c * 0.96, 2)
        rr        = round(pct / 0.04, 1)

    # MACD string
    prev_m = macd_line.iloc[-2] if len(macd_line) > 1 else c_macd
    prev_s = signal_line.iloc[-2] if len(signal_line) > 1 else c_signal
    if   c_macd > c_signal and prev_m <= prev_s: macd_str = "Bullish Crossover"
    elif c_macd > c_signal:                       macd_str = "Bullish"
    elif c_macd < c_signal:                       macd_str = "Bearish"
    else:                                          macd_str = "Neutral"

    # SMA alignment string
    if has_200:
        if c > c_sma20 > c_sma50 > c_sma200: sma_str = "Full Bull (Price > 20 > 50 > 200)"
        elif c > c_sma50 > c_sma200:          sma_str = "Bull (Price > 50 > 200 SMA)"
        elif c > c_sma200:                     sma_str = "Above 200 SMA"
        elif c > c_sma50:                      sma_str = "Above 50 SMA only"
        else:                                  sma_str = "Bearish"
    else:
        sma_str = "Bullish" if c > c_sma50 else "Bearish"

    # 52W high %
    w52_pct = None
    if len(high) >= 252:
        w52h = high.iloc[-252:].max()
        w52_pct = round(((w52h - c) / w52h) * 100, 1)

    # 1-day change
    change_pct = round(((c - close.iloc[-2]) / close.iloc[-2]) * 100, 2) if len(close) >= 2 else 0.0
    change     = round(c - close.iloc[-2], 2) if len(close) >= 2 else 0.0

    return {
        "score":        int(total),
        "signal":       signal,
        "confidence":   confidence,
        "current_price": round(float(c), 2),
        "change_pct":   change_pct,
        "change":       change,
        "target_price": round(float(target), 2),
        "stop_loss":    round(float(stop_loss), 2),
        "risk_reward":  rr,
        "scores": {
            "trend":        int(trend),
            "momentum":     int(mom),
            "volume":       int(vol),
            "breakout":     int(brk),
            "price_action": int(pa),
        },
        "indicators": {
            "rsi":          round(float(c_rsi), 1) if not np.isnan(c_rsi) else None,
            "macd_signal":  macd_str,
            "sma_alignment": sma_str,
            "volume_ratio": round(float(vol_ratio), 2),
            "week52_pct":   w52_pct,
            "atr":          round(float(c_atr), 2) if not np.isnan(c_atr) else None,
            "bb_position":  round(float(c_bb_pos), 2) if not np.isnan(c_bb_pos) else None,
            "adx":          round(float(c_adx), 1) if not np.isnan(c_adx) else None,
            "stoch_k":      round(float(c_stk_k), 1) if not np.isnan(c_stk_k) else None,
        },
        "reasons":   reasons[:6],
        "penalty":   int(penalty),
    }


# ─────────────────────────────────────────────────────────────
# SECTOR AGGREGATION
# ─────────────────────────────────────────────────────────────

def aggregate_sectors(results: list) -> list:
    bucket: dict[str, list] = {}
    for r in results:
        s = r.get("sector", "Other")
        bucket.setdefault(s, []).append(r["score"])
    out = []
    for sector, scores in bucket.items():
        avg = sum(scores) / len(scores)
        out.append({
            "sector": sector,
            "score":  int(avg),
            "trend":  "up" if avg >= 62 else "down" if avg < 48 else "neutral",
            "stocks_analyzed": len(scores),
        })
    return sorted(out, key=lambda x: x["score"], reverse=True)


# ─────────────────────────────────────────────────────────────
# MARKET BREADTH ESTIMATE
# ─────────────────────────────────────────────────────────────

def estimate_breadth(all_results: list) -> dict:
    adv = sum(1 for r in all_results if r.get("change_pct", 0) > 0.25)
    dec = sum(1 for r in all_results if r.get("change_pct", 0) < -0.25)
    unch = len(all_results) - adv - dec
    # Scale to NSE-wide estimate
    k = 3500 / max(len(all_results), 1)
    h52 = sum(1 for r in all_results if (r.get("indicators", {}).get("week52_pct") or 100) < 2)
    l52 = sum(1 for r in all_results if r.get("score", 50) < 30)
    return {
        "advances":   int(adv * k),
        "declines":   int(dec * k),
        "unchanged":  int(unch * k),
        "new_52w_high": int(h52 * k * 0.3),
        "new_52w_low":  int(l52 * k * 0.15),
    }


# ─────────────────────────────────────────────────────────────
# INDEX DATA FROM DB
# ─────────────────────────────────────────────────────────────

def build_index_snapshot(conn: sqlite3.Connection) -> dict:
    """
    Approximate Nifty/Sensex values from weighted avg of our stock universe.
    (Actual index tickers require a separate yfinance fetch — done in data_fetch.py
     if you add them to STOCKS, e.g. "^NSEI".)
    Returns placeholder structure that index.html can display.
    """
    try:
        df = pd.read_sql(
            "SELECT stock, date, close FROM stock_prices WHERE date >= date('now','-5 days') ORDER BY date DESC",
            conn,
        )
        if df.empty:
            raise ValueError("No recent data")
        latest = df["date"].max()
        prev   = df[df["date"] < latest]["date"].max()

        curr_avg = df[df["date"] == latest]["close"].mean()
        prev_avg = df[df["date"] == prev]["close"].mean() if prev else curr_avg

        chg = curr_avg - prev_avg
        chg_pct = (chg / prev_avg * 100) if prev_avg else 0

        # Synthetic index values (representative)
        return {
            "NIFTY50":        {"value": 22500.0,   "change": round(chg * 0.8,  2), "change_pct": round(chg_pct * 0.8,  2)},
            "SENSEX":         {"value": 74100.0,   "change": round(chg * 2.6,  2), "change_pct": round(chg_pct * 0.8,  2)},
            "NIFTY_BANK":     {"value": 48700.0,   "change": round(chg * 1.2,  2), "change_pct": round(chg_pct * 0.9,  2)},
            "NIFTY_IT":       {"value": 35200.0,   "change": round(chg * 1.0,  2), "change_pct": round(chg_pct * 1.1,  2)},
            "NIFTY_MIDCAP100":{"value": 50300.0,   "change": round(chg * 0.9,  2), "change_pct": round(chg_pct * 0.95, 2)},
        }
    except Exception as e:
        log.warning(f"  Index snapshot: {e}")
        return {}


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("  StockSage India — Rule-Based Analysis Engine")
    log.info(f"  {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info("=" * 55)

    conn = sqlite3.connect("stock_data.db")

    # Load all stock data from DB
    log.info("\n[1/3] Loading stock data from stock_data.db...")
    df_all = pd.read_sql(
        "SELECT * FROM stock_prices ORDER BY stock, date",
        conn,
    )
    log.info(f"  Rows loaded: {len(df_all):,}")

    # Score each stock
    log.info(f"\n[2/3] Scoring {df_all['stock'].nunique()} stocks...")
    all_results = []

    for ticker in df_all["stock"].unique():
        df = df_all[df_all["stock"] == ticker].copy()
        df = df.sort_values("date").reset_index(drop=True)
        df = df.dropna(subset=["close"])

        result = score_stock(ticker, df)
        if result is None:
            log.warning(f"  {ticker}: insufficient data — skipped")
            continue

        name, sector = STOCK_META.get(ticker, (ticker.replace(".NS", ""), "Other"))
        result["ticker"] = ticker
        result["symbol"] = ticker.replace(".NS", "")
        result["name"]   = name
        result["sector"] = sector

        all_results.append(result)

        rsi_str = f"RSI={result['indicators']['rsi']}" if result['indicators']['rsi'] else "RSI=n/a"
        log.info(
            f"  {'✓':<3} {ticker:<22} Score:{result['score']:>3} "
            f"| {result['signal']:<12} | {rsi_str}"
        )

    log.info(f"\n  Scored: {len(all_results)} stocks")

    # Classify
    log.info("\n[3/3] Ranking and building output...")
    ranked = sorted(all_results, key=lambda x: x["score"], reverse=True)

    top_picks = []
    watchlist = []
    avoid     = []

    for r in ranked:
        if r["signal"] in ("STRONG BUY", "BUY") and len(top_picks) < 15:
            r["rank"] = len(top_picks) + 1
            top_picks.append(r)
        elif r["signal"] == "WATCH" and len(watchlist) < 8:
            watchlist.append({
                "symbol":        r["symbol"],
                "name":          r["name"],
                "sector":        r["sector"],
                "current_price": r["current_price"],
                "score":         r["score"],
                "signal":        "WATCH",
                "reason":        r["reasons"][0] if r["reasons"] else "Monitor for entry signal",
            })
        elif r["signal"] == "AVOID" and len(avoid) < 5:
            avoid.append({
                "symbol":        r["symbol"],
                "name":          r["name"],
                "sector":        r["sector"],
                "current_price": r["current_price"],
                "score":         r["score"],
                "reason":        r["reasons"][0] if r["reasons"] else "Bearish signals",
            })

    output = {
        "generated_at":     datetime.now(IST).isoformat(),
        "market_date":      date.today().strftime("%Y-%m-%d"),
        "analysis_version": "2.1",
        "stocks_analyzed":  len(all_results),
        "indices":          build_index_snapshot(conn),
        "market_breadth":   estimate_breadth(all_results),
        "top_picks":        top_picks,
        "watchlist":        watchlist,
        "avoid":            avoid,
        "sector_momentum":  aggregate_sectors(all_results)[:12],
    }

    conn.close()

    with open("predictions.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    log.info(f"\n✅ predictions.json written")
    log.info(f"   Top picks : {len(top_picks)}")
    log.info(f"   Watchlist : {len(watchlist)}")
    log.info(f"   Avoid     : {len(avoid)}")

    if top_picks:
        log.info("\n📈 Top 5:")
        for p in top_picks[:5]:
            log.info(f"   #{p['rank']} {p['symbol']:<14} {p['score']:>3}/100 | {p['signal']}")

    # Also write the legacy gpt_recommendation.json so any existing
    # tooling that depends on it doesn't break
    legacy = [
        {
            "stock name":       p["symbol"],
            "reason":           " | ".join(p["reasons"][:2]),
            "target buy price": p["target_price"],
            "score":            p["score"],
            "signal":           p["signal"],
        }
        for p in top_picks[:5]
    ]
    with open("gpt_recommendation.json", "w") as f:
        json.dump(legacy, f, indent=2)
    log.info("   gpt_recommendation.json (legacy) also updated")


if __name__ == "__main__":
    main()
