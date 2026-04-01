from http.server import BaseHTTPRequestHandler
import json
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

STOCKS = {
    "RELIANCE.NS": ("Reliance Industries", "Conglomerate"),
    "TCS.NS":      ("Tata Consultancy Services", "IT"),
    "HDFCBANK.NS": ("HDFC Bank", "Banking"),
    "INFY.NS":     ("Infosys", "IT"),
    "ICICIBANK.NS":("ICICI Bank", "Banking"),
    "WIPRO.NS":    ("Wipro", "IT"),
    "HCLTECH.NS":  ("HCL Technologies", "IT"),
    "BAJFINANCE.NS":("Bajaj Finance", "NBFC"),
    "TITAN.NS":    ("Titan Company", "Consumer Disc."),
    "SUNPHARMA.NS":("Sun Pharmaceutical", "Pharma"),
    "MARUTI.NS":   ("Maruti Suzuki", "Automobiles"),
    "BHARTIARTL.NS":("Bharti Airtel", "Telecom"),
    "AXISBANK.NS": ("Axis Bank", "Banking"),
    "KOTAKBANK.NS":("Kotak Mahindra Bank", "Banking"),
    "LT.NS":       ("Larsen & Toubro", "Infrastructure"),
    "SBIN.NS":     ("State Bank of India", "Banking"),
    "NTPC.NS":     ("NTPC", "Power"),
    "APOLLOHOSP.NS":("Apollo Hospitals", "Healthcare"),
    "TRENT.NS":    ("Trent", "Retail"),
    "LTIM.NS":     ("LTIMindtree", "IT"),
    "ADANIPORTS.NS":("Adani Ports", "Infrastructure"),
    "HINDUNILVR.NS":("Hindustan Unilever", "FMCG"),
    "ITC.NS":      ("ITC", "FMCG"),
    "BAJAJFINSV.NS":("Bajaj Finserv", "NBFC"),
    "TATAMOTORS.NS":("Tata Motors", "Automobiles"),
}


def score_stock(ticker, df):
    try:
        c   = df["Close"]
        h   = df["High"]
        lo  = df["Low"]
        v   = df["Volume"]
        op  = df["Open"]

        close  = float(c.iloc[-1])
        prev   = float(c.iloc[-2]) if len(c) > 1 else close
        chg    = round((close - prev) / prev * 100, 2) if prev else 0

        sma20  = float(c.rolling(20).mean().iloc[-1])
        sma50  = float(c.rolling(50).mean().iloc[-1])
        sma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None

        ema12  = float(c.ewm(span=12, adjust=False).mean().iloc[-1])
        ema26  = float(c.ewm(span=26, adjust=False).mean().iloc[-1])

        # RSI
        d   = c.diff()
        g   = d.where(d > 0, 0.0).rolling(14).mean()
        l   = (-d.where(d < 0, 0.0)).rolling(14).mean()
        rs  = g / l.replace(0, np.nan)
        rsi = float((100 - 100 / (1 + rs)).iloc[-1])
        if np.isnan(rsi): rsi = 50.0

        # MACD
        ml   = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        sl   = ml.ewm(span=9, adjust=False).mean()
        hist = ml - sl
        macd_bull = float(ml.iloc[-1]) > float(sl.iloc[-1])
        hist_exp  = float(hist.iloc[-1]) > float(hist.iloc[-2]) if len(hist) > 1 else False

        # Volume
        vavg = float(v.rolling(20).mean().iloc[-1])
        vr   = round(float(v.iloc[-1]) / vavg, 2) if vavg > 0 else 1.0

        # Bollinger
        bb_m   = c.rolling(20).mean()
        bb_s   = c.rolling(20).std()
        bb_pos = float(((c - (bb_m - 2 * bb_s)) / (4 * bb_s.replace(0, np.nan))).iloc[-1])
        if np.isnan(bb_pos): bb_pos = 0.5

        # OBV slope
        obv       = (np.sign(c.diff()).fillna(0) * v).cumsum()
        obv_slope = float(obv.iloc[-1] - obv.iloc[-6]) if len(obv) >= 6 else 0

        # 52W
        w52h    = float(h.iloc[-252:].max()) if len(h) >= 252 else float(h.max())
        w52_pct = round((w52h - close) / w52h * 100, 1)

        # ATR
        hl  = h - lo
        hc  = (h - c.shift()).abs()
        lc  = (lo - c.shift()).abs()
        atr = float(pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(14).mean().iloc[-1])
        if np.isnan(atr): atr = close * 0.018

        # ── SCORING ──
        score   = 0
        reasons = []
        penalty = 0

        # Trend (0-30)
        if sma200:
            if close > sma20 > sma50 > sma200:
                score += 12; reasons.append("Full bullish SMA alignment")
            elif close > sma50 > sma200:
                score += 8;  reasons.append("Bullish SMA alignment")
            elif close > sma200:
                score += 4
            pct200 = (close - sma200) / sma200 * 100
            if 5 <= pct200 <= 25: score += 4
            elif 0 < pct200 < 5:  score += 2
        else:
            if close > sma20 > sma50: score += 8

        if ema12 > ema26:
            score += 4; reasons.append("EMA 12/26 bullish")
        trend_score = min(score, 30)

        # Momentum (0-25)
        mom = 0
        if   50 <= rsi <= 65: mom += 12; reasons.append(f"RSI at ideal zone ({rsi:.1f})")
        elif 45 <= rsi < 50:  mom += 7
        elif 65 < rsi <= 70:  mom += 5
        elif 40 <= rsi < 45:  mom += 3
        if macd_bull:   mom += 5; reasons.append("MACD bullish")
        if hist_exp:    mom += 4; reasons.append("MACD histogram expanding")
        mom_score = min(mom, 25)

        # Volume (0-20)
        vol = 0
        if   vr >= 2.5: vol += 10; reasons.append(f"Volume surge {vr}x avg")
        elif vr >= 2.0: vol += 8;  reasons.append(f"High volume {vr}x avg")
        elif vr >= 1.5: vol += 6;  reasons.append(f"Above avg volume {vr}x")
        elif vr >= 1.2: vol += 3
        elif vr < 0.7:  vol -= 2
        if obv_slope > 0: vol += 4; reasons.append("OBV trending up")
        vol_score = min(max(vol, 0), 20)

        # Breakout (0-15)
        brk = 0
        if   bb_pos >= 0.8: brk += 5; reasons.append("Near upper Bollinger Band")
        elif bb_pos >= 0.6: brk += 3
        elif bb_pos < 0.2:  brk -= 2
        if len(h) >= 21 and close > float(h.iloc[-21:-1].max()):
            brk += 5; reasons.append("Breaking 20-day high!")
        brk_score = min(max(brk, 0), 15)

        # Price Action (0-10)
        pa = 0
        if   w52_pct <= 3:  pa += 5; reasons.append(f"Near 52W high ({w52_pct}% away)")
        elif w52_pct <= 8:  pa += 3
        elif w52_pct <= 15: pa += 1
        if len(c) >= 6:
            r5 = (close - float(c.iloc[-6])) / float(c.iloc[-6]) * 100
            if r5 >= 5: pa += 3 
            elif r5 >= 2: pa += 2
            elif r5 >= 0: pa += 1
        pa_score = min(pa, 10)

        # Penalties
        if rsi > 78:                                   penalty += 12
        if sma200 and close < sma200:                  penalty += 6
        if sma200 and sma50 < sma200:                  penalty += 8

        total = max(0, min(100, trend_score + mom_score + vol_score + brk_score + pa_score - penalty))

        if   total >= 75: sig, conf = "STRONG BUY", "HIGH"
        elif total >= 60: sig, conf = "BUY",         "HIGH" if total >= 67 else "MEDIUM"
        elif total >= 45: sig, conf = "WATCH",       "MEDIUM"
        else:             sig, conf = "AVOID",       "LOW"

        # Entry / Exit
        entry_price = round(close - atr * 0.3, 2)
        entry_max   = round(close * 1.003, 2)
        limit_order = round(entry_price * 0.9993, 2)

        # Category
        if total >= 70 and brk_score >= 8:
            cat = "short"
        elif total >= 60 and trend_score >= 18:
            cat = "medium"
        else:
            cat = "long"

        # Targets by category
        if cat == "short":
            t1, t2, t3 = round(close + atr * 1.8, 2), round(close + atr * 2.8, 2), round(close + atr * 4.0, 2)
            hold_dur = "5-15 trading days"
            window   = "09:15-09:45 AM IST"
            strategy = "Place limit order before 9:14 AM. Cancel if not filled by 9:45 AM."
        elif cat == "medium":
            t1, t2, t3 = round(close + atr * 3.5, 2), round(close + atr * 5.5, 2), round(close + atr * 8.0, 2)
            hold_dur = "4-12 weeks"
            window   = "09:15-10:15 AM IST"
            strategy = "Place limit order. Patient fill — re-evaluate if not filled by 10:15 AM."
        else:
            t1, t2, t3 = round(close * 1.25, 2), round(close * 1.40, 2), round(close * 1.60, 2)
            hold_dur = "6-18 months"
            window   = "09:15 AM IST (GTC limit)"
            strategy = "Set GTC limit order. No urgency — buy at support."

        u1 = round((t1 - entry_price) / entry_price * 100, 1)
        u2 = round((t2 - entry_price) / entry_price * 100, 1)
        u3 = round((t3 - entry_price) / entry_price * 100, 1)

        # MACD & SMA strings
        macd_str = "Bullish" if macd_bull else "Bearish"
        if sma200:
            if close > sma20 > sma50 > sma200: sma_str = "Full Bull (>20>50>200)"
            elif close > sma50 > sma200:        sma_str = "Bull (>50>200)"
            elif close > sma200:                sma_str = "Above 200 SMA"
            else:                               sma_str = "Bearish"
        else:
            sma_str = "Bullish" if close > sma50 else "Bearish"

        return {
            "symbol":        ticker.replace(".NS", ""),
            "score":         int(total),
            "signal":        sig,
            "confidence":    conf,
            "current_price": round(close, 2),
            "change_pct":    chg,
            "change":        round(close - prev, 2),
            "target_price":  t2,
            "stop_loss":     round(close * 0.95, 2),
            "risk_reward":   round((t2 - entry_price) / (close - close * 0.95), 1),
            "holding_category": cat,
            "sector":        "Various",
            "confidence":    conf,
            "penalty":       int(penalty),
            "scores": {
                "trend":        int(trend_score),
                "momentum":     int(mom_score),
                "volume":       int(vol_score),
                "breakout":     int(brk_score),
                "price_action": int(pa_score),
            },
            "indicators": {
                "rsi":           round(rsi, 1),
                "macd_signal":   macd_str,
                "sma_alignment": sma_str,
                "volume_ratio":  vr,
                "week52_pct":    w52_pct,
                "bb_position":   round(bb_pos, 2),
                "adx":           None,
            },
            "reasons": reasons[:6],
            "trade_plan": {
                "category":  cat,
                "cat_score": int(total),
                "atr":       round(atr, 2),
                "atr_pct":   round(atr / close * 100, 2),
                "entry": {
                    "ideal_price":    entry_price,
                    "limit_order":    limit_order,
                    "acceptable_max": entry_max,
                    "entry_window":   window,
                    "order_strategy": strategy,
                    "note": f"Prev close ₹{close:,.2f} | Expected dip ~₹{round(atr*0.3,2)}",
                },
                "exit": {
                    "target_conservative": t1,
                    "target_ideal":        t2,
                    "target_stretch":      t3,
                    "upside_conservative": u1,
                    "upside_ideal":        u2,
                    "upside_stretch":      u3,
                    "hold_min_days":       5 if cat == "short" else 30 if cat == "medium" else 180,
                    "hold_max_days":      15 if cat == "short" else 90 if cat == "medium" else 540,
                    "hold_duration":       hold_dur,
                    "sell_trigger":        f"Sell when RSI > 72 or price reaches ₹{t2:,.2f}",
                    "hold_note":           "Review daily" if cat == "short" else "Review weekly" if cat == "medium" else "Review monthly",
                },
            },
        }
    except Exception as e:
        return None


def build_response():
    symbols = list(STOCKS.keys())
    now     = datetime.now(IST)

    try:
        df_all = yf.download(
            tickers=symbols, period="1y",
            interval="1d", progress=False,
            auto_adjust=True, group_by="ticker",
        )
    except Exception as e:
        return {"error": str(e), "generated_at": now.isoformat()}

    results = []
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
                r["name"]   = name
                r["sector"] = sector
                results.append(r)
        except:
            continue

    results.sort(key=lambda x: x["score"], reverse=True)

    short_picks  = [r for r in results if r["holding_category"] == "short"][:5]
    medium_picks = [r for r in results if r["holding_category"] == "medium"][:5]
    long_picks   = [r for r in results if r["holding_category"] == "long"][:5]

    for i, r in enumerate(short_picks):  r["rank"] = i + 1
    for i, r in enumerate(medium_picks): r["rank"] = i + 1
    for i, r in enumerate(long_picks):   r["rank"] = i + 1

    top_picks = results[:15]
    for i, r in enumerate(top_picks): r["rank"] = i + 1

    adv  = sum(1 for r in results if r["change_pct"] > 0.25)
    dec  = sum(1 for r in results if r["change_pct"] < -0.25)
    unch = len(results) - adv - dec
    k    = 3500 / max(len(results), 1)

    sec_b: dict = {}
    for r in results:
        sec_b.setdefault(r["sector"], []).append(r["score"])
    sector_momentum = sorted([
        {"sector": s, "score": int(sum(v) / len(v)),
         "trend": "up" if sum(v) / len(v) >= 62 else "down" if sum(v) / len(v) < 48 else "neutral"}
        for s, v in sec_b.items()
    ], key=lambda x: x["score"], reverse=True)

    return {
        "generated_at":     now.isoformat(),
        "market_date":      now.strftime("%Y-%m-%d"),
        "analysis_version": "4.0-live",
        "stocks_analyzed":  len(results),
        "indices": {
            "NIFTY50":         {"value": 0, "change": 0, "change_pct": 0},
            "SENSEX":          {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_BANK":      {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_IT":        {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_MIDCAP100": {"value": 0, "change": 0, "change_pct": 0},
        },
        "market_breadth": {
            "advances":     int(adv * k),
            "declines":     int(dec * k),
            "unchanged":    int(unch * k),
            "new_52w_high": 0,
            "new_52w_low":  0,
        },
        "short_term":      {"label": "Short Term (5-15 days)",   "picks": short_picks},
        "medium_term":     {"label": "Medium Term (4-12 weeks)", "picks": medium_picks},
        "long_term":       {"label": "Long Term (6-18 months)",  "picks": long_picks},
        "top_picks":       top_picks,
        "watchlist":       [{"symbol":r["symbol"],"name":r["name"],"sector":r["sector"],"current_price":r["current_price"],"score":r["score"],"signal":"WATCH","reason":r["reasons"][0] if r["reasons"] else "Monitor"} for r in results if r["signal"]=="WATCH"][:5],
        "avoid":           [{"symbol":r["symbol"],"name":r["name"],"sector":r["sector"],"current_price":r["current_price"],"score":r["score"],"reason":r["reasons"][0] if r["reasons"] else "Bearish"} for r in results if r["signal"]=="AVOID"][:3],
        "sector_momentum": sector_momentum[:10],
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        data = build_response()
        body = json.dumps(data, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control",  "no-cache, max-age=60")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()

    def log_message(self, *args):
        pass
