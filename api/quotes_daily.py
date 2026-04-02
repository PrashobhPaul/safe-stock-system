"""
quotes_daily.py — StockSage India
===================================
Standalone script for GitHub Actions.
Fetches data, scores stocks, saves predictions.json.
Called by daily.yml workflow.
"""

import json
import os
import sys
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

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


def score_stock(ticker, df):
    try:
        c  = df["Close"]
        h  = df["High"]
        lo = df["Low"]
        v  = df["Volume"]
        op = df["Open"]

        close = float(c.iloc[-1])
        prev  = float(c.iloc[-2]) if len(c) > 1 else close
        chg   = round((close - prev) / prev * 100, 2) if prev else 0

        sma20  = float(c.rolling(20).mean().iloc[-1])
        sma50  = float(c.rolling(50).mean().iloc[-1])
        sma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else None

        ema12 = float(c.ewm(span=12, adjust=False).mean().iloc[-1])
        ema26 = float(c.ewm(span=26, adjust=False).mean().iloc[-1])

        d   = c.diff()
        g   = d.where(d > 0, 0.0).rolling(14).mean()
        l   = (-d.where(d < 0, 0.0)).rolling(14).mean()
        rsi = float((100 - 100 / (1 + g / l.replace(0, np.nan))).iloc[-1])
        if np.isnan(rsi): rsi = 50.0

        ml     = c.ewm(span=12,adjust=False).mean() - c.ewm(span=26,adjust=False).mean()
        sl_    = ml.ewm(span=9,adjust=False).mean()
        hist   = ml - sl_
        macd_b = float(ml.iloc[-1]) > float(sl_.iloc[-1])
        hist_ex= float(hist.iloc[-1]) > float(hist.iloc[-2]) if len(hist)>1 else False

        vavg  = float(v.rolling(20).mean().iloc[-1])
        vr    = round(float(v.iloc[-1]) / vavg, 2) if vavg > 0 else 1.0

        bb_m  = c.rolling(20).mean()
        bb_s  = c.rolling(20).std()
        bb_pos= float(((c-(bb_m-2*bb_s))/(4*bb_s.replace(0,np.nan))).iloc[-1])
        if np.isnan(bb_pos): bb_pos = 0.5

        obv   = (np.sign(c.diff()).fillna(0) * v).cumsum()
        obv_up= float(obv.iloc[-1]-obv.iloc[-6]) > 0 if len(obv)>=6 else False

        hl  = h - lo
        hc  = (h - c.shift()).abs()
        lc  = (lo - c.shift()).abs()
        atr = float(pd.concat([hl,hc,lc],axis=1).max(axis=1).rolling(14).mean().iloc[-1])
        if np.isnan(atr): atr = close * 0.018

        w52h    = float(h.iloc[-252:].max()) if len(h)>=252 else float(h.max())
        w52_pct = round((w52h - close) / w52h * 100, 1)

        # Scoring
        reasons = []
        trend = 0
        if sma200:
            if close > sma20 > sma50 > sma200:
                trend += 12; reasons.append("Full bullish SMA alignment")
            elif close > sma50 > sma200:
                trend += 8;  reasons.append("Bullish SMA alignment")
            elif close > sma200:
                trend += 4;  reasons.append("Above 200 SMA")
            pct200 = (close - sma200) / sma200 * 100
            if 5 <= pct200 <= 25: trend += 4
            elif 0 < pct200 < 5:  trend += 2
        else:
            if close > sma20 > sma50: trend += 8
            elif close > sma50:       trend += 4
        if ema12 > ema26: trend += 3; reasons.append("EMA 12/26 bullish")
        if close > sma20: trend += 3
        trend = min(trend, 30)

        mom = 0
        if   50 <= rsi <= 65: mom += 12; reasons.append(f"RSI ideal zone ({rsi:.1f})")
        elif 45 <= rsi < 50:  mom += 8;  reasons.append(f"RSI building ({rsi:.1f})")
        elif 65 < rsi <= 72:  mom += 6;  reasons.append(f"RSI strong ({rsi:.1f})")
        elif 40 <= rsi < 45:  mom += 4
        elif 30 <= rsi < 40:  mom += 2
        if macd_b:  mom += 5; reasons.append("MACD bullish")
        if hist_ex: mom += 4; reasons.append("MACD histogram expanding")
        mom = min(mom, 25)

        vol = 0
        if   vr >= 2.5: vol += 10; reasons.append(f"Volume surge {vr:.1f}x")
        elif vr >= 2.0: vol += 8;  reasons.append(f"High volume {vr:.1f}x")
        elif vr >= 1.5: vol += 6;  reasons.append(f"Above avg vol {vr:.1f}x")
        elif vr >= 1.2: vol += 4
        elif vr >= 0.9: vol += 2
        if obv_up: vol += 4; reasons.append("OBV trending up")
        vol = min(max(vol, 0), 20)

        brk = 0
        if bb_pos >= 0.75: brk += 5; reasons.append("Near upper BB")
        elif bb_pos >= 0.55: brk += 3
        if len(h)>=21 and close > float(h.iloc[-21:-1].max()):
            brk += 5; reasons.append("Breaking 20-day high!")
        elif len(h)>=11 and close > float(h.iloc[-11:-1].max()):
            brk += 2
        if len(h)>=252 and close > float(h.iloc[-252:-1].max()):
            brk += 5; reasons.append("New 52-week high!")
        brk = min(max(brk, 0), 15)

        pa = 0
        if   w52_pct <= 3:  pa += 5; reasons.append(f"Near 52W high ({w52_pct:.1f}%)")
        elif w52_pct <= 8:  pa += 3
        elif w52_pct <= 15: pa += 2
        elif w52_pct <= 25: pa += 1
        if len(c)>=6:
            r5 = (close - float(c.iloc[-6])) / float(c.iloc[-6]) * 100
            if r5 >= 4: pa += 3
            elif r5 >= 2: pa += 2
            elif r5 >= 0: pa += 1
        if close > float(op.iloc[-1]): pa += 1
        pa = min(pa, 10)

        penalty = 0
        if rsi > 78:               penalty += 10
        if sma200 and close<sma200: penalty += 5
        if sma200 and sma50<sma200: penalty += 6
        if chg < -2:               penalty += 3

        total = max(0, min(100, trend+mom+vol+brk+pa-penalty))

        if   total >= 72: sig, conf = "STRONG BUY", "HIGH"
        elif total >= 55: sig, conf = "BUY",         "HIGH" if total>=63 else "MEDIUM"
        elif total >= 40: sig, conf = "WATCH",       "MEDIUM"
        else:             sig, conf = "AVOID",       "LOW"

        # Category
        short_pts = medium_pts = long_pts = 0
        if brk>=8:           short_pts += 4
        if vr>=1.5:          short_pts += 3
        if 50<=rsi<=67:      short_pts += 3
        if hist_ex:          short_pts += 3
        if w52_pct<=8:       short_pts += 3
        if chg>0.5:          short_pts += 2

        if trend>=15:        medium_pts += 4
        if macd_b:           medium_pts += 3
        if 45<=rsi<=65:      medium_pts += 3
        if vr>=1.2:          medium_pts += 2
        if 8<w52_pct<=25:    medium_pts += 3

        if sma200 and close>sma20>sma50>sma200: long_pts += 5
        if trend>=20:        long_pts += 3
        if rsi<60:           long_pts += 2
        if w52_pct>20:       long_pts += 3
        if obv_up:           long_pts += 2

        if short_pts>=medium_pts and short_pts>=long_pts: cat = "short"
        elif medium_pts>=long_pts:                        cat = "medium"
        else:                                             cat = "long"

        if cat == "short":
            pullback = atr*0.28; window="09:15–09:45 AM IST"
            strategy="Place limit order before 9:14 AM"
            t1=round(close+atr*1.8,2); t2=round(close+atr*2.8,2); t3=round(close+atr*4.0,2)
            hold_dur="5–15 trading days"
            sell_trig=f"Exit when RSI>72 or price hits ₹{t2:,.2f}"
        elif cat == "medium":
            pullback = atr*0.50; window="09:15–10:15 AM IST"
            strategy="Patient limit order — re-evaluate if not filled by 10:15"
            t1=round(close+atr*3.5,2); t2=round(close+atr*5.5,2); t3=round(close+atr*8.0,2)
            hold_dur="4–12 weeks"
            sell_trig=f"Exit on weekly MACD bearish cross or ₹{t2:,.2f}"
        else:
            pullback = atr*0.75; window="09:15 AM IST (GTC limit)"
            strategy="GTC limit order — no urgency, buy at support"
            t1=round(close*1.25,2); t2=round(close*1.45,2); t3=round(close*1.65,2)
            hold_dur="6–18 months"
            sell_trig=f"Exit if fundamentals deteriorate or ₹{t2:,.2f} hit"

        entry_price = round(close - pullback, 2)
        limit_ord   = round(entry_price * 0.9993, 2)
        entry_max   = round(close * (1.003 if cat=="short" else 1.005 if cat=="medium" else 1.010), 2)
        u1 = round((t1-entry_price)/entry_price*100,1)
        u2 = round((t2-entry_price)/entry_price*100,1)
        u3 = round((t3-entry_price)/entry_price*100,1)

        macd_str = "Bullish" if macd_b else "Bearish"
        if sma200:
            if close>sma20>sma50>sma200:  sma_str="Full Bull (>20>50>200)"
            elif close>sma50>sma200:       sma_str="Bull (>50>200)"
            elif close>sma200:             sma_str="Above 200 SMA"
            else:                          sma_str="Bearish"
        else:
            sma_str = "Bullish" if close>sma50 else "Bearish"

        return {
            "symbol":        ticker.replace(".NS",""),
            "score":         int(total),
            "signal":        sig,
            "confidence":    conf,
            "current_price": round(close,2),
            "change_pct":    chg,
            "change":        round(close-prev,2),
            "target_price":  t2,
            "stop_loss":     round(close*0.94,2),
            "risk_reward":   round((t2-entry_price)/max(close-close*0.94,1),1),
            "holding_category": cat,
            "sector":        "Various",
            "penalty":       int(penalty),
            "scores": {"trend":int(trend),"momentum":int(mom),"volume":int(vol),"breakout":int(brk),"price_action":int(pa)},
            "indicators": {
                "rsi":round(rsi,1),"macd_signal":macd_str,"sma_alignment":sma_str,
                "volume_ratio":vr,"week52_pct":w52_pct,"bb_position":round(bb_pos,2),"adx":None,
            },
            "reasons": reasons[:6],
            "trade_plan": {
                "category":cat,"cat_score":int(max(short_pts,medium_pts,long_pts)),
                "atr":round(atr,2),"atr_pct":round(atr/close*100,2),
                "entry":{
                    "ideal_price":entry_price,"limit_order":limit_ord,"acceptable_max":entry_max,
                    "entry_window":window,"order_strategy":strategy,
                    "note":f"Prev close ₹{close:,.2f} | Dip ~₹{round(pullback,2)}",
                },
                "exit":{
                    "target_conservative":t1,"target_ideal":t2,"target_stretch":t3,
                    "upside_conservative":u1,"upside_ideal":u2,"upside_stretch":u3,
                    "hold_min_days":5 if cat=="short" else 30 if cat=="medium" else 180,
                    "hold_max_days":15 if cat=="short" else 90 if cat=="medium" else 540,
                    "hold_duration":hold_dur,"sell_trigger":sell_trig,
                    "hold_note":"Review daily" if cat=="short" else "Review weekly" if cat=="medium" else "Review monthly",
                },
            },
        }
    except Exception as e:
        return None


def main():
    now = datetime.now(IST)
    print(f"StockSage Daily Analysis — {now.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"Analyzing {len(STOCKS)} stocks...")

    try:
        df_all = yf.download(
            tickers=list(STOCKS.keys()),
            period="1y", interval="1d",
            progress=False, auto_adjust=True, group_by="ticker",
        )
    except Exception as e:
        print(f"ERROR downloading data: {e}")
        sys.exit(1)

    results = []
    for ticker, (name, sector) in STOCKS.items():
        try:
            if isinstance(df_all.columns, pd.MultiIndex):
                if ticker not in df_all.columns.get_level_values(0): continue
                df = df_all[ticker].dropna(subset=["Close"]).copy()
            else:
                df = df_all.dropna(subset=["Close"]).copy()
            if len(df) < 30: continue
            r = score_stock(ticker, df)
            if r:
                r["name"] = name; r["sector"] = sector
                results.append(r)
                print(f"  ✓ {ticker:<22} {r['score']:>3}/100 | {r['signal']:<12} | {r['holding_category']}")
        except Exception as e:
            print(f"  ✗ {ticker}: {e}")

    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"\nScored {len(results)} stocks")

    # Force 5 picks per category
    used = set()
    def top5(cat):
        picks = [r for r in results if r["holding_category"]==cat and r["signal"] in ("STRONG BUY","BUY")]
        used.update(r["symbol"] for r in picks[:5])
        if len(picks) < 5:
            extras = [r for r in results if r["symbol"] not in used and r["signal"] in ("STRONG BUY","BUY","WATCH")]
            for r in extras:
                if len(picks) >= 5: break
                rc = dict(r); rc["holding_category"] = cat
                if cat=="short":
                    rc["trade_plan"]["entry"]["entry_window"]="09:15–09:45 AM IST"
                    rc["trade_plan"]["exit"]["hold_duration"]="5–15 trading days"
                elif cat=="medium":
                    rc["trade_plan"]["entry"]["entry_window"]="09:15–10:15 AM IST"
                    rc["trade_plan"]["exit"]["hold_duration"]="4–12 weeks"
                else:
                    rc["trade_plan"]["entry"]["entry_window"]="09:15 AM IST (GTC)"
                    rc["trade_plan"]["exit"]["hold_duration"]="6–18 months"
                picks.append(rc); used.add(rc["symbol"])
        for i,r in enumerate(picks[:5]): r["rank"]=i+1
        return picks[:5]

    short_picks  = top5("short")
    medium_picks = top5("medium")
    long_picks   = top5("long")
    top_picks    = results[:15]
    for i,r in enumerate(top_picks): r["rank"]=i+1

    adv = sum(1 for r in results if r["change_pct"]>0.25)
    dec = sum(1 for r in results if r["change_pct"]<-0.25)
    k   = 3500/max(len(results),1)

    sec_b: dict = {}
    for r in results: sec_b.setdefault(r["sector"],[]).append(r["score"])
    sector_momentum = sorted([
        {"sector":s,"score":int(sum(v)/len(v)),"trend":"up" if sum(v)/len(v)>=60 else "down" if sum(v)/len(v)<45 else "neutral"}
        for s,v in sec_b.items()
    ], key=lambda x:x["score"], reverse=True)

    output = {
        "generated_at":     now.isoformat(),
        "market_date":      now.strftime("%Y-%m-%d"),
        "analysis_version": "4.1",
        "stocks_analyzed":  len(results),
        "indices": {
            "NIFTY50":{"value":0,"change":0,"change_pct":0},
            "SENSEX":{"value":0,"change":0,"change_pct":0},
            "NIFTY_BANK":{"value":0,"change":0,"change_pct":0},
            "NIFTY_IT":{"value":0,"change":0,"change_pct":0},
            "NIFTY_MIDCAP100":{"value":0,"change":0,"change_pct":0},
        },
        "market_breadth":{"advances":int(adv*k),"declines":int(dec*k),"unchanged":int((len(results)-adv-dec)*k),"new_52w_high":0,"new_52w_low":0},
        "short_term":  {"label":"Short Term (5–15 days)",   "picks":short_picks},
        "medium_term": {"label":"Medium Term (4–12 weeks)", "picks":medium_picks},
        "long_term":   {"label":"Long Term (6–18 months)",  "picks":long_picks},
        "top_picks":   top_picks,
        "watchlist":   [{"symbol":r["symbol"],"name":r["name"],"sector":r["sector"],"current_price":r["current_price"],"score":r["score"],"signal":"WATCH","reason":r["reasons"][0] if r["reasons"] else "Monitor"} for r in results if r["signal"]=="WATCH"][:5],
        "avoid":       [{"symbol":r["symbol"],"name":r["name"],"sector":r["sector"],"current_price":r["current_price"],"score":r["score"],"reason":r["reasons"][0] if r["reasons"] else "Bearish"} for r in results if r["signal"]=="AVOID"][:3],
        "sector_momentum": sector_momentum[:10],
    }

    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "predictions.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n✅ predictions.json saved!")
    print(f"   Short term  : {len(short_picks)} picks — {[p['symbol'] for p in short_picks]}")
    print(f"   Medium term : {len(medium_picks)} picks — {[p['symbol'] for p in medium_picks]}")
    print(f"   Long term   : {len(long_picks)} picks — {[p['symbol'] for p in long_picks]}")


if __name__ == "__main__":
    main()
