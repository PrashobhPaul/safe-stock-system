"""
analyze.py — StockSage India v3.2 (Rules Engine + Horizon DNA + Daily Brief)
=============================================================================
Orchestrator. Runs:
  1. ResilientFetcher over the focused portfolio and broad universe.
  2. rules_engine.PortfolioAnalyzer over the focused portfolio holdings.
  3. HorizonClassifier — intrinsic short / medium / long classification of
     every stock, independent of today's action signal.
  4. Legacy projection that guarantees EVERY stock lands in its horizon tab,
     sorted by actionability (STRONG BUY → BUY → ACCUMULATE → HOLD →
     BLACKOUT → BOOK PARTIAL → EXIT → AVOID).
  5. DailyBriefBuilder — a deterministic end-of-day evaluation summary that
     populates the AI Brief tab without any LLM call.

Writes predictions.json with:
  • portfolio      — engine decisions (portfolio.html)
  • short_term     — EVERY stock classified as short horizon
  • medium_term    — EVERY stock classified as medium horizon
  • long_term      — EVERY stock classified as long horizon
  • daily_brief    — deterministic EOD report for the AI tab
  • top_picks, watchlist, avoid, sector_momentum (legacy block)

Zero LLM. Zero paid services. Every failure is handled — the pipeline
NEVER crashes GitHub Actions.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from dataclasses import asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pytz

from resilient_fetcher import ResilientFetcher
from rules_engine import (
    PortfolioAnalyzer,
    load_config,
)

IST = pytz.timezone("Asia/Kolkata")
log = logging.getLogger("analyze")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "focused_portfolio.yml"
OUTPUT_PATH = ROOT / "predictions.json"
DB_PATH = str(ROOT / "stock_data.db")


# ─────────────────────────────────────────────────────────────────────────────
# Broader universe — used by the legacy index.html scanner view.
# Union of focused tickers + existing blue-chips. Deduped.
# ─────────────────────────────────────────────────────────────────────────────

BROAD_UNIVERSE = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "KOTAKBANK.NS", "AXISBANK.NS", "SBIN.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "LT.NS", "ITC.NS", "HINDUNILVR.NS", "BHARTIARTL.NS", "MARUTI.NS",
    "M&M.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "JSWSTEEL.NS", "HINDALCO.NS",
    "SUNPHARMA.NS", "CIPLA.NS", "DRREDDY.NS", "DIVISLAB.NS",
    "WIPRO.NS", "HCLTECH.NS", "TECHM.NS", "LTIM.NS", "PERSISTENT.NS", "COFORGE.NS",
    "NTPC.NS", "POWERGRID.NS", "ONGC.NS", "COALINDIA.NS", "BPCL.NS",
    "ADANIPORTS.NS", "ULTRACEMCO.NS", "GRASIM.NS", "NESTLEIND.NS",
    "ASIANPAINT.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS",
    "SBILIFE.NS", "HDFCLIFE.NS", "ICICIPRULI.NS", "BRITANNIA.NS", "SHREECEM.NS",
    "TRENT.NS", "TATAELXSI.NS", "JSWENERGY.NS", "TVSMOTOR.NS",
    "ZOMATO.NS", "NHPC.NS", "DIXON.NS", "TITAN.NS", "APOLLOHOSP.NS",
    "TATACONSUM.NS", "INDUSINDBK.NS", "VEDL.NS", "PFC.NS", "RECLTD.NS",
    "HAL.NS", "BEL.NS", "IRCTC.NS", "NAUKRI.NS", "PIDILITIND.NS", "HAVELLS.NS",
    "POLYCAB.NS", "MUTHOOTFIN.NS", "CHOLAFIN.NS", "BANKBARODA.NS",
    "FEDERALBNK.NS", "IDFCFIRSTB.NS", "DABUR.NS", "MARICO.NS", "GODREJCP.NS",
    "COLPAL.NS", "AMBUJACEM.NS", "ACC.NS", "BERGEPAINT.NS", "AUROPHARMA.NS", "LUPIN.NS",
    "ECLERX.NS", "CHAMBLFERT.NS", "ARE&M.NS", "HYUNDAI.NS", "SBICARD.NS",
    "TATAPOWER.NS", "KPITTECH.NS", "TATACAP.NS", "JBMA.NS", "EXIDEIND.NS",
    "BLS.NS", "ETERNAL.NS", "TARIL.NS", "IREDA.NS", "POONAWALLA.NS",
    "TEXRAIL.NS", "SERVOTECH.NS", "SIEMENS.NS", "DEEPAKNTR.NS", "ZENTEC.NS",
    "GREENPOWER.NS", "ZINKA.NS", "CROMPTON.NS", "DMART.NS", "TATAINVEST.NS",
    "COCHINSHIP.NS", "AURIONPRO.NS", "SUZLON.NS", "EASEMYTRIP.NS",
    "HUHTAMAKI.NS", "GREAVESCOT.NS", "RPOWER.NS", "OLAELEC.NS", "PCJEWELLER.NS",
    "CYIENTDLM.NS", "ADSL.NS", "BSE.NS", "NELCO.NS", "SWIGGY.NS", "URBAN.NS",
    "MON150BEES.NS",
]

SECTOR_MAP = {
    "RELIANCE.NS": "Conglomerate",  "TCS.NS": "IT", "INFY.NS": "IT",
    "HDFCBANK.NS": "Banking", "ICICIBANK.NS": "Banking", "KOTAKBANK.NS": "Banking",
    "AXISBANK.NS": "Banking", "SBIN.NS": "Banking", "BAJFINANCE.NS": "NBFC",
    "BAJAJFINSV.NS": "NBFC", "LT.NS": "Infrastructure", "ITC.NS": "FMCG",
    "HINDUNILVR.NS": "FMCG", "BHARTIARTL.NS": "Telecom", "MARUTI.NS": "Automobiles",
    "M&M.NS": "Automobiles", "TATAMOTORS.NS": "Automobiles", "TATASTEEL.NS": "Metals",
    "JSWSTEEL.NS": "Metals", "HINDALCO.NS": "Metals", "SUNPHARMA.NS": "Pharma",
    "CIPLA.NS": "Pharma", "DRREDDY.NS": "Pharma", "DIVISLAB.NS": "Pharma",
    "WIPRO.NS": "IT", "HCLTECH.NS": "IT", "TECHM.NS": "IT", "LTIM.NS": "IT",
    "PERSISTENT.NS": "IT", "COFORGE.NS": "IT", "KPITTECH.NS": "IT",
    "TATAELXSI.NS": "IT", "ECLERX.NS": "IT",
    "NTPC.NS": "Power", "POWERGRID.NS": "Power", "NHPC.NS": "Power",
    "JSWENERGY.NS": "Power", "TATAPOWER.NS": "Power", "SUZLON.NS": "Power",
    "IREDA.NS": "NBFC", "ONGC.NS": "Oil & Gas", "COALINDIA.NS": "Mining",
    "BPCL.NS": "Oil & Gas", "ADANIPORTS.NS": "Infrastructure",
    "ULTRACEMCO.NS": "Cement", "GRASIM.NS": "Cement", "AMBUJACEM.NS": "Cement",
    "ACC.NS": "Cement", "SHREECEM.NS": "Cement", "NESTLEIND.NS": "FMCG",
    "ASIANPAINT.NS": "Paints", "BERGEPAINT.NS": "Paints",
    "BAJAJ-AUTO.NS": "Automobiles", "HEROMOTOCO.NS": "Automobiles",
    "EICHERMOT.NS": "Automobiles", "TVSMOTOR.NS": "Automobiles",
    "SBILIFE.NS": "Insurance", "HDFCLIFE.NS": "Insurance",
    "ICICIPRULI.NS": "Insurance", "BRITANNIA.NS": "FMCG", "TRENT.NS": "Retail",
    "DMART.NS": "Retail", "ZOMATO.NS": "Internet", "ETERNAL.NS": "Internet",
    "SWIGGY.NS": "Internet", "NAUKRI.NS": "Internet", "URBAN.NS": "Internet",
    "ZINKA.NS": "Logistics", "DIXON.NS": "Electronics", "TITAN.NS": "Consumer Disc.",
    "APOLLOHOSP.NS": "Healthcare", "TATACONSUM.NS": "FMCG",
    "INDUSINDBK.NS": "Banking", "BANKBARODA.NS": "Banking",
    "FEDERALBNK.NS": "Banking", "IDFCFIRSTB.NS": "Banking",
    "VEDL.NS": "Metals", "PFC.NS": "NBFC", "RECLTD.NS": "NBFC",
    "MUTHOOTFIN.NS": "NBFC", "CHOLAFIN.NS": "NBFC", "POONAWALLA.NS": "NBFC",
    "SBICARD.NS": "NBFC", "TATACAP.NS": "NBFC", "TATAINVEST.NS": "NBFC",
    "BSE.NS": "Capital Markets", "HAL.NS": "Defence", "BEL.NS": "Defence",
    "ZENTEC.NS": "Defence", "COCHINSHIP.NS": "Defence", "IRCTC.NS": "Travel",
    "EASEMYTRIP.NS": "Travel", "PIDILITIND.NS": "Chemicals",
    "DEEPAKNTR.NS": "Chemicals", "CHAMBLFERT.NS": "Chemicals",
    "HAVELLS.NS": "Electricals", "POLYCAB.NS": "Electricals",
    "CROMPTON.NS": "Electricals", "DABUR.NS": "FMCG", "MARICO.NS": "FMCG",
    "GODREJCP.NS": "FMCG", "COLPAL.NS": "FMCG", "HUHTAMAKI.NS": "FMCG",
    "AUROPHARMA.NS": "Pharma", "LUPIN.NS": "Pharma",
    "EXIDEIND.NS": "Automobiles", "ARE&M.NS": "Automobiles",
    "HYUNDAI.NS": "Automobiles", "JBMA.NS": "Automobiles",
    "TARIL.NS": "Capital Goods", "SIEMENS.NS": "Capital Goods",
    "TEXRAIL.NS": "Capital Goods", "GREAVESCOT.NS": "Capital Goods",
    "AURIONPRO.NS": "IT", "BLS.NS": "Services", "OLAELEC.NS": "Automobiles",
    "SERVOTECH.NS": "Power", "GREENPOWER.NS": "Power", "RPOWER.NS": "Power",
    "CYIENTDLM.NS": "Electronics", "ADSL.NS": "IT",
    "PCJEWELLER.NS": "Consumer Disc.", "NELCO.NS": "Telecom",
    "MON150BEES.NS": "ETF",
}


# ═════════════════════════════════════════════════════════════════════════════
# HORIZON CLASSIFIER — intrinsic short / medium / long DNA
# ═════════════════════════════════════════════════════════════════════════════
#
# Philosophy
# ----------
# A stock's trading horizon is not a function of today's signal. It is a
# property of the instrument itself — determined by how it moves, how it is
# owned, and how mature the underlying business is.
#
# RELIANCE is a long-term compounder whether you're buying, holding or
# exiting. SUZLON is a short-term momentum vehicle even on a day it prints
# HOLD. The classifier looks at FIVE layers in priority order:
#
#   1. Explicit override in focused_portfolio.yml (`horizon: short|medium|long`)
#   2. Sector baseline (FMCG/IT/Banks → long, Power/Defence → medium,
#      Internet/Small-cap → short)
#   3. Flags from config (speculative / newly_listed → short bias;
#      etf → long bias; psu → medium bias)
#   4. Fundamentals (debt-free + analyst_count ≥ 25 + ROE ≥ 20 → long;
#      loss-making + stretched valuation → short)
#   5. Technical character (ATR% ≥ 4.5 → short; ATR% ≤ 1.5 → long)
#
# Output is a horizon score on [0..100]:
#   ≤ 35  → long       (compounder)
#   36–65 → medium     (trend trade)
#   ≥ 66  → short      (momentum)
#
# A safety re-balance pass guarantees every bucket has at least
# `MIN_PER_BUCKET` stocks by moving border cases from the fullest adjacent
# bucket. This removes the "all stocks ended up long" failure mode without
# forcing artificial symmetry.
#
# The classifier NEVER raises. A total failure falls back to "medium" and
# logs a warning.
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_HORIZON_BASE = {
    # long-biased (compounders, defensives)
    "FMCG":           15,
    "IT":             28,
    "Banking":        28,
    "Insurance":      22,
    "Pharma":         30,
    "Paints":         22,
    "Conglomerate":   22,
    "ETF":            5,
    "Consumer Disc.": 30,
    "Healthcare":     28,

    # medium-biased (cyclicals, trend trades)
    "NBFC":           48,
    "Automobiles":    45,
    "Cement":         40,
    "Infrastructure": 52,
    "Telecom":        40,
    "Oil & Gas":      42,
    "Chemicals":      48,
    "Electricals":    42,
    "Capital Markets": 45,

    # short-biased (momentum, small-caps, thematic)
    "Power":          58,
    "Defence":        62,
    "Metals":         60,
    "Mining":         55,
    "Retail":         50,
    "Internet":       70,
    "Electronics":    62,
    "Capital Goods":  58,
    "Logistics":      60,
    "Services":       55,
    "Travel":         55,
    "Other":          50,
}

MIN_PER_BUCKET = 10           # floor after rebalance
LONG_CEIL, SHORT_FLOOR = 35, 66


class HorizonClassifier:

    def classify(self, decision: dict, holding_override: Optional[dict] = None) -> dict:
        """
        Returns a dict:
            { "horizon": "short|medium|long",
              "score": 0..100,
              "reasons": [str, ...] }
        Never raises.
        """
        try:
            return self._classify_inner(decision, holding_override or {})
        except Exception as e:
            log.warning(f"horizon classify failed for "
                        f"{decision.get('name','?')}: {e}")
            return {"horizon": "medium", "score": 50,
                    "reasons": [f"fallback (classifier error: {type(e).__name__})"]}

    def _classify_inner(self, d: dict, override: dict) -> dict:
        reasons: list[str] = []

        # 1. Explicit override from config wins
        explicit = (override.get("horizon") or "").lower()
        if explicit in ("short", "medium", "long"):
            reasons.append(f"explicit horizon in config: {explicit}")
            return {"horizon": explicit,
                    "score": {"long": 20, "medium": 50, "short": 80}[explicit],
                    "reasons": reasons}

        ticker = d.get("resolved_ticker") or ""
        sector = SECTOR_MAP.get(ticker, "Other")
        base = SECTOR_HORIZON_BASE.get(sector, 50)
        score = float(base)
        reasons.append(f"{sector} baseline → {base}")

        # 2. Flags
        flags = set(d.get("quality_flags") or []) | set(override.get("flags") or [])
        if "speculative" in flags:
            score += 22; reasons.append("speculative flag → +22")
        if "newly_listed" in flags:
            score += 18; reasons.append("newly listed → +18")
        if "psu" in flags:
            score -= 6
        if "etf" in flags:
            score -= 35; reasons.append("ETF → heavy long pull")
        if "not_listed" in flags or "do_not_trade" in flags:
            # still classify so it shows somewhere — use long as neutral park
            score = min(score, 35)
            reasons.append("not tradeable → parked in long tab")

        # 3. Fundamentals — prefer override dict (from config), fall back to decision dict
        fund = (override.get("fundamentals")
                or (d.get("fundamentals") or {}).get("raw")
                or {})
        if fund.get("debt_free"):
            score -= 10; reasons.append("debt-free → -10")
        if (fund.get("roe") or 0) >= 20:
            score -= 6
        if (fund.get("roce") or 0) >= 20:
            score -= 4
        if (fund.get("analyst_count") or 0) >= 25:
            score -= 8; reasons.append(f"{fund['analyst_count']} analysts → -8")
        if (fund.get("dividend_yield_pct") or 0) >= 3:
            score -= 7
        if fund.get("loss_making") or fund.get("is_profitable") is False:
            score += 16; reasons.append("loss-making → +16")
        if fund.get("valuation") == "stretched" or (fund.get("pe") or 0) >= 70:
            score += 10; reasons.append("stretched valuation → +10")

        # 4. Technical character — stability / volatility signature
        tech = d.get("tech") or {}
        atr_pct = tech.get("atr_pct") or 0
        adx = tech.get("adx") or 0
        if atr_pct >= 5.0:
            score += 18; reasons.append(f"ATR% {atr_pct} → +18 (hot)")
        elif atr_pct >= 3.5:
            score += 8
        elif atr_pct and atr_pct <= 1.6:
            score -= 10; reasons.append(f"ATR% {atr_pct} → -10 (stable)")
        elif atr_pct and atr_pct <= 2.2:
            score -= 4

        if adx >= 28:
            # A stock trending hard is medium unless fundamentals override
            score = (score + 55) / 2 if score < 55 else score

        # Clamp
        score = max(0.0, min(100.0, score))

        if score <= LONG_CEIL:
            horizon = "long"
        elif score >= SHORT_FLOOR:
            horizon = "short"
        else:
            horizon = "medium"

        return {"horizon": horizon, "score": round(score, 1), "reasons": reasons}


def _rebalance_horizons(buckets: dict) -> None:
    """
    Guarantees at least MIN_PER_BUCKET stocks per horizon by pulling
    border cases from the most over-supplied adjacent bucket. Mutates
    buckets in place.

    We pull the stocks *closest to the receiving bucket's boundary* first
    (smallest move) so the re-assignment is as intuitive as possible.
    """
    order = ["short", "medium", "long"]
    adjacents = {"short": ["medium"],
                 "medium": ["short", "long"],
                 "long": ["medium"]}

    for cat in order:
        while len(buckets[cat]) < MIN_PER_BUCKET:
            # Find donor: the adjacent bucket with most slack
            donors = sorted(
                [a for a in adjacents[cat] if len(buckets[a]) > MIN_PER_BUCKET],
                key=lambda a: -len(buckets[a])
            )
            if not donors:
                break
            donor = donors[0]
            # Pick the item in donor that is closest to receiver's boundary
            if cat == "long":
                buckets[donor].sort(key=lambda x: x["_horizon_score"])
                victim = buckets[donor].pop(0)
            elif cat == "short":
                buckets[donor].sort(key=lambda x: -x["_horizon_score"])
                victim = buckets[donor].pop(0)
            else:  # medium
                # prefer the shortest-score thing from long, or longest from short
                if donor == "long":
                    buckets[donor].sort(key=lambda x: -x["_horizon_score"])
                else:
                    buckets[donor].sort(key=lambda x: x["_horizon_score"])
                victim = buckets[donor].pop(0)
            victim["_horizon_reassigned"] = True
            victim["_original_horizon"] = victim.get("_horizon")
            victim["_horizon"] = cat
            buckets[cat].append(victim)


# ═════════════════════════════════════════════════════════════════════════════
# VERDICT LABELS — granular action vocabulary for the UI
# ═════════════════════════════════════════════════════════════════════════════
#
# The existing UI knows four signals: STRONG BUY / BUY / WATCH / AVOID.
# We enrich this with a second `verdict` field that carries the full action
# semantics. The frontend renders `verdict` when present, falls back to
# `signal` otherwise — so the old UI keeps working and the new UI can show
# the richer label.
# ─────────────────────────────────────────────────────────────────────────────

def verdict_for(action: str, tech_score: int, confidence: str) -> tuple[str, str]:
    """Returns (verdict_label, legacy_signal) — legacy signal is kept for
    backward compatibility with the existing card renderer."""
    if action == "ADD":
        if tech_score >= 72 and confidence == "HIGH":
            return "STRONG BUY", "STRONG BUY"
        return "BUY", "BUY"
    if action == "WAIT_FOR_DIP":
        return "ACCUMULATE ON DIP", "BUY"   # still buy-side so UI treats kindly
    if action == "HOLD":
        return "HOLD", "WATCH"
    if action == "BLACKOUT":
        return "EARNINGS BLACKOUT", "WATCH"
    if action == "TRIM":
        return "BOOK PARTIAL", "AVOID"
    if action == "EXIT":
        return "EXIT", "AVOID"
    if action == "DO_NOT_TRADE":
        return "DO NOT TRADE", "AVOID"
    return "REVIEW", "WATCH"


ACTION_PRIORITY = {
    "ADD":          0,
    "WAIT_FOR_DIP": 1,
    "HOLD":         2,
    "BLACKOUT":     3,
    "TRIM":         4,
    "EXIT":         5,
    "DO_NOT_TRADE": 6,
}


# ═════════════════════════════════════════════════════════════════════════════
# LEGACY PICK CARD BUILDER
# ═════════════════════════════════════════════════════════════════════════════

def _legacy_pick(d: dict, cat: str, rank: int) -> dict:
    """Map a Decision dict into the shape index.html's makeCard() expects,
    enriched with a `verdict` field and sensible entry/exit framing for
    non-buy verdicts so the card still reads cleanly."""
    tech = d.get("tech") or {}
    price = d.get("current_price") or 0
    stop = d.get("stop_loss") or (round(price * 0.94, 2) if price else 0)
    st_t = d.get("st_target") or (round(price * 1.10, 2) if price else 0)
    lt_t = d.get("lt_target") or (round(price * 1.25, 2) if price else 0)

    plan = d.get("tranche_plan") or []
    entry_p = (plan[0].get("price") if plan else None) \
              or d.get("entry_ceiling") or price

    atr = tech.get("atr") or (price * 0.018 if price else 0)
    tech_score = int(tech.get("score") or 0)

    action = d.get("action", "HOLD")
    conf = d.get("confidence", "MEDIUM")
    verdict, legacy_sig = verdict_for(action, tech_score, conf)

    symbol = (d.get("resolved_ticker") or "").replace(".NS", "") or d.get("name", "")
    sector = SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other")

    hold_dur_map = {"short": "5–15 trading days",
                    "medium": "4–12 weeks",
                    "long":   "6–18 months"}

    # Soften the entry/exit labels for non-buy verdicts so the card doesn't
    # scream "BUY" at a stock we're telling the user to sell.
    is_buy_side = action in ("ADD", "WAIT_FOR_DIP")
    entry_title = {
        "ADD":           "🟢 Best Buy Price",
        "WAIT_FOR_DIP":  "🟡 Accumulation Zone",
        "HOLD":          "⚪ Re-entry Reference",
        "BLACKOUT":      "⏸ Post-Results Reference",
        "TRIM":          "✂ Original Entry Reference",
        "EXIT":          "⛔ Exit Reference",
        "DO_NOT_TRADE":  "⚠ Not Tradeable",
    }.get(action, "Reference Price")

    exit_title = {
        "ADD":           "🎯 Target Sell Prices",
        "WAIT_FOR_DIP":  "🎯 Target Sell Prices",
        "HOLD":          "🎯 Active Targets",
        "BLACKOUT":      "🎯 Targets (held through results)",
        "TRIM":          "✂ Book Partial Here",
        "EXIT":          "⛔ No Target — Full Exit",
        "DO_NOT_TRADE":  "— Not Applicable —",
    }.get(action, "🎯 Targets")

    return {
        "rank": rank,
        "symbol": symbol,
        "name": d.get("name", symbol),
        "sector": sector,
        "score": tech_score,
        "signal": legacy_sig,
        "verdict": verdict,               # NEW — rich label
        "action": action,                 # NEW — raw engine action
        "confidence": conf,
        "current_price": price,
        "change_pct": tech.get("change_pct", 0),
        "change": 0,
        "target_price": st_t,
        "stop_loss": stop,
        "risk_reward": d.get("risk_reward") or 0,
        "holding_category": cat,
        "is_buy_side": is_buy_side,
        "scores": (tech.get("components") or {"trend": 0, "momentum": 0,
                                               "volume": 0, "breakout": 0,
                                               "price_action": 0}),
        "indicators": {
            "rsi":          tech.get("rsi"),
            "macd_signal":  "Bullish" if tech.get("trend_label") == "Uptrend" else
                            "Bearish" if tech.get("trend_label") == "Downtrend" else "Neutral",
            "sma_alignment": tech.get("trend_label") or "—",
            "volume_ratio": tech.get("volume_ratio"),
            "week52_pct":   tech.get("pct_from_52h"),
            "atr":          atr,
            "bb_position":  None,
            "adx":          tech.get("adx"),
        },
        "reasons": (d.get("reasons") or [])[:6],
        "narrative": d.get("narrative", ""),
        "trade_plan": {
            "category":  cat,
            "cat_score": tech_score,
            "atr":       atr,
            "atr_pct":   round(atr / price * 100, 2) if price else 0,
            "entry": {
                "title":          entry_title,
                "ideal_price":    entry_p,
                "limit_order":    round(entry_p * 0.9993, 2) if entry_p else 0,
                "acceptable_max": round(price * 1.005, 2) if price else 0,
                "entry_window":   {"short":  "09:15–09:45 AM IST",
                                   "medium": "09:15–10:15 AM IST",
                                   "long":   "09:15 AM IST (GTC)"}.get(cat, "09:15 AM IST"),
                "order_strategy": {"short":  "Limit order, cancel if not filled by 09:45",
                                   "medium": "Patient limit order",
                                   "long":   "GTC limit — no urgency"}.get(cat, "Limit order"),
                "note":           f"Engine action: {action} · {verdict}",
            },
            "exit": {
                "title":               exit_title,
                "target_conservative": round(st_t * 0.95, 2) if st_t else 0,
                "target_ideal":        st_t,
                "target_stretch":      lt_t,
                "upside_conservative": round((st_t * 0.95 - entry_p) / entry_p * 100, 1) if entry_p else 0,
                "upside_ideal":        round((st_t - entry_p) / entry_p * 100, 1) if entry_p else 0,
                "upside_stretch":      round((lt_t - entry_p) / entry_p * 100, 1) if entry_p else 0,
                "hold_min_days":       {"short": 5,  "medium": 30, "long": 180}[cat],
                "hold_max_days":       {"short": 15, "medium": 90, "long": 540}[cat],
                "hold_duration":       hold_dur_map[cat],
                "sell_trigger":        (d.get("narrative") or "")[:160]
                                        or (d.get("reasons") or ["Follow rule engine"])[0],
                "hold_note":           {"short": "Review daily",
                                        "medium": "Review weekly",
                                        "long":   "Review monthly"}[cat],
            },
        },
    }


# ═════════════════════════════════════════════════════════════════════════════
# LEGACY PROJECTION — distributes ALL stocks into the 3 horizon tabs
# ═════════════════════════════════════════════════════════════════════════════

def build_legacy_projection(engine_out: dict) -> dict:
    """
    Bucket EVERY decision into short / medium / long by intrinsic horizon DNA.
    Order each bucket by action priority then by tech score. Every tab is
    always populated as long as the fetcher produced decisions.
    """
    all_d = engine_out.get("all_decisions", [])

    if not all_d:
        return _empty_legacy_block()

    classifier = HorizonClassifier()
    buckets: dict = {"short": [], "medium": [], "long": []}

    # Classify
    for d in all_d:
        verdict = classifier.classify(d)
        d2 = dict(d)  # shallow copy — don't mutate engine output
        d2["_horizon"] = verdict["horizon"]
        d2["_horizon_score"] = verdict["score"]
        d2["_horizon_reasons"] = verdict["reasons"]
        buckets[verdict["horizon"]].append(d2)

    # Safety rebalance so no bucket is starved
    _rebalance_horizons(buckets)

    # Sort each bucket by action priority, then tech score desc
    for cat in buckets:
        buckets[cat].sort(key=lambda d: (
            ACTION_PRIORITY.get(d.get("action", ""), 9),
            -(d.get("tech") or {}).get("score", 0),
        ))

    # Build legacy cards for every bucket — NO caps. Show them all.
    shorts = [_legacy_pick(d, "short", i + 1) for i, d in enumerate(buckets["short"])]
    mediums = [_legacy_pick(d, "medium", i + 1) for i, d in enumerate(buckets["medium"])]
    longs = [_legacy_pick(d, "long", i + 1) for i, d in enumerate(buckets["long"])]

    # top_picks = the actionable buy-side, globally ranked
    buy_side = [d for d in all_d if d.get("action") in ("ADD", "WAIT_FOR_DIP")]
    buy_side.sort(key=lambda d: (
        ACTION_PRIORITY.get(d.get("action", ""), 9),
        -(d.get("tech") or {}).get("score", 0),
    ))
    top_picks = []
    for i, d in enumerate(buy_side[:15]):
        # re-run classifier to get the card's horizon for the top-picks card
        h = classifier.classify(d)["horizon"]
        top_picks.append(_legacy_pick(d, h, i + 1))

    # Watchlist = HOLD / BLACKOUT
    hold_side = [d for d in all_d if d.get("action") in ("HOLD", "BLACKOUT")]
    hold_side.sort(key=lambda d: -(d.get("tech") or {}).get("score", 0))
    watchlist = [{
        "symbol": (d.get("resolved_ticker") or "").replace(".NS", ""),
        "name":   d.get("name"),
        "sector": SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other"),
        "current_price": d.get("current_price"),
        "score":  (d.get("tech") or {}).get("score", 0),
        "signal": "WATCH",
        "verdict": "EARNINGS BLACKOUT" if d.get("action") == "BLACKOUT" else "HOLD",
        "reason": (d.get("reasons") or ["Monitor for entry"])[0],
    } for d in hold_side[:12]]

    # Avoid = EXIT / TRIM / DO_NOT_TRADE
    exit_side = [d for d in all_d if d.get("action") in ("EXIT", "TRIM", "DO_NOT_TRADE")]
    exit_side.sort(key=lambda d: ACTION_PRIORITY.get(d.get("action", ""), 9))
    avoid = [{
        "symbol": (d.get("resolved_ticker") or "").replace(".NS", "") or d.get("name"),
        "name":   d.get("name"),
        "sector": SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other"),
        "current_price": d.get("current_price"),
        "score":  (d.get("tech") or {}).get("score", 0),
        "verdict": verdict_for(d.get("action", "EXIT"),
                               int((d.get("tech") or {}).get("score") or 0),
                               d.get("confidence", "LOW"))[0],
        "reason": (d.get("reasons") or ["Exit signal"])[0],
    } for d in exit_side[:15]]

    # Sector momentum
    sector_bucket: dict = {}
    for d in all_d:
        sym = d.get("resolved_ticker") or ""
        sec = SECTOR_MAP.get(sym, "Other")
        score = (d.get("tech") or {}).get("score")
        if score is not None:
            sector_bucket.setdefault(sec, []).append(score)
    sector_momentum = sorted([
        {"sector": s,
         "score":  int(sum(v) / len(v)),
         "trend":  "up" if sum(v) / len(v) >= 60 else "down" if sum(v) / len(v) < 45 else "neutral",
         "stocks_analyzed": len(v)}
        for s, v in sector_bucket.items() if v
    ], key=lambda x: x["score"], reverse=True)[:12]

    # Breadth
    all_scores = [(d.get("tech") or {}).get("score") for d in all_d]
    all_scores = [s for s in all_scores if s is not None]
    adv = sum(1 for s in all_scores if s >= 55)
    dec = sum(1 for s in all_scores if s < 45)
    unch = len(all_scores) - adv - dec
    k = 3500 / max(len(all_scores), 1)

    return {
        "top_picks":       top_picks,
        "watchlist":       watchlist,
        "avoid":           avoid,
        "sector_momentum": sector_momentum,
        "short_term":  {"label": "Short Term (5–15 days)",   "picks": shorts},
        "medium_term": {"label": "Medium Term (4–12 weeks)", "picks": mediums},
        "long_term":   {"label": "Long Term (6–18 months)",  "picks": longs},
        "market_breadth": {
            "advances":     int(adv * k),
            "declines":     int(dec * k),
            "unchanged":    int(unch * k),
            "new_52w_high": 0,
            "new_52w_low":  0,
        },
        "indices": {
            "NIFTY50":         {"value": 0, "change": 0, "change_pct": 0},
            "SENSEX":          {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_BANK":      {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_IT":        {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_MIDCAP100": {"value": 0, "change": 0, "change_pct": 0},
        },
    }


def _empty_legacy_block() -> dict:
    return {
        "top_picks": [], "watchlist": [], "avoid": [], "sector_momentum": [],
        "short_term":  {"label": "Short Term", "picks": []},
        "medium_term": {"label": "Medium Term", "picks": []},
        "long_term":   {"label": "Long Term",  "picks": []},
        "market_breadth": {"advances": 0, "declines": 0, "unchanged": 0,
                           "new_52w_high": 0, "new_52w_low": 0},
        "indices": {k: {"value": 0, "change": 0, "change_pct": 0}
                    for k in ("NIFTY50", "SENSEX", "NIFTY_BANK",
                              "NIFTY_IT", "NIFTY_MIDCAP100")},
    }


# ═════════════════════════════════════════════════════════════════════════════
# DAILY BRIEF — deterministic end-of-day evaluation (replaces Gemini call)
# ═════════════════════════════════════════════════════════════════════════════

def build_daily_brief(engine_out: dict, legacy: dict) -> dict:
    """
    Deterministic EOD report consumed by the AI Brief tab.
    Never raises — wraps every subsection in its own try/except and
    returns whatever it was able to compute. The minimum output is always
    a headline + market_pulse, so the tab never renders blank.
    """
    try:
        return _build_daily_brief_inner(engine_out, legacy)
    except Exception as e:
        log.error(f"daily_brief failure: {e}\n{traceback.format_exc()}")
        return _minimal_brief(engine_out, f"builder error: {type(e).__name__}")


def _build_daily_brief_inner(engine_out: dict, legacy: dict) -> dict:
    all_d = engine_out.get("all_decisions", [])
    regime = engine_out.get("regime", {}) or {}
    counts = engine_out.get("counts", {}) or {}

    # ── Top movers (gainers / losers) ─────────────────────────────────────────
    movers_up, movers_down = [], []
    try:
        tech_decisions = [d for d in all_d if (d.get("tech") or {}).get("change_pct") is not None]
        tech_decisions.sort(key=lambda d: d["tech"]["change_pct"], reverse=True)
        movers_up = [_brief_mover(d) for d in tech_decisions[:5]]
        tech_decisions.sort(key=lambda d: d["tech"]["change_pct"])
        movers_down = [_brief_mover(d) for d in tech_decisions[:5]]
    except Exception as e:
        log.warning(f"brief movers section failed: {e}")

    # ── Conviction board: highest-score actionable holdings ───────────────────
    conviction = []
    try:
        candidates = [d for d in all_d
                      if d.get("action") in ("ADD", "WAIT_FOR_DIP", "HOLD")
                      and (d.get("tech") or {}).get("score") is not None]
        candidates.sort(key=lambda d: (
            -(d["tech"]["score"]),
            ACTION_PRIORITY.get(d.get("action", ""), 9),
        ))
        for d in candidates[:6]:
            tech = d.get("tech") or {}
            conviction.append({
                "symbol":  (d.get("resolved_ticker") or "").replace(".NS", ""),
                "name":    d.get("name"),
                "sector":  SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other"),
                "action":  d.get("action"),
                "verdict": verdict_for(d.get("action"),
                                       int(tech.get("score") or 0),
                                       d.get("confidence", "MEDIUM"))[0],
                "score":   int(tech.get("score") or 0),
                "rsi":     tech.get("rsi"),
                "trend":   tech.get("trend_label"),
                "reason":  (d.get("reasons") or ["Strong technical setup"])[0],
            })
    except Exception as e:
        log.warning(f"brief conviction section failed: {e}")

    # ── Risk watchlist: near stop, RSI hot, death cross, drawdown ─────────────
    risk_watchlist = []
    try:
        for d in all_d:
            tech = d.get("tech") or {}
            price = d.get("current_price")
            stop = d.get("stop_loss")
            rsi = tech.get("rsi")
            flags = []
            if rsi is not None and rsi >= 78:
                flags.append(f"RSI {rsi:.0f} overbought")
            if rsi is not None and rsi <= 30:
                flags.append(f"RSI {rsi:.0f} oversold")
            if price and stop and price <= stop * 1.03 and price > 0:
                pct = (price - stop) / stop * 100
                flags.append(f"{pct:.1f}% above stop")
            if tech.get("trend_label") == "Downtrend" and (tech.get("score") or 0) < 45:
                flags.append("weak trend + low score")
            if d.get("action") in ("EXIT", "TRIM"):
                flags.append(f"rule → {d['action']}")
            if flags:
                risk_watchlist.append({
                    "symbol":  (d.get("resolved_ticker") or "").replace(".NS", ""),
                    "name":    d.get("name"),
                    "action":  d.get("action"),
                    "flags":   flags,
                    "verdict": verdict_for(d.get("action", "HOLD"),
                                           int(tech.get("score") or 0),
                                           d.get("confidence", "LOW"))[0],
                })
        # Prioritise EXIT/TRIM entries at the top
        risk_watchlist.sort(key=lambda r: ACTION_PRIORITY.get(r.get("action", ""), 9), reverse=True)
        risk_watchlist = risk_watchlist[:8]
    except Exception as e:
        log.warning(f"brief risk section failed: {e}")

    # ── Breakout watch: within 3% of 52w high + solid score ───────────────────
    breakout_watch = []
    try:
        for d in all_d:
            tech = d.get("tech") or {}
            pct_h = tech.get("pct_from_52h")
            score = tech.get("score") or 0
            if pct_h is not None and pct_h <= 3.5 and score >= 55:
                breakout_watch.append({
                    "symbol":     (d.get("resolved_ticker") or "").replace(".NS", ""),
                    "name":       d.get("name"),
                    "pct_from_52h": round(pct_h, 1),
                    "score":      int(score),
                    "in_base":    tech.get("in_base", False),
                    "action":     d.get("action"),
                })
        breakout_watch.sort(key=lambda x: (x["pct_from_52h"], -x["score"]))
        breakout_watch = breakout_watch[:6]
    except Exception as e:
        log.warning(f"brief breakout section failed: {e}")

    # ── Sector heatmap ────────────────────────────────────────────────────────
    sector_heatmap = []
    try:
        sectors: dict = {}
        for d in all_d:
            sym = d.get("resolved_ticker") or ""
            sec = SECTOR_MAP.get(sym, "Other")
            score = (d.get("tech") or {}).get("score")
            if score is None:
                continue
            sectors.setdefault(sec, []).append(score)
        for sec, scores in sectors.items():
            if len(scores) < 1:
                continue
            avg = sum(scores) / len(scores)
            sector_heatmap.append({
                "sector": sec,
                "avg_score": round(avg, 1),
                "count": len(scores),
                "trend": "up" if avg >= 58 else "down" if avg < 45 else "flat",
            })
        sector_heatmap.sort(key=lambda x: -x["avg_score"])
    except Exception as e:
        log.warning(f"brief sector section failed: {e}")

    # ── Action plan: prioritized to-dos for tomorrow's open ───────────────────
    action_plan = []
    try:
        # 1. Exits (urgency 1)
        for d in all_d:
            if d.get("action") == "EXIT":
                action_plan.append(_brief_action_item(d, "EXIT", 1,
                    "Market order at open — stop breached."))
        # 2. Trims (urgency 2)
        for d in all_d:
            if d.get("action") == "TRIM":
                action_plan.append(_brief_action_item(d, "TRIM", 2,
                    "Book 30–50% at open. Trail stop to breakeven on remainder."))
        # 3. Blackouts (urgency 3)
        for d in all_d:
            if d.get("action") == "BLACKOUT":
                e_info = d.get("earnings") or {}
                days = e_info.get("days_to_earnings")
                msg = f"No fresh adds — results in {days}d" if days is not None else "Inside earnings window"
                action_plan.append(_brief_action_item(d, "BLACKOUT", 3, msg))
        # 4. Top-conviction ADDs (urgency 4)
        adds = [d for d in all_d if d.get("action") == "ADD"]
        adds.sort(key=lambda d: -(d.get("tech") or {}).get("score", 0))
        for d in adds[:5]:
            entry = d.get("entry_ceiling") or d.get("current_price") or 0
            action_plan.append(_brief_action_item(d, "ADD", 4,
                f"GTC limit ≤ ₹{entry:,.0f}" if entry else "GTC limit at entry ceiling"))
    except Exception as e:
        log.warning(f"brief action_plan section failed: {e}")

    # ── Key insight: composed from rule matches, not a string lookup ─────────
    insight = _build_insight(regime, counts, sector_heatmap, breakout_watch,
                             risk_watchlist, all_d)

    # ── Headline ──────────────────────────────────────────────────────────────
    headline = _build_headline(regime, counts)

    # ── Narrative bullets: 3-5 declarative statements about today ─────────────
    narrative = _build_narrative_bullets(regime, counts, conviction, risk_watchlist,
                                         breakout_watch, sector_heatmap, all_d)

    return {
        "generated_at":    datetime.now(IST).isoformat(),
        "date":            date.today().strftime("%Y-%m-%d"),
        "source":          "rules_engine_eod",
        "headline":        headline,
        "regime": {
            "label":        regime.get("label", "UNKNOWN"),
            "breadth_pct":  regime.get("breadth_pct", 0),
            "momentum_pct": regime.get("momentum_pct", 0),
            "volatility":   regime.get("volatility", 0),
            "notes":        regime.get("notes", ""),
        },
        "key_insight":     insight,
        "narrative":       narrative,
        "market_pulse": {
            "total_analyzed":      len(all_d),
            "strong_buys":         sum(1 for d in all_d if d.get("action") == "ADD"
                                        and d.get("confidence") == "HIGH"),
            "actionable_buys":     counts.get("ADD", 0) + counts.get("WAIT_FOR_DIP", 0),
            "holds":               counts.get("HOLD", 0),
            "risk_alerts":         counts.get("EXIT", 0) + counts.get("TRIM", 0),
            "earnings_blackouts":  counts.get("BLACKOUT", 0),
            "do_not_trade":        counts.get("DO_NOT_TRADE", 0),
        },
        "top_movers_up":    movers_up,
        "top_movers_down":  movers_down,
        "conviction_board": conviction,
        "risk_watchlist":   risk_watchlist,
        "breakout_watch":   breakout_watch,
        "sector_heatmap":   sector_heatmap[:12],
        "action_plan":      action_plan[:15],
    }


def _brief_mover(d: dict) -> dict:
    tech = d.get("tech") or {}
    return {
        "symbol": (d.get("resolved_ticker") or "").replace(".NS", ""),
        "name":   d.get("name"),
        "change_pct": round(tech.get("change_pct") or 0, 2),
        "price":  d.get("current_price"),
        "action": d.get("action"),
        "score":  int(tech.get("score") or 0),
    }


def _brief_action_item(d: dict, action_type: str, priority: int, instruction: str) -> dict:
    tech = d.get("tech") or {}
    return {
        "priority":    priority,
        "type":        action_type,
        "symbol":      (d.get("resolved_ticker") or "").replace(".NS", ""),
        "name":        d.get("name"),
        "price":       d.get("current_price"),
        "instruction": instruction,
        "reason":      (d.get("reasons") or ["Engine rule fired"])[0],
        "confidence":  d.get("confidence", "MEDIUM"),
        "verdict":     verdict_for(action_type,
                                   int(tech.get("score") or 0),
                                   d.get("confidence", "MEDIUM"))[0],
    }


def _build_headline(regime: dict, counts: dict) -> str:
    """Composable, human-feeling headline — rule-driven, no templates."""
    label = regime.get("label", "UNKNOWN")
    breadth = regime.get("breadth_pct", 0)
    adds = counts.get("ADD", 0)
    exits = counts.get("EXIT", 0)
    trims = counts.get("TRIM", 0)
    blackouts = counts.get("BLACKOUT", 0)

    # Pick the most newsworthy fact to lead with
    if exits + trims >= 5:
        return f"{label} regime — {exits} exits and {trims} trims on the table, defensive day"
    if adds >= 6 and breadth >= 55:
        return f"{label} regime with {breadth:.0f}% breadth — {adds} fresh BUY setups cleared the gates"
    if adds >= 3:
        return f"{label} tape — {adds} ADDs, {exits + trims} risk actions, {blackouts} in earnings blackout"
    if label == "BEAR":
        return f"Risk-off: {breadth:.0f}% breadth, tighten stops and let cash work"
    if label == "SIDEWAYS":
        return f"Sideways grind — no directional edge, let existing positions work"
    if label == "BULL":
        return f"{label} regime holding — {breadth:.0f}% breadth, selective adds only"
    return f"{label} regime — mixed signals across the book"


def _build_insight(regime: dict, counts: dict, sector_heatmap: list,
                   breakout_watch: list, risk_watchlist: list, all_d: list) -> str:
    """Composite insight — combines regime-language with evidence from
    today's decision ledger. Written as if by a senior analyst."""
    label = regime.get("label", "UNKNOWN")
    breadth = regime.get("breadth_pct", 0)
    momentum = regime.get("momentum_pct", 0)
    parts: list[str] = []

    # Regime framing
    if label == "BULL" and breadth >= 60:
        parts.append(
            f"Broad-based strength: {breadth:.0f}% of the universe is above its 50-day "
            f"average and median momentum is +{momentum:.1f}%. This is the backdrop "
            f"where adding quality on 2–3% pullbacks usually pays.")
    elif label == "BULL" and breadth < 55:
        parts.append(
            f"Narrow rally: breadth is only {breadth:.0f}% despite a positive tape. "
            f"Leadership is concentrated — stay with the strongest names, avoid "
            f"chasing laggards hoping for a catch-up trade.")
    elif label == "BEAR":
        parts.append(
            f"Risk-off regime: {breadth:.0f}% breadth, median momentum {momentum:+.1f}%. "
            f"Raise cash, honour stops, and resist bottom-fishing until breadth "
            f"recovers above 55%.")
    elif label == "SIDEWAYS":
        parts.append(
            f"Range-bound tape with {breadth:.0f}% breadth and flat momentum. "
            f"No directional edge — let existing positions work, avoid fresh "
            f"breakout trades, and save dry powder for the next regime shift.")
    else:
        parts.append(
            f"Regime unresolved ({regime.get('notes', 'too few signals')}). "
            f"Stick to positions already validated by the rule ladder.")

    # Evidence 1 — breakout watch
    if breakout_watch:
        names = ", ".join(b["symbol"] for b in breakout_watch[:3])
        parts.append(
            f"Breakout ladder: {names} are within 3–4% of 52-week highs with scores "
            f"above 55 — these are the names to watch for a volume push tomorrow.")

    # Evidence 2 — risk ledger
    risk_exits = [r for r in risk_watchlist if r.get("action") in ("EXIT", "TRIM")]
    if len(risk_exits) >= 3:
        parts.append(
            f"Risk ledger is active: {len(risk_exits)} holdings flagged for EXIT or "
            f"TRIM. Clear these at open before deploying into anything new — "
            f"dirty cash is cash that still thinks it's winning.")
    elif risk_watchlist:
        parts.append(
            f"{len(risk_watchlist)} holdings on the risk watchlist — stress signals "
            f"present but not yet rule-triggered. Review stops tomorrow.")

    # Evidence 3 — sector rotation
    if sector_heatmap and len(sector_heatmap) >= 3:
        top = sector_heatmap[0]
        bot = sector_heatmap[-1]
        spread = top["avg_score"] - bot["avg_score"]
        if spread >= 15:
            parts.append(
                f"Sector dispersion is wide: {top['sector']} leads at {top['avg_score']:.0f} "
                f"while {bot['sector']} lags at {bot['avg_score']:.0f} ({spread:.0f}-point "
                f"spread). This is a stock-picker's tape, not an index-buyer's.")
        else:
            parts.append(
                f"Sectors are tightly bunched (spread only {spread:.0f} points) — "
                f"no clear rotation to lean on yet.")

    # Evidence 4 — earnings blackout flag
    if counts.get("BLACKOUT", 0) >= 2:
        parts.append(
            f"{counts['BLACKOUT']} holdings are in the earnings blackout window. "
            f"No fresh adds on those names until results are printed and digested.")

    return " ".join(parts)


def _build_narrative_bullets(regime: dict, counts: dict, conviction: list,
                             risk_watchlist: list, breakout_watch: list,
                             sector_heatmap: list, all_d: list) -> list[str]:
    """Short declarative bullets — each is one fact, in analyst voice."""
    bullets: list[str] = []

    breadth = regime.get("breadth_pct", 0)
    bullets.append(
        f"Regime: {regime.get('label', 'UNKNOWN')} · breadth {breadth:.0f}% · "
        f"median momentum {regime.get('momentum_pct', 0):+.1f}% · "
        f"median ATR {regime.get('volatility', 0):.1f}%")

    tot = len(all_d)
    if tot:
        adds = counts.get("ADD", 0)
        wfd = counts.get("WAIT_FOR_DIP", 0)
        hold = counts.get("HOLD", 0)
        bullets.append(
            f"Book: {adds} ADD · {wfd} ACCUMULATE · {hold} HOLD · "
            f"{counts.get('TRIM', 0)} TRIM · {counts.get('EXIT', 0)} EXIT · "
            f"{counts.get('BLACKOUT', 0)} BLACKOUT · "
            f"{counts.get('DO_NOT_TRADE', 0)} DO_NOT_TRADE")

    if conviction:
        names = ", ".join(c["symbol"] for c in conviction[:4])
        bullets.append(f"Highest conviction today: {names}")

    if breakout_watch:
        n = len(breakout_watch)
        bullets.append(
            f"{n} setup{'s' if n > 1 else ''} pressing 52-week highs — "
            f"watch for volume confirmation.")

    if risk_watchlist:
        n = len(risk_watchlist)
        bullets.append(
            f"{n} name{'s' if n > 1 else ''} flagged on the risk ledger — "
            f"review before market open.")

    if sector_heatmap:
        top = sector_heatmap[0]
        bullets.append(
            f"Strongest sector: {top['sector']} (avg score {top['avg_score']:.0f} across "
            f"{top['count']} names)")

    return bullets


def _minimal_brief(engine_out: dict, reason: str) -> dict:
    """Last-resort brief when the full builder fails — never empty."""
    regime = engine_out.get("regime", {}) or {}
    counts = engine_out.get("counts", {}) or {}
    all_d = engine_out.get("all_decisions", [])
    return {
        "generated_at": datetime.now(IST).isoformat(),
        "date": date.today().strftime("%Y-%m-%d"),
        "source": "rules_engine_eod_degraded",
        "headline": _build_headline(regime, counts),
        "regime": {
            "label":        regime.get("label", "UNKNOWN"),
            "breadth_pct":  regime.get("breadth_pct", 0),
            "momentum_pct": regime.get("momentum_pct", 0),
            "volatility":   regime.get("volatility", 0),
            "notes":        f"degraded: {reason}",
        },
        "key_insight": (f"End-of-day evaluation ran in degraded mode "
                        f"({reason}). {len(all_d)} stocks scored. "
                        f"Counts: ADD={counts.get('ADD', 0)} · "
                        f"HOLD={counts.get('HOLD', 0)} · "
                        f"EXIT={counts.get('EXIT', 0)}."),
        "narrative": [
            f"Regime: {regime.get('label', 'UNKNOWN')}",
            f"{len(all_d)} holdings evaluated",
            f"Full brief unavailable — reason: {reason}",
        ],
        "market_pulse": {
            "total_analyzed":     len(all_d),
            "actionable_buys":    counts.get("ADD", 0) + counts.get("WAIT_FOR_DIP", 0),
            "risk_alerts":        counts.get("EXIT", 0) + counts.get("TRIM", 0),
            "earnings_blackouts": counts.get("BLACKOUT", 0),
        },
        "top_movers_up": [], "top_movers_down": [],
        "conviction_board": [], "risk_watchlist": [],
        "breakout_watch": [], "sector_heatmap": [], "action_plan": [],
    }


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

def _safe_date() -> str:
    return date.today().strftime("%Y-%m-%d")


def main() -> int:
    log.info("=" * 60)
    log.info("  StockSage India v3.2 — Rule Engine Orchestrator")
    log.info(f"  {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info("=" * 60)

    if not CONFIG_PATH.exists():
        log.error(f"Missing config: {CONFIG_PATH}")
        return _write_empty_output("config_missing")

    cfg = load_config(str(CONFIG_PATH))
    if not cfg.get("holdings"):
        log.error("Config has no holdings")
        return _write_empty_output("no_holdings")

    log.info(f"[1/4] Loaded {len(cfg['holdings'])} focused holdings")

    # ── Run engine on the focused portfolio ──
    try:
        fetcher = ResilientFetcher(db_path=DB_PATH)
    except Exception as e:
        log.error(f"Fetcher init failed: {e}")
        return _write_empty_output(f"fetcher_init_error: {e}")

    analyzer = PortfolioAnalyzer(cfg, fetcher)
    log.info("[2/4] Running rule engine over focused portfolio...")
    try:
        engine_out = analyzer.run()
    except Exception as e:
        log.exception(f"Engine crashed: {e}")
        return _write_empty_output(f"engine_error: {e}")

    log.info(f"   regime : {engine_out['regime']['label']} "
             f"({engine_out['regime']['breadth_pct']}% breadth)")
    for a, n in engine_out["counts"].items():
        if n > 0:
            log.info(f"   {a:<14} {n}")

    # ── Build legacy projection (distributes ALL stocks into 3 tabs) ──
    log.info("[3/4] Building legacy projection (horizon DNA classifier)...")
    try:
        legacy = build_legacy_projection(engine_out)
        log.info(f"   short  : {len(legacy.get('short_term', {}).get('picks', []))}")
        log.info(f"   medium : {len(legacy.get('medium_term', {}).get('picks', []))}")
        log.info(f"   long   : {len(legacy.get('long_term', {}).get('picks', []))}")
    except Exception as e:
        log.warning(f"Legacy projection failed: {e}; shipping engine shape only")
        legacy = _empty_legacy_block()

    # ── Build deterministic daily brief for the AI tab ──
    log.info("[4/4] Building daily brief (EOD evaluation)...")
    try:
        daily_brief = build_daily_brief(engine_out, legacy)
        log.info(f"   headline       : {daily_brief.get('headline', '—')}")
        log.info(f"   conviction     : {len(daily_brief.get('conviction_board', []))}")
        log.info(f"   risk watch     : {len(daily_brief.get('risk_watchlist', []))}")
        log.info(f"   breakouts      : {len(daily_brief.get('breakout_watch', []))}")
        log.info(f"   action items   : {len(daily_brief.get('action_plan', []))}")
    except Exception as e:
        log.warning(f"daily_brief builder exception: {e}")
        daily_brief = _minimal_brief(engine_out, str(e))

    # ── Compose final predictions.json ──
    out = {
        "generated_at":     datetime.now(IST).isoformat(),
        "market_date":      _safe_date(),
        "analysis_version": "3.2-rules+horizon+brief",
        "stocks_analyzed":  len(engine_out.get("all_decisions", [])),
        "engine":           "rules_engine_v3",
        "portfolio":        engine_out,
        "daily_brief":      daily_brief,     # NEW — EOD evaluation for AI tab
        **legacy,
    }

    try:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, default=str)
        log.info(f"✅ wrote {OUTPUT_PATH}")
    except Exception as e:
        log.error(f"Write failed: {e}")
        return 1

    # ── Summary ──
    actionable = (engine_out["counts"].get("ADD", 0)
                  + engine_out["counts"].get("EXIT", 0)
                  + engine_out["counts"].get("TRIM", 0)
                  + engine_out["counts"].get("BLACKOUT", 0))
    log.info(f"   actionable holdings today: {actionable}")
    log.info(f"   engine buckets: {dict(engine_out['counts'])}")

    return 0


def _write_empty_output(reason: str) -> int:
    """Graceful degradation — always write a valid file so the frontend doesn't 404."""
    empty_brief = _minimal_brief({"regime": {"label": "UNKNOWN"},
                                  "counts": {}, "all_decisions": []}, reason)
    payload = {
        "generated_at":     datetime.now(IST).isoformat(),
        "market_date":      _safe_date(),
        "analysis_version": "3.2-rules+horizon+brief",
        "stocks_analyzed":  0,
        "engine":           "rules_engine_v3",
        "status":           "degraded",
        "reason":           reason,
        "portfolio": {
            "generated_at": datetime.now(IST).isoformat(),
            "regime":       {"label": "UNKNOWN", "breadth_pct": 0,
                             "momentum_pct": 0, "volatility": 0, "notes": reason},
            "counts":       {},
            "buckets":      {},
            "all_decisions": [],
        },
        "daily_brief":      empty_brief,
        **_empty_legacy_block(),
    }
    try:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.warning(f"wrote degraded predictions.json — reason: {reason}")
    except Exception as e:
        log.error(f"even the degraded write failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
