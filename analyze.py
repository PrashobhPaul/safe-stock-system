"""
analyze.py — StockSage India v3 (Rules Engine)
================================================
Orchestrator. Runs:
  1. ResilientFetcher over the full universe (focused portfolio + broad NSE).
  2. rules_engine.PortfolioAnalyzer over the focused portfolio holdings.
  3. A legacy-shape projection for the existing index.html scanner UI.

Writes predictions.json with BOTH shapes so:
  • portfolio.html consumes the new `portfolio` block
  • index.html consumes the legacy `top_picks/short_term/...` block

Zero LLM. Zero paid services. Every failure is handled — the pipeline
NEVER crashes GitHub Actions.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pytz

from resilient_fetcher import ResilientFetcher
from rules_engine import (
    PortfolioAnalyzer,
    TechnicalScorer,
    TechnicalSnapshot,
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
    # ── Core blue chips ────────────────────────────────────────────────
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
    # ── Focused portfolio additions (from focused_portfolio.yml) ──────
    "ECLERX.NS", "CHAMBLFERT.NS", "ARE&M.NS", "HYUNDAI.NS", "SBICARD.NS",
    "TATAPOWER.NS", "KPITTECH.NS", "TATACAP.NS", "JBMA.NS", "EXIDEIND.NS",
    "BLS.NS", "ETERNAL.NS", "TARIL.NS", "IREDA.NS", "POONAWALLA.NS",
    "TEXRAIL.NS", "SERVOTECH.NS", "SIEMENS.NS", "DEEPAKNTR.NS", "ZENTEC.NS",
    "GREENPOWER.NS", "ZINKA.NS", "CROMPTON.NS", "DMART.NS", "TATAINVEST.NS",
    "COCHINSHIP.NS", "AURIONPRO.NS", "SUZLON.NS", "EASEMYTRIP.NS",
    "HUHTAMAKI.NS", "GREAVESCOT.NS", "RPOWER.NS", "OLAELEC.NS", "PCJEWELLER.NS",
    "CYIENTDLM.NS", "ADSL.NS", "BSE.NS", "NELCO.NS", "SWIGGY.NS", "URBAN.NS",
    "MON150BEES.NS",  # alternate ETF ticker candidate
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


# ─────────────────────────────────────────────────────────────────────────────
# Legacy projection — derives old-shape top_picks/short_term/etc from the new
# engine output so the existing index.html keeps working without any edits.
# ─────────────────────────────────────────────────────────────────────────────

def _pick_category(tech: dict) -> str:
    """Simple heuristic bucket for the legacy Short/Medium/Long panels."""
    if not tech:
        return "medium"
    trend = (tech.get("components") or {}).get("trend", 0)
    brk   = (tech.get("components") or {}).get("breakout", 0)
    in_base = tech.get("in_base", False)
    if brk >= 8 or (tech.get("volume_ratio") or 0) >= 1.5:
        return "short"
    if trend >= 20 and not in_base:
        return "long"
    return "medium"


def _tranche_entry_price(decision: dict) -> Optional[float]:
    plan = decision.get("tranche_plan") or []
    if plan:
        return plan[0].get("price")
    return decision.get("entry_ceiling") or decision.get("current_price")


def _legacy_pick(d: dict, cat: str, rank: int) -> dict:
    """Map a Decision dict to the shape index.html's makeCard() expects."""
    tech = d.get("tech") or {}
    price = d.get("current_price") or 0
    stop = d.get("stop_loss") or round(price * 0.94, 2) if price else 0
    entry_p = _tranche_entry_price(d) or price
    st_t = d.get("st_target") or (round(price * 1.10, 2) if price else 0)
    lt_t = d.get("lt_target") or (round(price * 1.25, 2) if price else 0)

    atr = tech.get("atr") or (price * 0.018 if price else 0)

    hold_dur_map = {"short": "5–15 trading days",
                    "medium": "4–12 weeks",
                    "long":   "6–18 months"}

    # Legacy signal mapping
    action = d.get("action", "HOLD")
    legacy_sig = {
        "ADD": "BUY" if (tech.get("score") or 0) < 75 else "STRONG BUY",
        "WAIT_FOR_DIP": "WATCH",
        "HOLD": "WATCH",
        "TRIM": "WATCH",
        "EXIT": "AVOID",
        "BLACKOUT": "WATCH",
        "DO_NOT_TRADE": "AVOID",
    }.get(action, "WATCH")

    symbol = (d.get("resolved_ticker") or "").replace(".NS", "") or d.get("name", "")

    return {
        "rank": rank,
        "symbol": symbol,
        "name": d.get("name", symbol),
        "sector": SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other"),
        "score": (tech.get("score") or 0),
        "signal": legacy_sig,
        "confidence": d.get("confidence", "MEDIUM"),
        "current_price": price,
        "change_pct": tech.get("change_pct", 0),
        "change": 0,
        "target_price": st_t,
        "stop_loss": stop,
        "risk_reward": d.get("risk_reward") or 0,
        "holding_category": cat,
        "scores": (tech.get("components") or {"trend":0,"momentum":0,"volume":0,"breakout":0,"price_action":0}),
        "indicators": {
            "rsi":          tech.get("rsi"),
            "macd_signal":  "Bullish" if (tech.get("trend_label") == "Uptrend") else
                             "Bearish" if (tech.get("trend_label") == "Downtrend") else "Neutral",
            "sma_alignment": tech.get("trend_label") or "—",
            "volume_ratio": tech.get("volume_ratio"),
            "week52_pct":   tech.get("pct_from_52h"),
            "atr":          atr,
            "bb_position":  None,
            "adx":          tech.get("adx"),
        },
        "reasons": d.get("reasons", [])[:6],
        "trade_plan": {
            "category":  cat,
            "cat_score": (tech.get("score") or 0),
            "atr":       atr,
            "atr_pct":   round(atr / price * 100, 2) if price else 0,
            "entry": {
                "ideal_price":    entry_p,
                "limit_order":    round(entry_p * 0.9993, 2) if entry_p else 0,
                "acceptable_max": round(price * 1.005, 2) if price else 0,
                "entry_window":   {"short":  "09:15–09:45 AM IST",
                                   "medium": "09:15–10:15 AM IST",
                                   "long":   "09:15 AM IST (GTC)"}.get(cat, "09:15 AM IST"),
                "order_strategy": {"short":  "Limit order, cancel if not filled by 09:45",
                                   "medium": "Patient limit order",
                                   "long":   "GTC limit — no urgency"}.get(cat, "Limit order"),
                "note":           f"Engine action: {action}",
            },
            "exit": {
                "target_conservative": round(st_t * 0.95, 2) if st_t else 0,
                "target_ideal":        st_t,
                "target_stretch":      lt_t,
                "upside_conservative": round((st_t * 0.95 - entry_p) / entry_p * 100, 1) if entry_p else 0,
                "upside_ideal":        round((st_t - entry_p) / entry_p * 100, 1) if entry_p else 0,
                "upside_stretch":      round((lt_t - entry_p) / entry_p * 100, 1) if entry_p else 0,
                "hold_min_days":       {"short": 5,  "medium": 30, "long": 180}[cat],
                "hold_max_days":       {"short": 15, "medium": 90, "long": 540}[cat],
                "hold_duration":       hold_dur_map[cat],
                "sell_trigger":        d.get("narrative", "")[:160],
                "hold_note":           {"short":"Review daily","medium":"Review weekly","long":"Review monthly"}[cat],
            },
        },
    }


def build_legacy_projection(engine_out: dict) -> dict:
    """
    Map the engine's portfolio buckets back to the legacy
    index.html shape: top_picks, short_term, medium_term, long_term,
    watchlist, avoid, sector_momentum.
    """
    all_d = engine_out.get("all_decisions", [])
    # Separate: actionable buy-side vs hold-side vs exit-side
    buy_side = [d for d in all_d if d["action"] in ("ADD", "WAIT_FOR_DIP")]
    exit_side = [d for d in all_d if d["action"] in ("EXIT", "TRIM", "DO_NOT_TRADE")]
    hold_side = [d for d in all_d if d["action"] in ("HOLD", "BLACKOUT")]

    # Sort buy_side by tech score descending
    buy_side.sort(key=lambda d: (d.get("tech") or {}).get("score", 0), reverse=True)

    shorts, mediums, longs, top_picks = [], [], [], []
    for i, d in enumerate(buy_side):
        cat = _pick_category(d.get("tech"))
        card = _legacy_pick(d, cat, rank=len(top_picks) + 1)
        top_picks.append(card)
        if cat == "short" and len(shorts) < 5:
            shorts.append(_legacy_pick(d, "short", len(shorts) + 1))
        elif cat == "medium" and len(mediums) < 5:
            mediums.append(_legacy_pick(d, "medium", len(mediums) + 1))
        elif cat == "long" and len(longs) < 5:
            longs.append(_legacy_pick(d, "long", len(longs) + 1))
        if len(top_picks) >= 15:
            break

    # Backfill any category that's light
    def _fill(target: list, cat: str):
        if len(target) >= 3:
            return
        for d in buy_side:
            if len(target) >= 5:
                break
            sym = (d.get("resolved_ticker") or "").replace(".NS", "")
            if any(x["symbol"] == sym for x in target):
                continue
            target.append(_legacy_pick(d, cat, len(target) + 1))

    _fill(shorts, "short"); _fill(mediums, "medium"); _fill(longs, "long")

    watchlist = [{
        "symbol": (d.get("resolved_ticker") or "").replace(".NS", ""),
        "name":   d.get("name"),
        "sector": SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other"),
        "current_price": d.get("current_price"),
        "score":  (d.get("tech") or {}).get("score", 0),
        "signal": "WATCH",
        "reason": (d.get("reasons") or ["Monitor for entry"])[0],
    } for d in hold_side[:10]]

    avoid = [{
        "symbol": (d.get("resolved_ticker") or "").replace(".NS", "") or d.get("name"),
        "name":   d.get("name"),
        "sector": SECTOR_MAP.get(d.get("resolved_ticker") or "", "Other"),
        "current_price": d.get("current_price"),
        "score":  (d.get("tech") or {}).get("score", 0),
        "reason": (d.get("reasons") or ["Exit signal"])[0],
    } for d in exit_side[:10]]

    # Sector momentum from tech scores
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

    # Breadth estimate
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
            "advances": int(adv * k),
            "declines": int(dec * k),
            "unchanged": int(unch * k),
            "new_52w_high": 0,
            "new_52w_low": 0,
        },
        "indices": {
            "NIFTY50":         {"value": 0, "change": 0, "change_pct": 0},
            "SENSEX":          {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_BANK":      {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_IT":        {"value": 0, "change": 0, "change_pct": 0},
            "NIFTY_MIDCAP100": {"value": 0, "change": 0, "change_pct": 0},
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def _safe_date() -> str:
    return date.today().strftime("%Y-%m-%d")


def main() -> int:
    log.info("=" * 60)
    log.info("  StockSage India v3 — Rule Engine Orchestrator")
    log.info(f"  {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info("=" * 60)

    if not CONFIG_PATH.exists():
        log.error(f"Missing config: {CONFIG_PATH}")
        return _write_empty_output("config_missing")

    cfg = load_config(str(CONFIG_PATH))
    if not cfg.get("holdings"):
        log.error("Config has no holdings")
        return _write_empty_output("no_holdings")

    log.info(f"[1/3] Loaded {len(cfg['holdings'])} focused holdings")

    # ── Run engine on the focused portfolio ──
    try:
        fetcher = ResilientFetcher(db_path=DB_PATH)
    except Exception as e:
        log.error(f"Fetcher init failed: {e}")
        return _write_empty_output(f"fetcher_init_error: {e}")

    analyzer = PortfolioAnalyzer(cfg, fetcher)
    log.info("[2/3] Running rule engine over focused portfolio...")
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

    # ── Build legacy projection for index.html compatibility ──
    log.info("[3/3] Building legacy projection for index.html...")
    try:
        legacy = build_legacy_projection(engine_out)
    except Exception as e:
        log.warning(f"Legacy projection failed: {e}; shipping new shape only")
        legacy = {}

    # ── Compose final predictions.json ──
    out = {
        "generated_at":     datetime.now(IST).isoformat(),
        "market_date":      _safe_date(),
        "analysis_version": "3.0-rules",
        "stocks_analyzed":  len(engine_out.get("all_decisions", [])),
        "engine":           "rules_engine_v3",
        "portfolio":        engine_out,   # ← primary shape for portfolio.html
        **legacy,                          # ← legacy shape for index.html
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
    payload = {
        "generated_at":     datetime.now(IST).isoformat(),
        "market_date":      _safe_date(),
        "analysis_version": "3.0-rules",
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
        "top_picks": [], "watchlist": [], "avoid": [],
        "sector_momentum": [],
        "short_term": {"label": "Short Term", "picks": []},
        "medium_term": {"label": "Medium Term", "picks": []},
        "long_term": {"label": "Long Term", "picks": []},
        "indices": {"NIFTY50":{"value":0,"change":0,"change_pct":0},
                    "SENSEX":{"value":0,"change":0,"change_pct":0},
                    "NIFTY_BANK":{"value":0,"change":0,"change_pct":0},
                    "NIFTY_IT":{"value":0,"change":0,"change_pct":0},
                    "NIFTY_MIDCAP100":{"value":0,"change":0,"change_pct":0}},
        "market_breadth": {"advances":0,"declines":0,"unchanged":0,
                           "new_52w_high":0,"new_52w_low":0},
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
