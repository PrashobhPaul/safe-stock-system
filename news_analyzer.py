"""
news_analyzer.py — rule-based sentiment, ticker matching, sector tagging
=========================================================================
Zero LLM. Uses keyword lexicons + strict verbatim ticker/company matching.
"""
from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from typing import Iterable

log = logging.getLogger("news_analyzer")

# ──────────────────────────────────────────────────────────────────────────────
# SENTIMENT LEXICON — financial news tuned
# ──────────────────────────────────────────────────────────────────────────────

BULLISH_TERMS = {
    # strong (weight 2)
    "surge": 2, "soars": 2, "soar": 2, "rally": 2, "rallies": 2, "breakout": 2,
    "multibagger": 2, "record high": 2, "all-time high": 2, "52-week high": 2,
    "upgrade": 2, "upgraded": 2, "buy rating": 2, "outperform": 2,
    "beats estimates": 2, "beat estimates": 2, "beats forecast": 2,
    "strong results": 2, "robust growth": 2, "bumper": 2,
    # moderate (weight 1)
    "rises": 1, "rose": 1, "gains": 1, "gained": 1, "jump": 1, "jumps": 1,
    "climbs": 1, "advances": 1, "higher": 1, "up": 1, "positive": 1,
    "profit": 1, "profits": 1, "growth": 1, "expansion": 1, "expand": 1,
    "order win": 1, "contract win": 1, "new order": 1, "bags order": 1,
    "dividend": 1, "buyback": 1, "bonus": 1, "stake": 1, "acquisition": 1,
    "approves": 1, "approved": 1, "launch": 1, "launches": 1, "milestone": 1,
    "boost": 1, "boosted": 1, "optimistic": 1, "bullish": 1, "recovery": 1,
    "outlook positive": 1, "raises guidance": 1, "hikes target": 1,
}

BEARISH_TERMS = {
    # strong (weight 2)
    "crash": 2, "plunge": 2, "plunges": 2, "slump": 2, "slumps": 2,
    "tumble": 2, "tumbles": 2, "sinks": 2, "collapse": 2, "collapses": 2,
    "downgrade": 2, "downgraded": 2, "sell rating": 2, "underperform": 2,
    "misses estimates": 2, "miss estimates": 2, "misses forecast": 2,
    "weak results": 2, "poor results": 2, "disappoints": 2, "disappointing": 2,
    "52-week low": 2, "fraud": 2, "scam": 2, "probe": 2, "raid": 2, "raids": 2,
    "default": 2, "defaults": 2, "bankruptcy": 2, "insolvency": 2,
    "ban": 2, "banned": 2, "penalty": 2, "fine": 2, "lawsuit": 2, "sued": 2,
    # moderate (weight 1)
    "falls": 1, "fell": 1, "drops": 1, "dropped": 1, "decline": 1, "declines": 1,
    "slips": 1, "slides": 1, "lower": 1, "down": 1, "negative": 1,
    "loss": 1, "losses": 1, "weak": 1, "weakness": 1, "pressure": 1,
    "concern": 1, "concerns": 1, "worry": 1, "worries": 1, "caution": 1,
    "cautious": 1, "bearish": 1, "risk": 1, "risks": 1, "headwinds": 1,
    "slowdown": 1, "slowing": 1, "contraction": 1, "cuts": 1, "cut": 1,
    "reduce": 1, "reduces": 1, "warns": 1, "warning": 1, "delay": 1, "delays": 1,
    "miss": 1, "lowers guidance": 1, "profit taking": 1, "correction": 1,
}

MACRO_BULLISH = {
    "rate cut": 2, "rate cuts": 2, "liquidity boost": 2, "stimulus": 2,
    "gdp growth": 2, "inflation eases": 2, "inflation cools": 2,
    "fii inflow": 2, "dii buying": 2, "foreign inflow": 1, "strong demand": 1,
    "policy support": 1, "reform": 1, "reforms": 1, "tax cut": 2,
    "budget boost": 2, "capex push": 2,
}

MACRO_BEARISH = {
    "rate hike": 2, "rate hikes": 2, "inflation surge": 2, "inflation rises": 2,
    "recession": 2, "slowdown fears": 2, "fii outflow": 2, "fii selling": 2,
    "crude surges": 2, "oil spike": 2, "rupee weakens": 2, "rupee falls": 2,
    "geopolitical": 1, "war": 2, "conflict": 1, "sanctions": 1, "trade war": 2,
    "tariff": 1, "tariffs": 1, "election uncertainty": 1, "tax hike": 2,
}

NEGATION = {"no", "not", "without", "never", "isn't", "won't", "doesn't", "didn't"}

# ──────────────────────────────────────────────────────────────────────────────
# STOCK REGISTRY — company name + aliases for strict verbatim matching
# ──────────────────────────────────────────────────────────────────────────────

STOCK_ALIASES: dict[str, list[str]] = {
    # symbol (without .NS) → list of names/aliases to match verbatim in headline/summary
    "RELIANCE":    ["Reliance Industries", "Reliance Ind", "RIL", "Mukesh Ambani"],
    "TCS":         ["Tata Consultancy", "TCS"],
    "INFY":        ["Infosys"],
    "HDFCBANK":    ["HDFC Bank"],
    "ICICIBANK":   ["ICICI Bank"],
    "KOTAKBANK":   ["Kotak Mahindra Bank", "Kotak Bank"],
    "AXISBANK":    ["Axis Bank"],
    "SBIN":        ["State Bank of India", "SBI"],
    "BAJFINANCE":  ["Bajaj Finance"],
    "BAJAJFINSV":  ["Bajaj Finserv"],
    "LT":          ["Larsen & Toubro", "L&T"],
    "ITC":         ["ITC Ltd", "ITC "],
    "HINDUNILVR":  ["Hindustan Unilever", "HUL"],
    "BHARTIARTL":  ["Bharti Airtel", "Airtel"],
    "MARUTI":      ["Maruti Suzuki", "Maruti"],
    "TATAMOTORS":  ["Tata Motors"],
    "TATASTEEL":   ["Tata Steel"],
    "JSWSTEEL":    ["JSW Steel"],
    "HINDALCO":    ["Hindalco"],
    "SUNPHARMA":   ["Sun Pharma", "Sun Pharmaceutical"],
    "CIPLA":       ["Cipla"],
    "DRREDDY":     ["Dr Reddy", "Dr. Reddy", "Dr Reddys", "Dr. Reddy's"],
    "DIVISLAB":    ["Divi's Lab", "Divis Lab", "Divi Lab"],
    "WIPRO":       ["Wipro"],
    "HCLTECH":     ["HCL Tech", "HCL Technologies"],
    "TECHM":       ["Tech Mahindra"],
    "LTIM":        ["LTIMindtree", "LTI Mindtree"],
    "PERSISTENT":  ["Persistent Systems", "Persistent "],
    "COFORGE":     ["Coforge"],
    "NTPC":        ["NTPC"],
    "POWERGRID":   ["Power Grid Corp", "Power Grid"],
    "ONGC":        ["ONGC", "Oil and Natural Gas"],
    "COALINDIA":   ["Coal India"],
    "BPCL":        ["BPCL", "Bharat Petroleum"],
    "ADANIPORTS":  ["Adani Ports"],
    "ULTRACEMCO":  ["UltraTech Cement"],
    "GRASIM":      ["Grasim"],
    "NESTLEIND":   ["Nestle India", "Nestle "],
    "ASIANPAINT":  ["Asian Paints"],
    "BAJAJ-AUTO":  ["Bajaj Auto"],
    "HEROMOTOCO":  ["Hero MotoCorp"],
    "EICHERMOT":   ["Eicher Motors", "Royal Enfield"],
    "TITAN":       ["Titan Company", "Titan "],
    "TRENT":       ["Trent Ltd", "Trent "],
    "ZOMATO":      ["Zomato"],
    "ETERNAL":     ["Eternal Ltd"],
    "SWIGGY":      ["Swiggy"],
    "APOLLOHOSP":  ["Apollo Hospitals"],
    "TATACONSUM":  ["Tata Consumer"],
    "INDUSINDBK":  ["IndusInd Bank"],
    "DIXON":       ["Dixon Technologies", "Dixon "],
    "POLYCAB":     ["Polycab"],
    "HAL":         ["Hindustan Aeronautics", "HAL "],
    "BEL":         ["Bharat Electronics", "BEL "],
    "IRCTC":       ["IRCTC"],
    "NAUKRI":      ["Naukri", "Info Edge"],
    "PFC":         ["Power Finance Corp", "PFC "],
    "RECLTD":      ["REC Ltd"],
    "IREDA":       ["IREDA"],
    "SUZLON":      ["Suzlon"],
    "TATAPOWER":   ["Tata Power"],
    "BANKBARODA":  ["Bank of Baroda"],
    "FEDERALBNK":  ["Federal Bank"],
    "IDFCFIRSTB":  ["IDFC First Bank"],
    "DABUR":       ["Dabur"],
    "MARICO":      ["Marico"],
    "GODREJCP":    ["Godrej Consumer"],
    "PIDILITIND":  ["Pidilite"],
    "HAVELLS":     ["Havells"],
    "BRITANNIA":   ["Britannia"],
    "DMART":       ["DMart", "Avenue Supermarts"],
    "SIEMENS":     ["Siemens India", "Siemens Ltd"],
    "VEDL":        ["Vedanta"],
    "BSE":         ["BSE Ltd"],
    "OLAELEC":     ["Ola Electric"],
    "HYUNDAI":     ["Hyundai Motor India"],
    "TATAELXSI":   ["Tata Elxsi"],
    "KPITTECH":    ["KPIT"],
    "TATACAP":     ["Tata Capital"],
    "MON150BEES":  ["Nifty 150 BeES"],
}

# Sector keywords — for tagging macro/sector stories when no direct ticker match
SECTOR_KEYWORDS: dict[str, list[str]] = {
    "Banking":        ["bank", "banking", "nifty bank", "rbi", "private bank", "psu bank"],
    "IT":             ["it sector", "it stocks", "software exports", "nasscom", "it services"],
    "Pharma":         ["pharma", "pharmaceutical", "drug", "fda", "usfda", "cdsco"],
    "Automobiles":    ["auto sales", "passenger vehicle", "two wheeler", "ev sales", "auto industry"],
    "FMCG":           ["fmcg", "consumer goods", "rural demand"],
    "Metals":         ["steel", "iron ore", "copper", "aluminium", "metal prices", "lme"],
    "Oil & Gas":      ["crude oil", "brent", "opec", "oil prices", "petrol", "diesel", "lpg"],
    "Power":          ["power sector", "electricity", "renewable", "solar", "wind energy"],
    "Defence":        ["defence", "defense", "drdo", "moddef", "indigenisation"],
    "Infrastructure": ["infrastructure", "nhai", "capex", "roads", "highways"],
    "Cement":         ["cement prices", "cement sector", "housing"],
    "Telecom":        ["telecom", "5g", "spectrum", "arpu", "trai"],
    "Realty":         ["real estate", "housing sales", "rera"],
    "NBFC":           ["nbfc", "non-banking financial"],
}


# ──────────────────────────────────────────────────────────────────────────────
# ANALYSIS FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def _score_lexicon(text: str, lexicon: dict[str, int]) -> tuple[int, list[str]]:
    """Count weighted occurrences. Returns (score, matched_terms)."""
    t = text.lower()
    score = 0
    matched: list[str] = []
    for term, w in lexicon.items():
        # word-boundary for single words; substring for multi-word
        if " " in term:
            if term in t:
                # check negation in 3 words before
                idx = t.find(term)
                window = t[max(0, idx - 25):idx]
                if not any(n in window.split()[-3:] for n in NEGATION):
                    score += w
                    matched.append(term)
        else:
            for m in re.finditer(r"\b" + re.escape(term) + r"\b", t):
                idx = m.start()
                window = t[max(0, idx - 25):idx]
                if not any(n in window.split()[-3:] for n in NEGATION):
                    score += w
                    matched.append(term)
                    break
    return score, matched


def score_item(item: dict) -> dict:
    """Adds sentiment, matched_tickers, matched_sectors to a news item. Never raises."""
    try:
        title = item.get("title", "") or ""
        summary = item.get("summary", "") or ""
        headline_zone = title + " " + summary[:250]  # "first paragraph"

        # Sentiment
        bull, bull_terms = _score_lexicon(headline_zone, BULLISH_TERMS)
        bear, bear_terms = _score_lexicon(headline_zone, BEARISH_TERMS)
        macro_bull, _ = _score_lexicon(headline_zone, MACRO_BULLISH)
        macro_bear, _ = _score_lexicon(headline_zone, MACRO_BEARISH)

        raw = bull + macro_bull - bear - macro_bear
        weight = item.get("weight", 1.0)
        weighted = raw * weight

        if weighted >= 3:   label = "BULLISH"
        elif weighted >= 1: label = "MILDLY_BULLISH"
        elif weighted <= -3: label = "BEARISH"
        elif weighted <= -1: label = "MILDLY_BEARISH"
        else:               label = "NEUTRAL"

        # Ticker matching — strict verbatim
        matched_tickers: list[str] = []
        for sym, aliases in STOCK_ALIASES.items():
            for alias in aliases:
                # case-insensitive substring match on headline zone
                if alias.lower() in headline_zone.lower():
                    matched_tickers.append(sym)
                    break

        # Sector matching — only used when no direct ticker match
        matched_sectors: list[str] = []
        if not matched_tickers:
            low = headline_zone.lower()
            for sec, kws in SECTOR_KEYWORDS.items():
                if any(k in low for k in kws):
                    matched_sectors.append(sec)

        item["sentiment"]        = label
        item["sentiment_score"]  = round(weighted, 2)
        item["sentiment_raw"]    = raw
        item["bull_terms"]       = bull_terms[:4]
        item["bear_terms"]       = bear_terms[:4]
        item["matched_tickers"]  = matched_tickers[:8]
        item["matched_sectors"]  = matched_sectors[:5]
        return item
    except Exception as e:
        log.warning("score_item failed: %s", e)
        item["sentiment"] = "NEUTRAL"
        item["sentiment_score"] = 0
        item["matched_tickers"] = []
        item["matched_sectors"] = []
        return item


def dedupe(items: list[dict], threshold: float = 0.85) -> list[dict]:
    """Remove near-duplicate headlines. Keeps highest-weight copy."""
    if not items:
        return []
    items = sorted(items, key=lambda x: -x.get("weight", 1.0))
    kept: list[dict] = []
    for it in items:
        title_low = it.get("title", "").lower()
        dup = False
        for k in kept:
            if SequenceMatcher(None, title_low, k.get("title", "").lower()).ratio() >= threshold:
                dup = True
                break
        if not dup:
            kept.append(it)
    return kept


def analyze(items: list[dict]) -> list[dict]:
    """Full pipeline: score, then dedupe."""
    if not items:
        return []
    scored = [score_item(dict(it)) for it in items]
    return dedupe(scored)


def news_for_ticker(items: list[dict], symbol: str, limit: int = 5) -> list[dict]:
    """Strict matches only — items where symbol appears in matched_tickers."""
    hits = [it for it in items if symbol in (it.get("matched_tickers") or [])]
    hits.sort(key=lambda x: (-abs(x.get("sentiment_score", 0)), -x.get("published_ts", 0)))
    return hits[:limit]


def news_for_sector(items: list[dict], sector: str, limit: int = 5) -> list[dict]:
    hits = [it for it in items if sector in (it.get("matched_sectors") or [])]
    hits.sort(key=lambda x: (-abs(x.get("sentiment_score", 0)), -x.get("published_ts", 0)))
    return hits[:limit]


def aggregate_ticker_sentiment(items: list[dict], symbol: str) -> dict:
    """Returns {score, label, count, modifier} where modifier is a score delta for rules engine."""
    hits = news_for_ticker(items, symbol, limit=20)
    if not hits:
        return {"score": 0, "label": "NEUTRAL", "count": 0, "modifier": 0, "top_headline": None}

    total = sum(h.get("sentiment_score", 0) for h in hits)
    avg = total / len(hits)
    count = len(hits)

    # Modifier for technical score: bounded ±8 points
    modifier = max(-8, min(8, int(round(avg * 1.5))))

    if avg >= 2:   label = "BULLISH"
    elif avg >= 0.5: label = "MILDLY_BULLISH"
    elif avg <= -2: label = "BEARISH"
    elif avg <= -0.5: label = "MILDLY_BEARISH"
    else: label = "NEUTRAL"

    top = max(hits, key=lambda x: abs(x.get("sentiment_score", 0)))
    return {
        "score":        round(avg, 2),
        "label":        label,
        "count":        count,
        "modifier":     modifier,
        "top_headline": top.get("title"),
        "top_source":   top.get("source"),
        "top_link":     top.get("link"),
    }


def aggregate_macro_sentiment(items: list[dict]) -> dict:
    """Overall macro/geopolitical pulse from items without direct ticker matches."""
    macro = [it for it in items
             if it.get("category") in ("macro", "policy", "global")
             or it.get("matched_sectors")]
    if not macro:
        return {"score": 0, "label": "NEUTRAL", "count": 0, "highlights": []}

    total = sum(it.get("sentiment_score", 0) for it in macro)
    avg = total / len(macro)

    if avg >= 1.5: label = "RISK_ON"
    elif avg >= 0.3: label = "MILDLY_POSITIVE"
    elif avg <= -1.5: label = "RISK_OFF"
    elif avg <= -0.3: label = "MILDLY_NEGATIVE"
    else: label = "NEUTRAL"

    highlights = sorted(macro, key=lambda x: (-abs(x.get("sentiment_score", 0)),
                                              -x.get("weight", 1),
                                              -x.get("published_ts", 0)))[:6]
    return {
        "score":      round(avg, 2),
        "label":      label,
        "count":      len(macro),
        "highlights": [{
            "title":    h.get("title"),
            "source":   h.get("source"),
            "category": h.get("category"),
            "sentiment": h.get("sentiment"),
            "sectors":  h.get("matched_sectors", []),
            "link":     h.get("link"),
        } for h in highlights],
    }
