"""
advisor_narrator.py — per-stock advisor narrative generator
============================================================
Pure rule-based 4-8 sentence narratives combining technicals, fundamentals,
news, regime, sector context. Reads like a senior advisor talking to a client.
"""
from __future__ import annotations

import logging
import random

log = logging.getLogger("advisor_narrator")


def _tech_paragraph(symbol: str, tech: dict, price: float, change_pct: float) -> str:
    score = tech.get("score") or 0
    rsi = tech.get("rsi") or 50
    trend = tech.get("trend_label", "")
    vol_ratio = tech.get("volume_ratio", 1) or 1
    sma = tech.get("sma_alignment", "")

    parts = []
    if score >= 70:
        parts.append(f"{symbol} is firing on all cylinders — our engine scores it {score}/100")
    elif score >= 55:
        parts.append(f"{symbol} is holding a constructive setup at {score}/100")
    elif score >= 40:
        parts.append(f"{symbol} sits in the middle of the pack with a {score}/100 read")
    else:
        parts.append(f"{symbol} is flashing weakness, scoring just {score}/100")

    if trend:
        parts.append(f"with {trend.lower()}")
    if sma and sma != trend:
        parts.append(f"({sma})")

    sentence1 = " ".join(parts) + "."

    # Sentence 2 — momentum reading
    if rsi >= 70:
        mom = f"Momentum is hot — RSI is at {rsi:.0f}, meaning buyers are in control but the name is stretched"
    elif rsi >= 55:
        mom = f"Momentum is healthy with RSI at {rsi:.0f}, leaving room before overbought territory"
    elif rsi >= 45:
        mom = f"Momentum is neutral (RSI {rsi:.0f}), waiting for a catalyst either way"
    elif rsi >= 30:
        mom = f"Momentum is fading (RSI {rsi:.0f}) and the stock needs to find footing"
    else:
        mom = f"Momentum is washed out at RSI {rsi:.0f} — bounces from here tend to be sharp but unreliable"

    if vol_ratio >= 2:
        mom += f", and today's volume came in at {vol_ratio:.1f}× the 20-day average, which is the kind of participation trend-followers look for"
    elif vol_ratio >= 1.3:
        mom += f" on above-average volume ({vol_ratio:.1f}×)"
    elif vol_ratio <= 0.7:
        mom += f" on light volume ({vol_ratio:.1f}×) — any move here should be taken with a pinch of salt"
    mom += "."

    return sentence1 + " " + mom


def _news_paragraph(news_info: dict) -> str:
    if not news_info or not news_info.get("count"):
        return ""
    count = news_info.get("count", 0)
    label = news_info.get("label", "NEUTRAL")
    headline = news_info.get("top_headline") or ""
    source = news_info.get("top_source") or ""

    if label in ("BULLISH", "MILDLY_BULLISH"):
        lead = f"News flow is a tailwind — {count} item{'s' if count > 1 else ''} in the last 48 hours lean positive"
    elif label in ("BEARISH", "MILDLY_BEARISH"):
        lead = f"News flow is a headwind — {count} item{'s' if count > 1 else ''} in the last 48 hours skew negative"
    else:
        lead = f"News flow is mixed across {count} recent item{'s' if count > 1 else ''}"

    if headline:
        lead += f", with the most prominent being '{headline[:110]}' ({source})"
    lead += "."
    return lead


def _regime_paragraph(regime: dict, sector: str, sector_momentum: list) -> str:
    label = (regime or {}).get("label", "UNKNOWN")
    breadth = (regime or {}).get("breadth_pct", 0)

    if label == "BULL":
        regime_text = f"The broader tape is supportive — {breadth:.0f}% breadth in a BULL regime means a rising tide"
    elif label == "BEAR":
        regime_text = f"The broader tape is working against you — {breadth:.0f}% breadth in a BEAR regime means even good names struggle"
    elif label == "SIDEWAYS":
        regime_text = f"The broader tape is range-bound with {breadth:.0f}% breadth, so stock selection matters more than direction"
    else:
        regime_text = "The broader tape is hard to classify today, so we lean on stock-specific signals"

    # Sector context
    if sector and sector_momentum:
        sec_entry = next((s for s in sector_momentum if s.get("sector") == sector), None)
        if sec_entry:
            sec_score = sec_entry.get("score", 50)
            sec_trend = sec_entry.get("trend", "flat")
            if sec_score >= 60:
                regime_text += f", and {sector} is one of the stronger sectors today (score {sec_score}, trend {sec_trend})"
            elif sec_score <= 45:
                regime_text += f", and {sector} is lagging (score {sec_score}, trend {sec_trend})"
            else:
                regime_text += f", with {sector} sector performing in-line"
    regime_text += "."
    return regime_text


def _action_paragraph(action: str, verdict: str, tech: dict, forecasts: dict,
                      price: float, entry_ceiling: float) -> str:
    t = forecasts.get("1d", {})
    w = forecasts.get("1w", {})
    m = forecasts.get("1m", {})

    if action == "ADD":
        core = f"The verdict is {verdict} — deploy on pullbacks toward ₹{entry_ceiling:,.0f} if you don't already own it"
    elif action == "WAIT_FOR_DIP":
        core = f"The verdict is {verdict} — the setup is good but the entry is rich; wait for price to come to you near ₹{entry_ceiling:,.0f}"
    elif action == "HOLD":
        core = f"The verdict is HOLD — if you own it, let it work; if you don't, there are cleaner setups elsewhere"
    elif action == "BLACKOUT":
        core = "The verdict is EARNINGS BLACKOUT — no fresh adds until results are printed and digested"
    elif action == "TRIM":
        core = "The verdict is BOOK PARTIAL — take 30-50% off the table and trail the stop on the rest"
    elif action == "EXIT":
        core = "The verdict is EXIT — the rule ladder has flagged it, clear the position at open and redeploy"
    elif action == "DO_NOT_TRADE":
        core = "The verdict is DO NOT TRADE — data or liquidity is unreliable"
    else:
        core = f"The verdict is {verdict} — review and decide"

    # Append horizon summary
    bias_1d = t.get("bias", "Neutral")
    bias_1w = w.get("bias", "Neutral")
    bias_1m = m.get("bias", "Neutral")
    horizons_line = (f" Our three-horizon read: {bias_1d} tomorrow, "
                     f"{bias_1w} over the next week, {bias_1m} through the month.")

    return core + "." + horizons_line


def build_narrative(symbol: str, name: str, sector: str,
                    tech: dict, price: float, change_pct: float,
                    action: str, verdict: str,
                    news_info: dict, regime: dict, sector_momentum: list,
                    forecasts: dict, entry_ceiling: float = 0) -> str:
    """
    Returns a 4-6 sentence narrative. Never raises.
    """
    try:
        paragraphs = []

        # Opening — technical reading
        p1 = _tech_paragraph(symbol, tech, price, change_pct)
        paragraphs.append(p1)

        # News layer
        p2 = _news_paragraph(news_info)
        if p2:
            paragraphs.append(p2)

        # Regime + sector layer
        p3 = _regime_paragraph(regime, sector, sector_momentum)
        paragraphs.append(p3)

        # Action + three-horizon
        p4 = _action_paragraph(action, verdict, tech, forecasts, price, entry_ceiling or price)
        paragraphs.append(p4)

        return " ".join(paragraphs)
    except Exception as e:
        log.warning("narrative failed for %s: %s", symbol, e)
        return f"{symbol}: {verdict}. Rule engine scored {tech.get('score', 0)}/100. Review the technical and news panels for details."
