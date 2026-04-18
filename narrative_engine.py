"""
narrative_engine.py — ProfitPilot Narrative Engine v1.0
========================================================
Plug-and-play replacement for advisor_narrator.build_narrative().

Transforms raw technical signals into human-like stock analysis by:
  1. Computing Derived Intelligence Layers (momentum_state, trend_quality, etc.)
  2. Detecting signal conflicts (trend vs momentum disagreement)
  3. Generating context-aware narratives per horizon (short/medium/long tone)
  4. Using phrase pools with controlled randomization (anti-repetition)
  5. Adding forward-looking statements + bull/bear scenarios
  6. Turning scores into explained reasoning
  7. Integrating regime + sector awareness

Drop-in: replaces build_narrative() in quotes_daily.py.
Zero LLM. Zero paid services. 100% deterministic per seed.
"""
from __future__ import annotations

import hashlib
import logging
import random
from typing import Optional

log = logging.getLogger("narrative_engine")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DERIVED INTELLIGENCE LAYERS
# ═══════════════════════════════════════════════════════════════════════════════

def _derive_momentum_state(rsi: float, volume_ratio: float, change_pct: float) -> str:
    """Interpret raw RSI + volume into a human-readable momentum state."""
    if rsi >= 78:
        return "overheated"
    if rsi >= 68 and volume_ratio >= 1.3:
        return "strong_and_confirmed"
    if rsi >= 68:
        return "strong_but_extended"
    if 55 <= rsi < 68 and change_pct > 0:
        return "building"
    if 45 <= rsi < 55:
        return "neutral"
    if 35 <= rsi < 45:
        return "fading"
    if rsi < 35:
        return "washed_out"
    return "neutral"


def _derive_trend_quality(score: int, sma_alignment: str, adx: Optional[float]) -> str:
    """Classify the trend's structural quality."""
    is_full_bull = "Full Bull" in (sma_alignment or "")
    is_bull = "Bull" in (sma_alignment or "") and not is_full_bull
    is_above_200 = "Above 200" in (sma_alignment or "")

    if is_full_bull and score >= 60:
        return "clean_strong_trend"
    if is_full_bull:
        return "strong_trend"
    if is_bull and score >= 50:
        return "constructive_trend"
    if is_above_200:
        return "early_trend"
    if "Bearish" in (sma_alignment or ""):
        return "broken_trend"
    return "no_trend"


def _derive_entry_quality(rsi: float, bb_position: float, change_pct: float,
                           volume_ratio: float) -> str:
    """Assess whether the current price offers a good entry."""
    if rsi >= 75 and change_pct > 2:
        return "chasing_risk"
    if rsi >= 70 and bb_position >= 0.95:
        return "extended_entry"
    if 50 <= rsi <= 62 and volume_ratio >= 1.2:
        return "ideal_pullback"
    if rsi <= 45 and change_pct < -1:
        return "falling_knife"
    if 55 <= rsi <= 68 and 0.5 <= bb_position <= 0.8:
        return "clean_entry"
    return "neutral_entry"


def _derive_risk_state(atr_pct: float, rsi: float, change_pct: float,
                        regime_label: str) -> str:
    """Assess the risk environment for this stock."""
    if atr_pct >= 4.5 and rsi >= 75:
        return "high_volatility_overbought"
    if atr_pct >= 4.0:
        return "volatile"
    if regime_label == "BEAR" and rsi < 45:
        return "regime_pressure"
    if rsi >= 78:
        return "overbought_risk"
    if change_pct < -3:
        return "sharp_decline"
    if atr_pct <= 1.8 and 45 <= rsi <= 65:
        return "low_risk_consolidation"
    return "moderate"


def derive_intelligence(tech: dict, price: float, change_pct: float,
                         regime: dict) -> dict:
    """Compute all derived intelligence layers from raw signals."""
    rsi = tech.get("rsi") or 50
    vol_ratio = tech.get("volume_ratio") or 1.0
    score = tech.get("score") or 50
    sma = tech.get("trend_label") or tech.get("sma_alignment") or ""
    adx = tech.get("adx")
    atr_pct = (tech.get("atr_pct") or 0) or (
        ((tech.get("atr") or 0) / price * 100) if price else 0
    )
    bb_pos = tech.get("bb_position") or 0.5
    regime_label = (regime or {}).get("label", "UNKNOWN")

    return {
        "momentum_state":  _derive_momentum_state(rsi, vol_ratio, change_pct),
        "trend_quality":   _derive_trend_quality(score, sma, adx),
        "entry_quality":   _derive_entry_quality(rsi, bb_pos, change_pct, vol_ratio),
        "risk_state":      _derive_risk_state(atr_pct, rsi, change_pct, regime_label),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. PHRASE POOLS (anti-repetition system)
# ═══════════════════════════════════════════════════════════════════════════════

# Each pool is keyed by derived state → list of phrase variants.
# The engine picks one per stock using a deterministic seed (symbol + date).

TREND_PHRASES = {
    "clean_strong_trend": [
        "The trend remains firmly upward with price holding above all key moving averages, indicating sustained institutional interest.",
        "Price structure is textbook-strong — full bullish alignment across 20/50/200 SMAs with genuine follow-through.",
        "Buyers continue to dominate the structure, with clean alignment across every major moving average.",
        "This is a well-established uptrend with all moving averages stacked bullishly — the path of least resistance is higher.",
    ],
    "strong_trend": [
        "The trend is intact with price above key moving averages, though not yet in perfect alignment.",
        "Structural trend remains positive — price is holding above the 50 and 200 SMAs comfortably.",
        "The broader trend leans constructive with most moving averages confirming upside bias.",
    ],
    "constructive_trend": [
        "The setup is constructive — price is above the 50-day and 200-day averages, building a base for the next move.",
        "Trend structure is positive but not aggressive — the kind of setup that rewards patient entries.",
        "A trend-following setup is taking shape, with price respecting key support levels.",
    ],
    "early_trend": [
        "Price is above its 200-day average, which is the minimum bar for trend-followers, but alignment is incomplete.",
        "Early signs of trend recovery — price has reclaimed the 200 SMA, but the 50 SMA hasn't confirmed yet.",
        "The stock is above its long-term average, suggesting the worst may be over, but conviction is limited.",
    ],
    "broken_trend": [
        "The trend structure is compromised — price is below key moving averages and the path of least resistance is lower.",
        "Moving averages are aligned bearishly, which is a structural headwind even if short-term bounces occur.",
        "The trend is working against you here — any rally into the falling averages is likely to meet supply.",
    ],
    "no_trend": [
        "No clear trend is in place — the stock is range-bound between support and resistance.",
        "Price is drifting without direction, making it a stock-picker's nightmare until a catalyst emerges.",
        "The trend is unclear — neither bulls nor bears have control, and clarity is needed before committing.",
    ],
}

MOMENTUM_PHRASES = {
    "overheated": [
        "Momentum is overheated — RSI has pushed past 78, meaning the easy money has already been made on this leg.",
        "Buyers are firmly in control but the stock is stretched — RSI above 78 typically precedes a pause or pullback.",
        "Momentum is maxed out — at these RSI levels, the risk of a mean-reversion snap is elevated.",
    ],
    "strong_and_confirmed": [
        "Momentum is strong and confirmed by volume — this is the kind of participation that sustains moves.",
        "Buying pressure is building with volume backing the move — momentum has substance behind it.",
        "The combination of strong RSI and above-average volume is what trend-followers look for.",
    ],
    "strong_but_extended": [
        "Momentum is strong but slightly extended, suggesting limited immediate upside without a pause.",
        "Buying pressure remains healthy but the RSI is pushing upper bounds — a brief consolidation would be constructive.",
        "The momentum read is positive but stretched — the setup favors dip-buying over chasing.",
    ],
    "building": [
        "Momentum is building — RSI is in the constructive 55–68 zone with room to run before overbought territory.",
        "Buying interest is picking up gradually — this is the early-to-mid phase of a momentum move.",
        "The momentum profile is healthy and building, with RSI leaving plenty of room before things get stretched.",
    ],
    "neutral": [
        "Momentum is flat — RSI is parked in no-man's land, waiting for a catalyst in either direction.",
        "Neither buyers nor sellers have momentum on their side — the stock needs a trigger to break the stalemate.",
        "Momentum is neutral, which means the stock is coiling — the next directional move could be decisive.",
    ],
    "fading": [
        "Momentum is fading — RSI has slipped into the 35–45 range, indicating weakening buying interest.",
        "Buying pressure is drying up — the stock needs to find footing soon or risk accelerating lower.",
        "The momentum read is deteriorating, with RSI suggesting sellers are gradually taking control.",
    ],
    "washed_out": [
        "Momentum is washed out at extremely low RSI — bounces from here tend to be sharp but unreliable.",
        "The stock is deeply oversold — while a snapback is possible, sustained recovery needs volume and time.",
        "RSI has cratered into washed-out territory — contrarian interest may emerge but the trend is hostile.",
    ],
}

ENTRY_PHRASES = {
    "chasing_risk": [
        "At current levels, you'd be chasing — the entry is rich after a strong run, and the risk-reward is unfavorable.",
        "Entry risk is elevated — buying here means paying up after the move, not ahead of it.",
    ],
    "extended_entry": [
        "The entry is stretched — price is near the upper Bollinger Band with RSI hot, favoring patience over urgency.",
        "Waiting for a 2–3% pullback would meaningfully improve the risk-reward on this entry.",
    ],
    "ideal_pullback": [
        "This is the kind of entry window trend-followers wait for — a pullback into support with volume confirmation.",
        "The entry quality is strong — a dip into an ideal RSI zone with volume backing the thesis.",
    ],
    "clean_entry": [
        "Entry conditions are clean — momentum is constructive, volatility is contained, and price is well-positioned.",
        "The setup offers a reasonable entry point with acceptable risk-reward parameters.",
    ],
    "falling_knife": [
        "Catching this dip is tempting but risky — the slide has momentum behind it and no clear floor yet.",
        "The stock is in freefall mode — until price action stabilizes, entries are speculative at best.",
    ],
    "neutral_entry": [
        "Entry quality is neither compelling nor dangerous — standard position sizing applies.",
        "No strong edge on timing here — enter on your terms with appropriate risk management.",
    ],
}

RISK_PHRASES = {
    "high_volatility_overbought": [
        "Risk is elevated on both fronts — high volatility combined with overbought momentum means this can snap either way.",
    ],
    "volatile": [
        "Volatility is running hot — position size accordingly, as the stock can move 4%+ in a single session.",
    ],
    "regime_pressure": [
        "The broader market regime is working against this name — even good setups struggle in BEAR regimes.",
    ],
    "overbought_risk": [
        "Overbought conditions create a heightened risk of profit-taking — don't let RSI warnings go unheeded.",
    ],
    "sharp_decline": [
        "A sharp decline is underway — sitting on hands is often smarter than trying to time the bottom.",
    ],
    "low_risk_consolidation": [
        "Risk is contained — low volatility and balanced momentum create a quiet, manageable setup.",
    ],
    "moderate": [
        "Risk is moderate and manageable with standard position sizing and stop discipline.",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CONFLICT DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_conflicts(intel: dict, tech: dict, news_info: dict) -> list[str]:
    """Detect disagreements between signals — the interesting part of analysis."""
    conflicts = []
    trend_q = intel["trend_quality"]
    mom_s = intel["momentum_state"]

    # Trend strong but momentum fading
    if trend_q in ("clean_strong_trend", "strong_trend") and mom_s in ("fading", "neutral"):
        conflicts.append(
            "Trend remains intact, but momentum is fading — this often signals consolidation "
            "ahead rather than a breakdown, but fresh entries should wait for a reset."
        )

    # Momentum strong but trend weak
    if mom_s in ("strong_and_confirmed", "strong_but_extended") and trend_q in ("broken_trend", "no_trend"):
        conflicts.append(
            "Momentum is surging but the underlying trend structure hasn't confirmed — "
            "this could be a counter-trend bounce rather than the start of a new move."
        )

    # Price up but volume absent
    vol_ratio = tech.get("volume_ratio") or 1.0
    change_pct = tech.get("change_pct") or 0
    if change_pct > 1.5 and vol_ratio < 0.8:
        conflicts.append(
            "Price is moving higher but volume isn't participating — "
            "low-conviction rallies tend to fade, so this advance should be viewed with skepticism."
        )

    # News bearish but technicals bullish
    news_label = (news_info or {}).get("label", "NEUTRAL")
    if news_label in ("BEARISH", "MILDLY_BEARISH") and trend_q in ("clean_strong_trend", "strong_trend"):
        conflicts.append(
            "News flow is negative but the technical structure hasn't cracked — "
            "the tape often leads the headlines, so watch price action over sentiment."
        )

    # News bullish but technicals weak
    if news_label in ("BULLISH", "MILDLY_BULLISH") and trend_q in ("broken_trend", "no_trend"):
        conflicts.append(
            "Positive news hasn't translated into technical improvement — "
            "until moving averages confirm, the narrative is running ahead of the chart."
        )

    # Overbought on declining volume
    rsi = tech.get("rsi") or 50
    if rsi >= 72 and vol_ratio < 0.9:
        conflicts.append(
            "The stock is overbought on thinning volume — a classic setup for a pullback "
            "or at minimum a period of sideways digestion."
        )

    return conflicts[:2]  # cap at 2 to avoid bloat


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SCENARIO ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def _build_scenarios(symbol: str, price: float, atr: float, tech: dict,
                     intel: dict, forecasts: dict) -> dict:
    """Generate bull/bear case with price levels."""
    atr = atr or (price * 0.02)

    # Use forecast price bands if available
    fc_1w = forecasts.get("1w", {}).get("price_band", {})
    fc_1m = forecasts.get("1m", {}).get("price_band", {})

    bull_target = fc_1w.get("high") or round(price + atr * 2.5, 2)
    bear_level = fc_1w.get("low") or round(price - atr * 2.0, 2)

    # Build contextual scenario text
    if intel["trend_quality"] in ("clean_strong_trend", "strong_trend"):
        bull_text = (f"Sustained above ₹{price:,.0f} could push towards "
                     f"₹{bull_target:,.0f} as trend followers add exposure.")
        bear_text = (f"A drop below ₹{bear_level:,.0f} would dent the "
                     f"short-term thesis and trigger trailing stop reviews.")
    elif intel["momentum_state"] in ("building", "strong_and_confirmed"):
        bull_text = (f"If momentum holds, a test of ₹{bull_target:,.0f} "
                     f"is in play over the coming sessions.")
        bear_text = (f"Failure to hold ₹{bear_level:,.0f} would suggest "
                     f"the momentum burst is fading and a retest of lower support is likely.")
    else:
        bull_text = (f"A catalyst-driven move above ₹{round(price * 1.03):,.0f} "
                     f"could open room towards ₹{bull_target:,.0f}.")
        bear_text = (f"Weakness below ₹{bear_level:,.0f} would confirm "
                     f"the current range is resolving lower.")

    return {
        "bull_case": bull_text,
        "bear_case": bear_text,
        "bull_target": bull_target,
        "bear_level": bear_level,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 5. SCORE EXPLANATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def _explain_score(score: int, components: dict, intel: dict) -> str:
    """Turn a raw score into a one-line explanation."""
    parts = []

    # Find strongest and weakest components
    if components:
        max_comp = max(components.items(), key=lambda x: x[1])
        min_comp = min(components.items(), key=lambda x: x[1])
        parts.append(f"{max_comp[0].replace('_', ' ')} is the primary driver")
        if min_comp[1] <= 2:
            parts.append(f"{min_comp[0].replace('_', ' ')} is a drag")

    # Tie to derived state
    if intel["momentum_state"] == "overheated":
        parts.append("but overbought conditions cap the upside")
    elif intel["entry_quality"] == "ideal_pullback":
        parts.append("with an ideal entry window open")
    elif intel["risk_state"] == "volatile":
        parts.append("in a volatile environment")

    explanation = ", ".join(parts) if parts else "balanced across components"
    return f"{score}/100 — {explanation}."


# ═══════════════════════════════════════════════════════════════════════════════
# 6. FORWARD-LOOKING STATEMENT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _forward_looking(symbol: str, price: float, atr: float, intel: dict,
                     forecasts: dict) -> str:
    """Generate a forward-looking conditional statement."""
    atr = atr or (price * 0.02)
    upper = round(price + atr * 1.2, 0)
    lower = round(price - atr * 1.5, 0)

    fc_1d = forecasts.get("1d", {})
    bias = fc_1d.get("bias", "Neutral")

    templates = {
        "Bullish": [
            f"Continuation likely if volume expands above ₹{upper:,.0f}.",
            f"A break above ₹{upper:,.0f} could trigger further upside momentum.",
        ],
        "Mildly Bullish": [
            f"Sustained buying above ₹{round(price):,.0f} keeps the setup alive; "
            f"a push through ₹{upper:,.0f} would be the confirmation.",
            f"Look for a volume expansion day above ₹{upper:,.0f} as the next trigger.",
        ],
        "Neutral": [
            f"The stock needs to resolve this range — ₹{upper:,.0f} on the upside, "
            f"₹{lower:,.0f} on the downside — before a directional bet makes sense.",
        ],
        "Mildly Bearish": [
            f"Failure to hold ₹{lower:,.0f} may trigger short-term weakness.",
            f"Watch ₹{lower:,.0f} as the line in the sand — a break below signals further downside.",
        ],
        "Bearish": [
            f"Downside risk toward ₹{lower:,.0f} is elevated; protect capital first.",
        ],
    }

    pool = templates.get(bias, templates["Neutral"])
    return _pick_phrase(pool, symbol)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. TAB-AWARE TONE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

TONE_CONFIG = {
    "short": {
        "voice": "tactical",
        "tempo_words": ["breakout", "momentum", "trigger", "snap", "immediate"],
        "action_frame": "deploy on the next pullback — speed matters",
        "hold_tone": "fast-paced setup requiring daily monitoring",
    },
    "medium": {
        "voice": "strategic",
        "tempo_words": ["swing", "trend", "building", "gradual", "developing"],
        "action_frame": "build a position on dips — let the trend work",
        "hold_tone": "trend-following setup with room for gradual upside",
    },
    "long": {
        "voice": "conviction",
        "tempo_words": ["structural", "compounding", "accumulation", "durable", "franchise"],
        "action_frame": "accumulate on weakness — this is a compounder",
        "hold_tone": "structurally strong stock suitable for accumulation on dips",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 8. NEWS LAYER INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

def _news_sentence(news_info: dict) -> str:
    """Generate a news context sentence, or empty string if no news."""
    if not news_info or not news_info.get("count"):
        return ""
    count = news_info.get("count", 0)
    label = news_info.get("label", "NEUTRAL")
    headline = news_info.get("top_headline") or ""

    if label in ("BULLISH", "MILDLY_BULLISH"):
        lead = f"News flow is a tailwind — {count} item{'s' if count > 1 else ''} in the last 48h lean positive"
    elif label in ("BEARISH", "MILDLY_BEARISH"):
        lead = f"News flow is a headwind — {count} item{'s' if count > 1 else ''} in the last 48h skew negative"
    else:
        lead = f"News flow is mixed across {count} recent item{'s' if count > 1 else ''}"

    if headline:
        # Truncate cleanly
        h = headline[:100].rsplit(" ", 1)[0] if len(headline) > 100 else headline
        lead += f", led by '{h}'"
    return lead + "."


# ═══════════════════════════════════════════════════════════════════════════════
# 9. REGIME + SECTOR AWARENESS
# ═══════════════════════════════════════════════════════════════════════════════

def _regime_sentence(regime: dict, sector: str, sector_momentum: list) -> str:
    """One sentence about market context."""
    label = (regime or {}).get("label", "UNKNOWN")
    breadth = (regime or {}).get("breadth_pct", 0)

    regime_map = {
        "BULL": f"The broader tape is supportive — {breadth:.0f}% breadth in a BULL regime means a rising tide",
        "BEAR": f"The broader tape is hostile — {breadth:.0f}% breadth in a BEAR regime means even good setups struggle",
        "SIDEWAYS": f"The tape is range-bound with {breadth:.0f}% breadth — stock selection matters more than direction",
    }
    base = regime_map.get(label, "Regime signals are mixed — lean on stock-specific evidence")

    # Sector context
    if sector and sector_momentum:
        sec_entry = next((s for s in sector_momentum if s.get("sector") == sector), None)
        if sec_entry:
            sec_score = sec_entry.get("score", 50)
            if sec_score >= 58:
                base += f", and {sector} is leading today (score {sec_score})"
            elif sec_score <= 40:
                base += f", and {sector} is lagging (score {sec_score})"
    return base + "."


# ═══════════════════════════════════════════════════════════════════════════════
# 10. ACTION SENTENCE GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _action_sentence(action: str, verdict: str, entry_ceiling: float,
                     forecasts: dict, horizon: str) -> str:
    """Verdict + three-horizon read in one sentence."""
    tone = TONE_CONFIG.get(horizon, TONE_CONFIG["medium"])

    action_map = {
        "ADD": f"Verdict: {verdict} — {tone['action_frame']} toward ₹{entry_ceiling:,.0f}",
        "WAIT_FOR_DIP": f"Verdict: {verdict} — the setup is good but the entry is rich; wait for price to come to you near ₹{entry_ceiling:,.0f}",
        "HOLD": f"Verdict: HOLD — if you own it, let it work; if you don't, there are cleaner setups elsewhere",
        "BLACKOUT": "Verdict: EARNINGS BLACKOUT — no fresh adds until results are printed and digested",
        "TRIM": "Verdict: BOOK PARTIAL — take 30–50% off the table and trail the stop on the rest",
        "EXIT": "Verdict: EXIT — the rule ladder has flagged it, clear the position at open and redeploy capital",
        "DO_NOT_TRADE": "Verdict: DO NOT TRADE — data or liquidity is unreliable",
    }
    core = action_map.get(action, f"Verdict: {verdict} — review and decide")

    # Three-horizon summary
    biases = []
    for key, label in [("1d", "tomorrow"), ("1w", "this week"), ("1m", "this month")]:
        fc = forecasts.get(key, {})
        b = fc.get("bias", "Neutral")
        biases.append(f"{b} {label}")
    horizon_line = " · ".join(biases)

    return f"{core}. Outlook: {horizon_line}."


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY: Deterministic phrase picker (anti-repetition)
# ═══════════════════════════════════════════════════════════════════════════════

def _pick_phrase(pool: list[str], seed: str) -> str:
    """Pick a phrase deterministically based on seed (symbol + date context)."""
    if not pool:
        return ""
    h = int(hashlib.md5(seed.encode()).hexdigest(), 16)
    return pool[h % len(pool)]


def _pick_from_pool(pool_dict: dict, key: str, seed: str) -> str:
    """Pick from a keyed phrase pool, falling back gracefully."""
    phrases = pool_dict.get(key, [])
    if not phrases:
        # Try a generic fallback
        for fallback_key in pool_dict:
            if pool_dict[fallback_key]:
                return _pick_phrase(pool_dict[fallback_key], seed)
        return ""
    return _pick_phrase(phrases, seed)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN: build_narrative (drop-in replacement)
# ═══════════════════════════════════════════════════════════════════════════════

def build_narrative(symbol: str, name: str, sector: str,
                    tech: dict, price: float, change_pct: float,
                    action: str, verdict: str,
                    news_info: dict, regime: dict, sector_momentum: list,
                    forecasts: dict, entry_ceiling: float = 0,
                    horizon: str = "medium") -> str:
    """
    Build a rich, human-like narrative for a stock pick.

    Returns a multi-sentence narrative string. Never raises.

    Parameters match the existing advisor_narrator.build_narrative() signature
    with one addition: `horizon` (short/medium/long) for tone control.
    """
    try:
        seed = f"{symbol}_{(regime or {}).get('label', 'X')}"

        # ── Derived intelligence ──
        intel = derive_intelligence(tech, price, change_pct, regime)

        # ── Score explanation ──
        components = tech.get("components") or {}
        score = tech.get("score") or 0
        score_line = _explain_score(score, components, intel)

        # ── Assemble paragraphs ──
        paragraphs = []

        # P1: Trend + Momentum (from phrase pools)
        trend_phrase = _pick_from_pool(TREND_PHRASES, intel["trend_quality"], seed + "_t")
        mom_phrase = _pick_from_pool(MOMENTUM_PHRASES, intel["momentum_state"], seed + "_m")
        p1 = f"{symbol} ({score_line}) {trend_phrase} {mom_phrase}"
        paragraphs.append(p1)

        # P2: Conflict detection (if any)
        conflicts = _detect_conflicts(intel, tech, news_info or {})
        if conflicts:
            paragraphs.append(" ".join(conflicts))

        # P3: News layer
        news_line = _news_sentence(news_info)
        if news_line:
            paragraphs.append(news_line)

        # P4: Regime + sector context
        regime_line = _regime_sentence(regime, sector, sector_momentum)
        paragraphs.append(regime_line)

        # P5: Entry quality insight
        entry_phrase = _pick_from_pool(ENTRY_PHRASES, intel["entry_quality"], seed + "_e")
        if entry_phrase and action in ("ADD", "WAIT_FOR_DIP"):
            paragraphs.append(entry_phrase)

        # P6: Scenarios (bull/bear)
        atr = tech.get("atr") or (price * 0.02 if price else 0)
        scenarios = _build_scenarios(symbol, price, atr, tech, intel, forecasts or {})
        scenario_text = f"Bull case: {scenarios['bull_case']} Bear case: {scenarios['bear_case']}"
        paragraphs.append(scenario_text)

        # P7: Forward-looking + Action
        fwd = _forward_looking(symbol, price, atr, intel, forecasts or {})
        action_line = _action_sentence(action, verdict, entry_ceiling or price,
                                        forecasts or {}, horizon)
        paragraphs.append(f"{fwd} {action_line}")

        return " ".join(paragraphs)

    except Exception as e:
        log.warning("narrative_engine failed for %s: %s", symbol, e)
        return (f"{symbol}: {verdict}. Score {tech.get('score', 0)}/100. "
                f"Review the technical and news panels for details.")


# ═══════════════════════════════════════════════════════════════════════════════
# STRUCTURED OUTPUT (for frontend rendering)
# ═══════════════════════════════════════════════════════════════════════════════

def build_narrative_structured(symbol: str, name: str, sector: str,
                                tech: dict, price: float, change_pct: float,
                                action: str, verdict: str,
                                news_info: dict, regime: dict,
                                sector_momentum: list,
                                forecasts: dict, entry_ceiling: float = 0,
                                horizon: str = "medium") -> dict:
    """
    Returns a structured dict with all narrative components for frontend use.
    The frontend can render each section independently with custom styling.
    """
    try:
        seed = f"{symbol}_{(regime or {}).get('label', 'X')}"
        intel = derive_intelligence(tech, price, change_pct, regime)
        components = tech.get("components") or {}
        score = tech.get("score") or 0
        atr = tech.get("atr") or (price * 0.02 if price else 0)

        conflicts = _detect_conflicts(intel, tech, news_info or {})
        scenarios = _build_scenarios(symbol, price, atr, tech, intel, forecasts or {})

        return {
            "narrative": build_narrative(
                symbol, name, sector, tech, price, change_pct,
                action, verdict, news_info, regime, sector_momentum,
                forecasts, entry_ceiling, horizon
            ),
            "intelligence": intel,
            "score_explanation": _explain_score(score, components, intel),
            "conflicts": conflicts,
            "scenarios": scenarios,
            "forward_looking": _forward_looking(symbol, price, atr, intel, forecasts or {}),
            "news_context": _news_sentence(news_info),
            "regime_context": _regime_sentence(regime, sector, sector_momentum),
            "tone": TONE_CONFIG.get(horizon, TONE_CONFIG["medium"])["voice"],
        }
    except Exception as e:
        log.warning("narrative_engine structured failed for %s: %s", symbol, e)
        return {
            "narrative": f"{symbol}: {verdict}. Score {tech.get('score', 0)}/100.",
            "intelligence": {},
            "score_explanation": "",
            "conflicts": [],
            "scenarios": {},
            "forward_looking": "",
            "news_context": "",
            "regime_context": "",
            "tone": "medium",
        }
