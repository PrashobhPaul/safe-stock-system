"""
narrative_engine.py — ProfitPilot Narrative Engine v2.0
========================================================
Generates human-like stock analysis that reads differently for every stock.

Key design principles:
  - NEVER the same sentence order for two stocks
  - Lead with the most interesting signal (conflict, breakout, news, momentum)
  - Mix sentence lengths: punchy one-liners + analytical multi-clause
  - Stock personality: a 60-score BUY ≠ a 47-score HOLD in tone or structure
  - Controlled randomization via deterministic seed (symbol hash)

Drop-in replacement: same signature as advisor_narrator.build_narrative()
"""
from __future__ import annotations

import hashlib
import logging
from typing import Optional

log = logging.getLogger("narrative_engine")


# ═══════════════════════════════════════════════════════════════════════════════
# DERIVED INTELLIGENCE LAYERS
# ═══════════════════════════════════════════════════════════════════════════════

def _derive_momentum_state(rsi: float, volume_ratio: float, change_pct: float) -> str:
    if rsi >= 78:
        return "overheated"
    if rsi >= 68 and volume_ratio >= 1.3:
        return "strong_confirmed"
    if rsi >= 68:
        return "strong_extended"
    if 55 <= rsi < 68 and change_pct > 0:
        return "building"
    if 45 <= rsi < 55:
        return "neutral"
    if 35 <= rsi < 45:
        return "fading"
    if rsi < 35:
        return "washed_out"
    return "neutral"


def _derive_trend_quality(score: int, sma: str, adx: Optional[float]) -> str:
    sma = sma or ""
    if "Full Bull" in sma and score >= 60:
        return "pristine"
    if "Full Bull" in sma:
        return "strong"
    if "Bull" in sma and "Full" not in sma and score >= 50:
        return "constructive"
    if "Above 200" in sma:
        return "early"
    if "Bearish" in sma:
        return "broken"
    return "absent"


def _derive_entry_quality(rsi: float, bb_pos: float, change_pct: float,
                           vol_ratio: float) -> str:
    if rsi >= 75 and change_pct > 2:
        return "chasing"
    if rsi >= 70 and bb_pos >= 0.95:
        return "stretched"
    if 50 <= rsi <= 62 and vol_ratio >= 1.2:
        return "ideal"
    if rsi <= 45 and change_pct < -1:
        return "falling_knife"
    if 55 <= rsi <= 68 and 0.5 <= bb_pos <= 0.8:
        return "clean"
    return "neutral"


def _derive_risk_state(atr_pct: float, rsi: float, change_pct: float,
                        regime_label: str) -> str:
    if atr_pct >= 4.5 and rsi >= 75:
        return "high_vol_overbought"
    if atr_pct >= 4.0:
        return "volatile"
    if regime_label == "BEAR" and rsi < 45:
        return "regime_hostile"
    if rsi >= 78:
        return "overbought"
    if change_pct < -3:
        return "sharp_decline"
    if atr_pct <= 1.8 and 45 <= rsi <= 65:
        return "low_risk"
    return "moderate"


def _derive(tech: dict, price: float, change_pct: float, regime: dict) -> dict:
    rsi = tech.get("rsi") or 50
    vol_ratio = tech.get("volume_ratio") or 1.0
    score = tech.get("score") or 50
    sma = tech.get("trend_label") or tech.get("sma_alignment") or ""
    adx = tech.get("adx")
    atr_pct = tech.get("atr_pct") or (
        ((tech.get("atr") or 0) / price * 100) if price else 0
    )
    bb_pos = tech.get("bb_position") or 0.5
    regime_label = (regime or {}).get("label", "UNKNOWN")

    return {
        "momentum":  _derive_momentum_state(rsi, vol_ratio, change_pct),
        "trend":     _derive_trend_quality(score, sma, adx),
        "entry":     _derive_entry_quality(rsi, bb_pos, change_pct, vol_ratio),
        "risk":      _derive_risk_state(atr_pct, rsi, change_pct, regime_label),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONFLICT DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_conflicts(intel: dict, tech: dict, news_info: dict) -> list[str]:
    conflicts = []
    rsi = tech.get("rsi") or 50
    vol_ratio = tech.get("volume_ratio") or 1.0
    change_pct = tech.get("change_pct") or 0
    news_label = (news_info or {}).get("label", "NEUTRAL")

    if intel["trend"] in ("pristine", "strong") and intel["momentum"] in ("fading", "neutral"):
        conflicts.append("trend_mom_diverge")
    if intel["momentum"] in ("strong_confirmed", "strong_extended") and intel["trend"] in ("broken", "absent"):
        conflicts.append("mom_no_trend")
    if change_pct > 1.5 and vol_ratio < 0.8:
        conflicts.append("rally_no_volume")
    if news_label in ("BEARISH", "MILDLY_BEARISH") and intel["trend"] in ("pristine", "strong"):
        conflicts.append("news_vs_chart")
    if news_label in ("BULLISH", "MILDLY_BULLISH") and intel["trend"] in ("broken", "absent"):
        conflicts.append("news_ahead_of_chart")
    if rsi >= 72 and vol_ratio < 0.9:
        conflicts.append("overbought_thin")

    return conflicts[:2]


CONFLICT_TEXT = {
    "trend_mom_diverge": [
        "Here's the wrinkle: the trend is intact but momentum has started to cool. That usually means consolidation ahead, not a collapse — but chasing here is risky.",
        "One thing to watch: momentum is diverging from the trend. The structure is fine but the engine is losing steam.",
    ],
    "mom_no_trend": [
        "A word of caution — momentum is surging but the moving average structure hasn't confirmed. This could be a counter-trend bounce rather than a genuine reversal.",
        "The energy is there but the foundation isn't. Until the 50-day crosses above the 200-day, treat this as a trade, not a position.",
    ],
    "rally_no_volume": [
        "The move higher is happening on thin volume. Low-conviction rallies have a habit of fading just when you commit.",
        "Volume isn't backing this advance. That's a yellow flag — real breakouts come with participation.",
    ],
    "news_vs_chart": [
        "Headlines are negative but the chart hasn't cracked. In these situations, price usually leads sentiment — watch what it does, not what they say.",
        "Interesting disconnect: the news tone is bearish but the technical structure holds.",
    ],
    "news_ahead_of_chart": [
        "Positive headlines haven't translated into chart improvement yet. Until the averages confirm, the story is running ahead of the evidence.",
    ],
    "overbought_thin": [
        "Overbought on declining volume — a classic combination for a pullback or at minimum a breather.",
        "RSI is stretched and the volume is thinning. That's the kind of setup where profit-taking hits without warning.",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# NARRATIVE BUILDING BLOCKS
# ═══════════════════════════════════════════════════════════════════════════════

def _pick(pool: list[str], seed: str) -> str:
    if not pool:
        return ""
    h = int(hashlib.md5(seed.encode()).hexdigest(), 16)
    return pool[h % len(pool)]


# ── OPENERS ──

def _opener_breakout(sym: str, score: int, w52_pct: float) -> str:
    if w52_pct <= 2:
        return f"{sym} is knocking on the door of its 52-week high, just {w52_pct}% away."
    if w52_pct <= 5:
        return f"{sym} has closed within striking distance of its 52-week high — {w52_pct}% to go."
    return ""

def _opener_momentum(sym: str, rsi: float, vol_ratio: float) -> str:
    if rsi >= 75 and vol_ratio >= 1.5:
        return f"{sym} is on fire — RSI at {rsi:.0f} with volume running {vol_ratio:.1f}x average. Buyers are aggressive."
    if rsi >= 70:
        return f"{sym}'s momentum is running hot with RSI at {rsi:.0f}. The question is whether this leg has more room or needs a breather."
    if rsi <= 35:
        return f"{sym} is deeply oversold at RSI {rsi:.0f}. The selling may be overdone, but catching falling knives requires conviction."
    return ""

def _opener_news(sym: str, news_info: dict) -> str:
    if not news_info or not news_info.get("count"):
        return ""
    label = news_info.get("label", "NEUTRAL")
    headline = news_info.get("top_headline", "")
    if label in ("BULLISH", "MILDLY_BULLISH") and headline:
        h = headline[:90].rsplit(" ", 1)[0] if len(headline) > 90 else headline
        return f"The newsflow around {sym} is constructive — '{h}' is the lead story driving sentiment."
    if label in ("BEARISH", "MILDLY_BEARISH") and headline:
        h = headline[:90].rsplit(" ", 1)[0] if len(headline) > 90 else headline
        return f"{sym} is dealing with negative newsflow — '{h}' is the headline weighing on sentiment."
    return ""

def _opener_conflict(sym: str, conflicts: list[str], seed: str) -> str:
    if not conflicts:
        return ""
    c = conflicts[0]
    pool = CONFLICT_TEXT.get(c, [])
    if pool:
        return _pick(pool, seed + "_conflict_open")
    return ""

def _opener_score(sym: str, score: int, components: dict) -> str:
    if not components:
        return f"{sym} scores {score}/100 on our engine."
    sorted_comp = sorted(components.items(), key=lambda x: -x[1])
    top_name = sorted_comp[0][0].replace("_", " ")
    top_val = sorted_comp[0][1]
    bot_name = sorted_comp[-1][0].replace("_", " ")
    bot_val = sorted_comp[-1][1]
    if score >= 65:
        return f"{sym} is putting up a {score}/100 — {top_name} ({top_val}) is doing the heavy lifting."
    if score >= 50:
        return f"{sym} lands at {score}/100 — decent but not dominant. {top_name.capitalize()} is the strongest signal; {bot_name} is the weak link."
    if score >= 40:
        return f"{sym} comes in at {score}/100 — middling. {top_name.capitalize()} ({top_val}) provides some anchor but {bot_name} ({bot_val}) is dragging."
    return f"{sym} is struggling at {score}/100. Not much to like — {bot_name} ({bot_val}) sums up the problem."


# ── BODY SENTENCES ──

def _body_trend(intel: dict, seed: str) -> str:
    pools = {
        "pristine": [
            "The trend is about as clean as it gets — all moving averages stacked bullishly with price leading the parade.",
            "Full bullish alignment. 20 above 50, 50 above 200, price above everything. This is what institutional buying looks like on a chart.",
            "Structurally, this is textbook. Every major moving average is lined up, and dip-buyers have been rewarded consistently.",
        ],
        "strong": [
            "The trend is solidly positive — price sits above all key averages, though alignment isn't perfect.",
            "Moving averages are bullish. Not pristine, but the kind of structure where you buy dips rather than sell rallies.",
        ],
        "constructive": [
            "The setup is building. Price is above the 50 and 200-day averages — the foundation is there, it just needs follow-through.",
            "Trend-following signals are turning positive. Not enough for an aggressive bet, but enough to stay constructive.",
        ],
        "early": [
            "Price has reclaimed its 200-day average — the first hurdle for any recovery. But the 50-day hasn't confirmed yet, so this is early days.",
            "Above the 200 SMA. That's the bare minimum for trend followers, and it's only just been reclaimed.",
        ],
        "broken": [
            "The moving average structure is bearish — price is below key averages and rallies into them tend to fail.",
            "The trend is working against this name. Short of a major catalyst, the path of least resistance is lower.",
        ],
        "absent": [
            "There's no discernible trend. The stock is range-bound and waiting for direction.",
            "Directionless. Neither bulls nor bears have control, and that won't change without a catalyst.",
        ],
    }
    return _pick(pools.get(intel["trend"], []), seed + "_trend")


def _body_momentum(intel: dict, rsi: float, vol_ratio: float, seed: str) -> str:
    pools = {
        "overheated": [
            f"RSI at {rsi:.0f} — that's overcooked. The easy gains are behind you on this leg.",
            f"With RSI at {rsi:.0f}, this is the part of the move where late buyers provide exit liquidity for early ones.",
        ],
        "strong_confirmed": [
            f"Momentum is strong and backed by volume ({vol_ratio:.1f}x average). That's the kind of conviction that sustains moves.",
            f"RSI at {rsi:.0f} with {vol_ratio:.1f}x volume — buyers mean business.",
        ],
        "strong_extended": [
            f"RSI is at {rsi:.0f} — momentum is real but getting stretched. A consolidation here would be healthy, not alarming.",
            f"Buyers are in control (RSI {rsi:.0f}), but the stock is getting ahead of itself. Best entries come after a pause.",
        ],
        "building": [
            f"Momentum is picking up nicely — RSI at {rsi:.0f} has room to run before hitting overbought territory.",
            f"The momentum profile is constructive. RSI at {rsi:.0f} means the move is early-to-mid phase, not late innings.",
        ],
        "neutral": [
            f"Momentum is flat — RSI at {rsi:.0f} sits in no-man's land. The stock is waiting for a trigger.",
            f"RSI {rsi:.0f}. Neutral. Nothing to act on from momentum alone.",
        ],
        "fading": [
            f"Momentum is deteriorating — RSI has slipped to {rsi:.0f}. Buying interest is drying up.",
            f"The momentum read is weak. RSI at {rsi:.0f} suggests sellers are gradually gaining the upper hand.",
        ],
        "washed_out": [
            f"RSI has cratered to {rsi:.0f}. Oversold bounces are possible but tend to be sharp and unreliable.",
        ],
    }
    return _pick(pools.get(intel["momentum"], []), seed + "_mom")


def _body_volume_color(vol_ratio: float) -> str:
    if vol_ratio >= 2.0:
        return f"Volume at {vol_ratio:.1f}x average is speaking loudly."
    if vol_ratio >= 1.4:
        return f"Above-average volume ({vol_ratio:.1f}x) adds weight to the move."
    if vol_ratio <= 0.6:
        return f"Volume is thin ({vol_ratio:.1f}x) — take any move with a pinch of salt."
    return ""


def _body_news(news_info: dict) -> str:
    if not news_info or not news_info.get("count"):
        return ""
    count = news_info["count"]
    label = news_info.get("label", "NEUTRAL")
    headline = news_info.get("top_headline", "")
    source = news_info.get("top_source", "")
    if label in ("BULLISH", "MILDLY_BULLISH"):
        tone = "positive" if label == "BULLISH" else "mildly positive"
        lead = f"Newsflow is {tone} — {count} item{'s' if count > 1 else ''} in the last 48 hours"
    elif label in ("BEARISH", "MILDLY_BEARISH"):
        tone = "negative" if label == "BEARISH" else "cautious"
        lead = f"News sentiment is {tone} across {count} recent item{'s' if count > 1 else ''}"
    else:
        return ""
    if headline and source:
        h = headline[:95].rsplit(" ", 1)[0] if len(headline) > 95 else headline
        lead += f" — '{h}' ({source}) is the headline to watch"
    return lead + "."


def _body_regime(regime: dict, sector: str, sector_momentum: list) -> str:
    label = (regime or {}).get("label", "UNKNOWN")
    breadth = (regime or {}).get("breadth_pct", 0)
    if label == "BULL" and breadth >= 55:
        base = f"The market is helping — {breadth:.0f}% breadth in a bullish regime lifts most boats."
    elif label == "BULL":
        base = f"Regime is bullish but breadth is narrowing at {breadth:.0f}% — stay with the leaders."
    elif label == "BEAR":
        base = f"Market headwinds are real — {breadth:.0f}% breadth in a bearish regime means even good setups get sold."
    elif label == "SIDEWAYS":
        base = f"The tape is chopping sideways with {breadth:.0f}% breadth. Stock-picking matters more than market direction here."
    else:
        base = "Market regime is unclear — lean on individual setups."
    if sector and sector_momentum:
        sec = next((s for s in sector_momentum if s.get("sector") == sector), None)
        if sec:
            ss = sec.get("score", 50)
            if ss >= 58:
                base += f" {sector} is one of the stronger sectors right now (score {ss})."
            elif ss <= 38:
                base += f" {sector} is lagging the broader tape (score {ss}) — an extra headwind."
    return base


# ── SCENARIOS ──

def _scenarios(price: float, atr: float, intel: dict, forecasts: dict) -> tuple[str, str]:
    atr = atr or (price * 0.02)
    fc = forecasts.get("1w", {}).get("price_band", {})
    up = fc.get("high") or round(price + atr * 2.5, 0)
    dn = fc.get("low") or round(price - atr * 2.0, 0)
    if intel["trend"] in ("pristine", "strong"):
        bull = f"Bull case: sustains above ₹{price:,.0f} and pushes towards ₹{up:,.0f} as trend followers pile in."
        bear = f"Bear case: a slip below ₹{dn:,.0f} triggers trailing stops and dents the short-term thesis."
    elif intel["momentum"] in ("building", "strong_confirmed"):
        bull = f"Bull case: momentum carries to ₹{up:,.0f} if volume confirms."
        bear = f"Bear case: fades below ₹{dn:,.0f} — failed rally territory."
    else:
        bull = f"Bull case: catalyst above ₹{round(price * 1.03):,.0f} opens ₹{up:,.0f}."
        bear = f"Bear case: ₹{dn:,.0f} breaks and the range resolves lower."
    return bull, bear


# ── FORWARD-LOOKING ──

def _forward(price: float, atr: float, bias: str, seed: str) -> str:
    atr = atr or (price * 0.02)
    up = round(price + atr * 1.2, 0)
    dn = round(price - atr * 1.5, 0)
    pools = {
        "Bullish": [
            f"Continuation likely above ₹{up:,.0f} with volume expansion.",
            f"A push through ₹{up:,.0f} triggers the next wave of buying.",
        ],
        "Mildly Bullish": [
            f"Above ₹{round(price):,.0f}, the setup stays alive. ₹{up:,.0f} is the next hurdle.",
            f"Look for a close above ₹{up:,.0f} as confirmation.",
        ],
        "Neutral": [
            f"Range-bound between ₹{dn:,.0f} and ₹{up:,.0f}. Patience over conviction.",
            f"Needs to resolve ₹{dn:,.0f}–₹{up:,.0f} before a directional bet pays.",
        ],
        "Mildly Bearish": [
            f"Watch ₹{dn:,.0f} — a break below opens further downside.",
            f"₹{dn:,.0f} is the line in the sand.",
        ],
        "Bearish": [
            f"Downside risk towards ₹{dn:,.0f} is real. Capital preservation first.",
        ],
    }
    return _pick(pools.get(bias, pools["Neutral"]), seed + "_fwd")


# ── VERDICT ──

def _verdict_line(action: str, verdict: str, entry_ceil: float,
                  horizon: str, forecasts: dict) -> str:
    biases = []
    for k in ("1d", "1w", "1m"):
        b = (forecasts or {}).get(k, {}).get("bias", "Neutral")
        biases.append(b)
    outlook = f"{biases[0]} → {biases[1]} → {biases[2]}"

    if action == "ADD":
        speed = {"short": "speed matters", "medium": "let the trend work", "long": "no urgency"}.get(horizon, "")
        return f"Verdict: {verdict}. Entry zone near ₹{entry_ceil:,.0f} — {speed}. Outlook: {outlook}."
    if action == "WAIT_FOR_DIP":
        return f"Verdict: {verdict}. Setup is right, price isn't — wait for ₹{entry_ceil:,.0f}. Outlook: {outlook}."
    if action == "HOLD":
        return f"Verdict: HOLD. Own it? Let it work. Don't own it? Cleaner entries exist elsewhere. Outlook: {outlook}."
    if action == "EXIT":
        return f"Verdict: EXIT. Rule ladder triggered — clear at open. Outlook: {outlook}."
    if action == "TRIM":
        return f"Verdict: BOOK PARTIAL. Take 30–50% off, trail the rest. Outlook: {outlook}."
    if action == "BLACKOUT":
        return f"Verdict: EARNINGS BLACKOUT. No fresh adds until results print. Outlook: {outlook}."
    return f"Verdict: {verdict}. Review and decide. Outlook: {outlook}."


# ═══════════════════════════════════════════════════════════════════════════════
# NARRATIVE ASSEMBLY — DYNAMIC ORDERING (the core innovation)
# ═══════════════════════════════════════════════════════════════════════════════

def _choose_opener(sym: str, score: int, tech: dict, intel: dict,
                   news_info: dict, conflicts: list, seed: str) -> tuple[str, str]:
    """Pick the most interesting opening. Returns (opener_text, opener_type)."""
    rsi = tech.get("rsi") or 50
    vol_ratio = tech.get("volume_ratio") or 1.0
    w52 = tech.get("pct_from_52h") or tech.get("week52_pct") or 50
    components = tech.get("components") or {}

    # Priority 1: breakout proximity
    if w52 <= 3 and score >= 55:
        op = _opener_breakout(sym, score, w52)
        if op:
            return op, "breakout"

    # Priority 2: conflict
    if conflicts:
        op = _opener_conflict(sym, conflicts, seed)
        if op:
            return op, "conflict"

    # Priority 3: strong news
    if news_info and news_info.get("count", 0) >= 1 and abs(news_info.get("score", 0)) >= 1.5:
        op = _opener_news(sym, news_info)
        if op:
            return op, "news"

    # Priority 4: extreme momentum
    if rsi >= 75 or rsi <= 35:
        op = _opener_momentum(sym, rsi, vol_ratio)
        if op:
            return op, "momentum"

    # Default: score-based
    return _opener_score(sym, score, components), "score"


def build_narrative(symbol: str, name: str, sector: str,
                    tech: dict, price: float, change_pct: float,
                    action: str, verdict: str,
                    news_info: dict, regime: dict, sector_momentum: list,
                    forecasts: dict, entry_ceiling: float = 0,
                    horizon: str = "medium") -> str:
    """
    Build a rich, human-like narrative. Same signature as advisor_narrator.build_narrative()
    plus horizon param. Never raises.
    """
    try:
        seed = f"{symbol}_{(regime or {}).get('label', 'X')}_{horizon}"
        intel = _derive(tech, price, change_pct, regime or {})
        conflicts = _detect_conflicts(intel, tech, news_info or {})

        rsi = tech.get("rsi") or 50
        vol_ratio = tech.get("volume_ratio") or 1.0
        atr = tech.get("atr") or (price * 0.02 if price else 0)
        score = tech.get("score") or 0
        fc_1d_bias = (forecasts or {}).get("1d", {}).get("bias", "Neutral")

        parts = []

        # 1. Opener (dynamically chosen based on what's most interesting)
        opener, opener_type = _choose_opener(symbol, score, tech, intel, news_info, conflicts, seed)
        parts.append(opener)

        # 2. Body — skip sections the opener already covered
        if opener_type != "breakout":
            t = _body_trend(intel, seed)
            if t:
                parts.append(t)

        if opener_type != "momentum":
            m = _body_momentum(intel, rsi, vol_ratio, seed)
            if m:
                parts.append(m)

        v = _body_volume_color(vol_ratio)
        if v and opener_type != "momentum":
            parts.append(v)

        if conflicts and opener_type != "conflict":
            c = conflicts[0]
            pool = CONFLICT_TEXT.get(c, [])
            if pool:
                parts.append(_pick(pool, seed + "_conflict"))

        if opener_type != "news":
            n = _body_news(news_info)
            if n:
                parts.append(n)

        parts.append(_body_regime(regime, sector, sector_momentum))

        # Scenarios for actionable or high-score picks
        if action in ("ADD", "WAIT_FOR_DIP") or score >= 50:
            bull, bear = _scenarios(price, atr, intel, forecasts or {})
            parts.append(f"{bull} {bear}")

        parts.append(_forward(price, atr, fc_1d_bias, seed))
        parts.append(_verdict_line(action, verdict, entry_ceiling or price, horizon, forecasts))

        return " ".join(p for p in parts if p)

    except Exception as e:
        log.warning("narrative_engine failed for %s: %s", symbol, e)
        return (f"{symbol}: {verdict}. Score {tech.get('score', 0)}/100. "
                f"Review the technical and news panels for details.")


# ═══════════════════════════════════════════════════════════════════════════════
# STRUCTURED OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

def build_narrative_structured(symbol: str, name: str, sector: str,
                                tech: dict, price: float, change_pct: float,
                                action: str, verdict: str,
                                news_info: dict, regime: dict,
                                sector_momentum: list,
                                forecasts: dict, entry_ceiling: float = 0,
                                horizon: str = "medium") -> dict:
    try:
        seed = f"{symbol}_{(regime or {}).get('label', 'X')}_{horizon}"
        intel = _derive(tech, price, change_pct, regime or {})
        conflicts = _detect_conflicts(intel, tech, news_info or {})
        atr = tech.get("atr") or (price * 0.02 if price else 0)
        fc_1d_bias = (forecasts or {}).get("1d", {}).get("bias", "Neutral")
        bull, bear = _scenarios(price, atr, intel, forecasts or {})
        score = tech.get("score") or 0
        components = tech.get("components") or {}

        if components:
            top = max(components.items(), key=lambda x: x[1])
            bot = min(components.items(), key=lambda x: x[1])
            score_expl = f"{score}/100 — {top[0].replace('_', ' ')} leads ({top[1]}), {bot[0].replace('_', ' ')} lags ({bot[1]})"
        else:
            score_expl = f"{score}/100"

        conflict_texts = []
        for c in conflicts:
            pool = CONFLICT_TEXT.get(c, [])
            if pool:
                conflict_texts.append(_pick(pool, seed + f"_c_{c}"))

        return {
            "narrative": build_narrative(
                symbol, name, sector, tech, price, change_pct,
                action, verdict, news_info, regime, sector_momentum,
                forecasts, entry_ceiling, horizon
            ),
            "intelligence": intel,
            "score_explanation": score_expl,
            "conflicts": conflict_texts,
            "scenarios": {"bull_case": bull, "bear_case": bear},
            "forward_looking": _forward(price, atr, fc_1d_bias, seed),
            "news_context": _body_news(news_info),
            "regime_context": _body_regime(regime or {}, sector, sector_momentum),
            "tone": {"short": "tactical", "medium": "strategic", "long": "conviction"}.get(horizon, "strategic"),
        }
    except Exception as e:
        log.warning("narrative_engine structured failed for %s: %s", symbol, e)
        return {
            "narrative": f"{symbol}: {verdict}. Score {tech.get('score', 0)}/100.",
            "intelligence": {}, "score_explanation": "", "conflicts": [],
            "scenarios": {}, "forward_looking": "", "news_context": "",
            "regime_context": "", "tone": "strategic",
        }
