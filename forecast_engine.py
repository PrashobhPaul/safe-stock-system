"""
forecast_engine.py — three-horizon directional forecasts per stock
===================================================================
Rule-based forecasts for Tomorrow (1d), 1 Week (5d), 1 Month (21d).
Each horizon has: bias, confidence, price band (low/mid/high), catalyst, risk.
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("forecast_engine")


def _bias_from_score(tech_score: float, news_mod: float, regime_bias: float,
                     horizon_weight: float) -> tuple[str, str, float]:
    """
    Returns (bias_label, confidence_label, composite_score 0..100).
    horizon_weight: how much news and regime matter relative to technicals.
      1.0 for tomorrow (news dominates), 0.6 for week, 0.3 for month (tech dominates)
    """
    tech = tech_score or 50
    news_contrib = news_mod * horizon_weight * 1.5
    regime_contrib = regime_bias * horizon_weight

    composite = max(0, min(100, tech + news_contrib + regime_contrib))

    if composite >= 68:   bias = "Bullish"
    elif composite >= 55: bias = "Mildly Bullish"
    elif composite >= 45: bias = "Neutral"
    elif composite >= 32: bias = "Mildly Bearish"
    else:                 bias = "Bearish"

    # Confidence from agreement between technicals and news
    if tech >= 60 and news_mod >= 2:   conf = "High"
    elif tech <= 40 and news_mod <= -2: conf = "High"
    elif tech >= 55 and news_mod >= 0:  conf = "Medium"
    elif tech <= 45 and news_mod <= 0:  conf = "Medium"
    elif abs(tech - 50) < 5 and abs(news_mod) < 1: conf = "Low"
    else: conf = "Medium"

    return bias, conf, round(composite, 1)


def _price_band(price: float, atr: float, days: int, bias: str) -> dict:
    """
    Expected price range over `days` trading days.
    Uses ATR * sqrt(days) as the baseline move, skewed by bias.
    """
    if not price or price <= 0:
        return {"low": 0, "mid": 0, "high": 0}

    atr = atr or (price * 0.018)
    base_move = atr * (days ** 0.5)

    # Skew: bullish → mid above current, bearish → below
    skew_map = {
        "Bullish":         0.6,
        "Mildly Bullish":  0.25,
        "Neutral":         0.0,
        "Mildly Bearish": -0.25,
        "Bearish":        -0.6,
    }
    skew = skew_map.get(bias, 0.0)
    mid = price + base_move * skew
    half_width = base_move * 1.1  # 1.1σ-ish band

    return {
        "low":  round(mid - half_width, 2),
        "mid":  round(mid, 2),
        "high": round(mid + half_width, 2),
        "move_pct_mid": round((mid - price) / price * 100, 2),
    }


def _catalyst_for(horizon: str, bias: str, tech: dict, news_info: dict,
                  regime_label: str) -> str:
    """Pick the single most convincing catalyst for this horizon."""
    rsi = tech.get("rsi")
    score = tech.get("score", 0)
    trend = tech.get("trend_label", "")
    vol_ratio = tech.get("volume_ratio", 1)

    if news_info.get("count", 0) > 0 and abs(news_info.get("score", 0)) >= 1.5:
        return f"News flow: {news_info.get('top_headline', '')[:90]}"

    if horizon == "1d":
        if vol_ratio and vol_ratio >= 1.8:
            return f"Heavy volume today ({vol_ratio:.1f}x avg) with {trend.lower() or 'price action'}"
        if rsi and rsi >= 65:
            return f"RSI at {rsi:.0f} — momentum carryover likely"
        if rsi and rsi <= 35:
            return f"RSI at {rsi:.0f} — oversold bounce probable"
        return f"{trend or 'Mixed'} setup, score {score}/100"

    if horizon == "1w":
        if "Bull" in bias and score >= 60:
            return f"Trend intact ({trend}) with {score}/100 setup, week builds on momentum"
        if "Bear" in bias:
            return f"Weak setup ({score}/100) — expect continued pressure"
        return f"Range-bound week; {trend or 'neutral'} bias"

    # 1 month
    if "Bull" in bias:
        return f"Structural trend {trend.lower() or 'positive'}; {regime_label} regime supportive"
    if "Bear" in bias:
        return f"Structural weakness; {regime_label} regime adds pressure"
    return f"Consolidation phase in {regime_label} regime"


def _risk_for(horizon: str, bias: str, tech: dict, news_info: dict) -> str:
    rsi = tech.get("rsi") or 50
    if "Bull" in bias and rsi >= 72:
        return f"Overbought (RSI {rsi:.0f}) — chase risk high"
    if "Bear" in bias and rsi <= 30:
        return f"Oversold (RSI {rsi:.0f}) — short-squeeze possible"
    if news_info.get("label") == "BEARISH":
        return "Negative news flow could accelerate downside"
    if news_info.get("label") == "BULLISH":
        return "News-driven spike may fade without volume confirmation"
    if horizon == "1d":
        return "Gap risk at open; size positions accordingly"
    if horizon == "1w":
        return "Regime shift or sector rotation could invalidate setup"
    return "Macro shocks or earnings surprises are the biggest swing factor"


def forecast_stock(tech: dict, price: float, atr: float, news_info: dict,
                   regime: dict, sector: str = "") -> dict:
    """
    Produces {tomorrow, week, month} forecasts. Never raises.
    """
    try:
        tech_score = tech.get("score") or 50
        news_mod = news_info.get("modifier", 0) if news_info else 0

        regime_label = (regime or {}).get("label", "UNKNOWN")
        regime_bias_map = {"BULL": 6, "SIDEWAYS": 0, "BEAR": -6, "UNKNOWN": 0}
        regime_bias = regime_bias_map.get(regime_label, 0)

        # Horizon weights — news fades with time, regime matters more long-term
        horizons = [
            ("1d",  "Tomorrow",  1,  1.0, 0.8),
            ("1w",  "1 Week",    5,  0.6, 1.0),
            ("1m",  "1 Month",   21, 0.3, 1.4),
        ]

        forecasts = {}
        for key, label, days, news_w, regime_w in horizons:
            bias, conf, composite = _bias_from_score(
                tech_score, news_mod, regime_bias * regime_w / max(regime_w, 1),
                news_w
            )
            band = _price_band(price, atr, days, bias)
            catalyst = _catalyst_for(key, bias, tech, news_info or {}, regime_label)
            risk = _risk_for(key, bias, tech, news_info or {})

            forecasts[key] = {
                "label":      label,
                "days":       days,
                "bias":       bias,
                "confidence": conf,
                "composite":  composite,
                "price_band": band,
                "catalyst":   catalyst,
                "primary_risk": risk,
            }
        return forecasts
    except Exception as e:
        log.warning("forecast_stock failed: %s", e)
        return {}
