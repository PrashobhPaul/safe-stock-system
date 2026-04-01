from http.server import BaseHTTPRequestHandler
import json
import os
import requests
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"


def call_gemini(prompt: str, api_key: str) -> dict:
    url  = f"{GEMINI_URL}?key={api_key}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature":      0.35,
            "maxOutputTokens":  1200,
            "responseMimeType": "application/json",
        },
    }
    r = requests.post(url, json=body, timeout=25)
    r.raise_for_status()
    text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
    text = text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    return json.loads(text)


def build_prompt(predictions: dict, mode: str, symbol: str = "", sector: str = "") -> str:
    picks = predictions.get("top_picks", [])[:10]
    secs  = predictions.get("sector_momentum", [])[:6]
    date  = predictions.get("market_date", "today")

    if mode == "stock" and symbol:
        pick = next((p for p in predictions.get("top_picks", []) if p["symbol"] == symbol), None)
        if not pick:
            return None
        ind = pick.get("indicators", {})
        tp  = pick.get("trade_plan", {})
        entry = tp.get("entry", {})
        exit_ = tp.get("exit", {})
        return f"""Analyse {pick['symbol']} ({pick.get('name','')}) for a retail NSE investor.
Score:{pick['score']}/100 Signal:{pick['signal']} Sector:{pick.get('sector','')}
Price:₹{pick['current_price']} Change:{pick['change_pct']}%
Entry:₹{entry.get('ideal_price','')} Target:₹{exit_.get('target_ideal','')} Hold:{exit_.get('hold_duration','')}
RSI:{ind.get('rsi','')} MACD:{ind.get('macd_signal','')} Vol:{ind.get('volume_ratio','')}x
SMA:{ind.get('sma_alignment','')} 52W:{ind.get('week52_pct','')}% away
Signals: {' | '.join(pick.get('reasons',[]))}

Respond ONLY with JSON (no markdown):
{{"summary":"3-4 sentence view","bullish_case":"2-3 sentences","bearish_case":"2-3 sentences","entry_strategy":"practical entry advice","exit_strategy":"when to exit","position_sizing":"% portfolio suggestion","verdict":"BUY_TOMORROW|WAIT_FOR_PULLBACK|AVOID|WATCH","one_line":"single sentence thesis"}}"""

    if mode == "quick":
        lines = [f"{p['symbol']} Score:{p['score']} RSI:{p['indicators'].get('rsi','?')} Vol:{p['indicators'].get('volume_ratio','?')}x Chg:{p['change_pct']}%" for p in picks[:5]]
        return f"""NSE top stocks today:
{chr(10).join(lines)}

Respond ONLY with JSON:
{{"overall_sentiment":"BULLISH|MILDLY_BULLISH|NEUTRAL|MILDLY_BEARISH|BEARISH","sentiment_reason":"one sentence","top_insight":"one sentence","momentum_read":"one sentence"}}"""

    # Full briefing
    up_secs   = [f"{s['sector']}({s['score']})" for s in secs if s['trend'] == 'up']
    down_secs = [f"{s['sector']}({s['score']})" for s in secs if s['trend'] == 'down']
    brd       = predictions.get("market_breadth", {})
    brd_str   = f"Adv:{brd.get('advances',0):,} Dec:{brd.get('declines',0):,}"

    pick_lines = []
    for p in picks:
        ind = p.get("indicators", {})
        sc  = p.get("scores", {})
        tp  = p.get("trade_plan", {})
        entry = tp.get("entry", {}) if tp else {}
        exit_ = tp.get("exit", {}) if tp else {}
        pick_lines.append(
            f"#{p.get('rank','?')} {p['symbol']} Score:{p['score']}/100 {p['signal']} "
            f"₹{p['current_price']} ({'+' if p['change_pct']>=0 else ''}{p['change_pct']}%) "
            f"Entry:₹{entry.get('ideal_price','')} Target:₹{exit_.get('target_ideal','')} "
            f"Hold:{exit_.get('hold_duration','')} "
            f"RSI:{ind.get('rsi','?')} MACD:{ind.get('macd_signal','?')} Vol:{ind.get('volume_ratio','?')}x"
        )

    return f"""You are a senior NSE equity analyst. Date:{date}
Market Breadth:{brd_str}
Strong sectors:{', '.join(up_secs) if up_secs else 'mixed'}
Weak sectors:{', '.join(down_secs) if down_secs else 'none'}

TOP PICKS:
{chr(10).join(pick_lines)}

Write a professional daily briefing. Be specific. 2-3 sentences per stock narrative.
Do NOT invent data. Respond ONLY with JSON (no markdown, no backticks):
{{"market_summary":"2-3 sentence overview","overall_sentiment":"BULLISH|MILDLY_BULLISH|NEUTRAL|MILDLY_BEARISH|BEARISH","sentiment_reason":"one sentence","top_insight":"single most important thing for tomorrow","momentum_read":"one sentence on momentum","stock_narratives":{{"SYMBOL":"2-3 sentence analysis"}},"sector_themes":["theme1","theme2","theme3"],"risks_to_watch":["risk1","risk2","risk3"],"best_risk_reward":"SYMBOL — one sentence","beginner_tip":"one practical tip","confidence_note":"honest limitation note"}}"""


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_POST(self):
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            self._json({"error": "GEMINI_API_KEY not set in Vercel environment variables"}, 500)
            return

        try:
            length      = int(self.headers.get("Content-Length", 0))
            body        = json.loads(self.rfile.read(length)) if length else {}
            predictions = body.get("predictions", {})
            mode        = body.get("mode", "full")
            symbol      = body.get("symbol", "")
            sector      = body.get("sector", "")
        except Exception:
            self._json({"error": "Invalid JSON body"}, 400)
            return

        if not predictions:
            self._json({"error": "predictions field required"}, 400)
            return

        try:
            prompt = build_prompt(predictions, mode, symbol, sector)
            if not prompt:
                self._json({"error": f"Symbol {symbol} not found"}, 404)
                return

            result = call_gemini(prompt, api_key)
            result["mode"]         = mode
            result["generated_at"] = datetime.now(IST).isoformat()
            result["model_used"]   = "Google Gemini 1.5 Flash"
            result["provider"]     = "gemini-1.5-flash"

            self._json({"llm_analysis": result, "generated_at": result["generated_at"]})

        except requests.HTTPError as e:
            self._json({"error": f"Gemini API error {e.response.status_code}"}, 502)
        except json.JSONDecodeError as e:
            self._json({"error": f"Gemini returned invalid JSON: {str(e)}"}, 502)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _json(self, data: dict, status: int = 200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def log_message(self, *args):
        pass
