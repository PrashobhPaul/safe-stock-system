from http.server import BaseHTTPRequestHandler
import json
import os
import urllib.request
import urllib.error
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models"
    "/gemini-1.5-flash:generateContent"
)


def call_gemini(prompt: str, api_key: str) -> dict:
    """Call Gemini using only stdlib urllib — no requests dependency needed."""
    url  = f"{GEMINI_URL}?key={api_key}"
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature":      0.3,
            "maxOutputTokens":  800,
            "responseMimeType": "application/json",
        },
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    text = data["candidates"][0]["content"]["parts"][0]["text"]
    text = text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return json.loads(text.strip())


def build_lean_prompt(predictions: dict) -> str:
    """Build a compact prompt — only essential data, no huge JSON blobs."""
    picks   = predictions.get("top_picks", [])[:8]
    secs    = predictions.get("sector_momentum", [])[:5]
    brd     = predictions.get("market_breadth", {})
    date    = predictions.get("market_date", "today")
    total   = predictions.get("stocks_analyzed", 0)

    # Compact pick lines
    lines = []
    for p in picks:
        ind = p.get("indicators", {})
        tp  = p.get("trade_plan", {})
        ep  = tp.get("entry", {}).get("ideal_price", "?") if tp else "?"
        tgt = tp.get("exit", {}).get("target_ideal", "?") if tp else "?"
        hld = tp.get("exit", {}).get("hold_duration", "?") if tp else "?"
        lines.append(
            f"#{p.get('rank','?')} {p['symbol']} "
            f"Score:{p['score']}/100 {p['signal']} "
            f"₹{p['current_price']} ({'+' if p.get('change_pct',0)>=0 else ''}{p.get('change_pct',0)}%) "
            f"Entry:₹{ep} Target:₹{tgt} Hold:{hld} "
            f"RSI:{ind.get('rsi','?')} Vol:{ind.get('volume_ratio','?')}x "
            f"SMA:{ind.get('sma_alignment','?')}"
        )

    up_secs   = [s["sector"] for s in secs if s.get("trend") == "up"]
    down_secs = [s["sector"] for s in secs if s.get("trend") == "down"]
    adv = brd.get("advances", 0)
    dec = brd.get("declines", 0)

    prompt = f"""You are a senior NSE equity analyst. Date: {date}
{total} stocks analyzed. Advance/Decline: {adv:,}/{dec:,}
Strong sectors: {', '.join(up_secs) or 'mixed'}
Weak sectors: {', '.join(down_secs) or 'none'}

TOP PICKS:
{chr(10).join(lines) if lines else 'No picks today'}

Write a brief professional market briefing for Indian retail investors.
Be specific, honest, and mention key risks.

Respond ONLY with this exact JSON structure (no markdown, no explanation):
{{
  "market_summary": "2-3 sentence market overview for tomorrow",
  "overall_sentiment": "BULLISH",
  "sentiment_reason": "one sentence why",
  "top_insight": "single most important thing investors should know",
  "sector_themes": ["theme1", "theme2"],
  "risks_to_watch": ["risk1", "risk2"],
  "best_risk_reward": "SYMBOL — one sentence on best setup",
  "beginner_tip": "one practical tip for new investors",
  "stock_narratives": {{
    "SYMBOL": "2 sentence analysis"
  }}
}}"""
    return prompt


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        """Health check endpoint."""
        api_key = os.environ.get("GEMINI_API_KEY", "")
        status = {
            "status": "ok",
            "gemini_key_set": bool(api_key),
            "timestamp": datetime.now(IST).isoformat(),
        }
        self._json(status)

    def do_POST(self):
        # Check API key first
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            self._json({
                "error": "GEMINI_API_KEY not configured in Vercel environment variables. Go to Vercel → Settings → Environment Variables → Add GEMINI_API_KEY"
            }, 500)
            return

        # Parse request body
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 0:
                raw  = self.rfile.read(length)
                body = json.loads(raw)
            else:
                body = {}
        except Exception as e:
            self._json({"error": f"Invalid request body: {str(e)}"}, 400)
            return

        predictions = body.get("predictions", {})
        if not predictions:
            self._json({"error": "Missing predictions data in request body"}, 400)
            return

        # Build prompt and call Gemini
        try:
            prompt = build_lean_prompt(predictions)
            result = call_gemini(prompt, api_key)

            # Add metadata
            result["generated_at"] = datetime.now(IST).isoformat()
            result["model_used"]   = "Google Gemini 1.5 Flash"
            result["provider"]     = "gemini-1.5-flash"

            self._json({"llm_analysis": result})

        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="ignore")[:300]
            if e.code == 400:
                self._json({"error": f"Gemini rejected request (400): {body_text}"}, 502)
            elif e.code == 403:
                self._json({"error": "Gemini API key invalid or quota exceeded (403)"}, 502)
            elif e.code == 429:
                self._json({"error": "Gemini rate limit hit (429). Try again in 60 seconds."}, 429)
            else:
                self._json({"error": f"Gemini HTTP error {e.code}: {body_text}"}, 502)

        except json.JSONDecodeError as e:
            self._json({"error": f"Gemini returned invalid JSON: {str(e)[:100]}"}, 502)

        except TimeoutError:
            self._json({"error": "Gemini request timed out (20s). Try again."}, 504)

        except Exception as e:
            self._json({"error": f"Internal error: {str(e)}"}, 500)

    def _json(self, data: dict, status: int = 200):
        body = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type",   "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def log_message(self, *args):
        pass
