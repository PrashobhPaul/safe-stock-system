# ProfitPilot — Enterprise PWA

> AI-powered Indian stock market predictions. Zero login. Zero config. Open to all.
**[🌐 Live App →](https://prashobhpaul.github.io/ProfitPilot/)**

---

## Philosophy

- **Offline-first** — shows last cached data when offline
- **Three horizons** — Short (5–15d), Medium (4–12w), Long (6–18m)
- **Precise entry/exit** — limit order prices for first 30-60 min after market open

---

## File Structure

```
safe-stock-system/
├── index.html          ← PWA app (no login, instant load)
├── manifest.json       ← PWA manifest (makes it installable)
├── sw.js               ← Service worker (offline, caching)
├── offline.html        ← Shown when offline with no cache
├── predictions.json    ← Updated daily by GitHub Actions
├── generate_icons.py   ← Run once to create PNG icons
├── icons/              ← App icons (generate with script)
│   ├── icon-72.png
│   ├── icon-96.png
│   ├── icon-128.png
│   ├── icon-144.png
│   ├── icon-152.png
│   ├── icon-192.png
│   ├── icon-384.png
│   └── icon-512.png
├── analyze.py          ← Rule-based scoring engine
├── data_fetch.py       ← Yahoo Finance data fetcher
├── entry_exit.py       ← Entry/exit price calculator
├── llm_analyze.py      ← Gemini AI narrative layer
├── requirements.txt    ← Python deps
├── vercel.json         ← Vercel API config
├── api/
│   ├── quotes.py       ← Live scoring endpoint
│   └── briefing.py     ← Gemini briefing endpoint
└── .github/
    └── workflows/
        └── daily.yml   ← Runs daily at 4:15 PM IST
```

---

## Setup Steps

### Step 1 — Generate Icons (run once)

```bash
python generate_icons.py
git add icons/
git commit -m "Add PWA icons"
git push
```

### Step 2 — Enable GitHub Pages

Settings → Pages → Deploy from main branch → / (root)

Your app: `https://PrashobhPaul.github.io/safe-stock-system`

### Step 3 (Optional) — Add Vercel for live data

1. Deploy repo to [vercel.com](https://vercel.com) (free)
2. Add `GEMINI_API_KEY` in Vercel → Settings → Environment Variables
3. In `index.html`, find this line and paste your Vercel URL:
   ```javascript
   const VERCEL_URL = 'https://safe-stock-system.vercel.app';
   ```
4. Commit & push — live updates activate automatically

---

## Convert to Android APK (3 methods)

### Method A — PWABuilder (Easiest, 5 minutes)

1. Go to [pwabuilder.com](https://www.pwabuilder.com)
2. Enter your GitHub Pages URL
3. Click **Start** → **Android** → **Download Package**
4. You get a signed APK ready for sideloading or Play Store submission

### Method B — Bubblewrap (Google TWA)

```bash
npm install -g @bubblewrap/cli
bubblewrap init --manifest https://PrashobhPaul.github.io/safe-stock-system/manifest.json
bubblewrap build
```

Output: `app-release-signed.apk`

### Method C — Capacitor (Ionic)

```bash
npm install @capacitor/core @capacitor/android
npx cap init StockSage com.stocksage.india
npx cap add android
npx cap copy
npx cap open android   # Opens in Android Studio
```

---

## How the No-Login Flow Works

```
User visits URL (or opens installed app)
         ↓
index.html loads immediately
         ↓
Checks VERCEL_URL in code
  ├── Set → Fetches live /api/quotes (Vercel)
  └── Not set → Fetches predictions.json (GitHub)
         ↓
Renders all three horizon panels
No prompts. No login. No configuration.
```

---

## Data Flow

```
[GitHub Actions — 4:15 PM IST daily]
  data_fetch.py → stock_data.db → analyze.py + entry_exit.py
  → llm_analyze.py (Gemini via Vercel env var, key never in GitHub)
  → predictions.json → git push → GitHub Pages auto-deploys

[User opens app]
  → Loads predictions.json (or live Vercel API)
  → Sees Short/Medium/Long picks with entry/exit prices
  → Service worker caches for offline access
```

---

## Disclaimer

For educational purposes only. Not financial advice.
The no-stop-loss approach carries significant risk.
Always consult a SEBI-registered financial advisor.
