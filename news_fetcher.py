"""
news_fetcher.py — RSS-only news layer for StockSage/ProfitPilot
================================================================
Fetches ~20 Indian + global RSS feeds in parallel with per-feed timeouts.
Never crashes the pipeline — failed feeds return empty lists.
Output: list[dict] with keys: title, summary, link, published_ts, source, weight, category
"""
from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Any

try:
    import feedparser
except ImportError:
    feedparser = None

log = logging.getLogger("news_fetcher")

# Feed registry — (source, url, weight, category)
# weight: 1.0 baseline · regulatory boosted · global lower
FEEDS: list[dict] = [
    # Regulatory / official (highest weight)
    {"source": "PIB Finance",         "url": "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",                     "weight": 1.6, "category": "policy"},
    {"source": "PIB All India",       "url": "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=0",                     "weight": 1.4, "category": "policy"},
    {"source": "RBI Press",           "url": "https://www.rbi.org.in/pressreleases_rss.xml",                               "weight": 1.6, "category": "policy"},

    # Indian financial press (high weight)
    {"source": "Moneycontrol Latest", "url": "https://www.moneycontrol.com/rss/latestnews.xml",                            "weight": 1.2, "category": "market"},
    {"source": "Moneycontrol Mkt",    "url": "https://www.moneycontrol.com/rss/marketreports.xml",                         "weight": 1.3, "category": "market"},
    {"source": "Moneycontrol Biz",    "url": "https://www.moneycontrol.com/rss/business.xml",                              "weight": 1.2, "category": "market"},
    {"source": "Moneycontrol Econ",   "url": "https://www.moneycontrol.com/rss/economy.xml",                               "weight": 1.2, "category": "macro"},
    {"source": "ET Markets",          "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",       "weight": 1.2, "category": "market"},
    {"source": "ET Economy",          "url": "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms",  "weight": 1.2, "category": "macro"},
    {"source": "ET Industry",         "url": "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",        "weight": 1.1, "category": "sector"},
    {"source": "Mint Markets",        "url": "https://www.livemint.com/rss/markets",                                       "weight": 1.2, "category": "market"},
    {"source": "Mint Economy",        "url": "https://www.livemint.com/rss/economy",                                       "weight": 1.2, "category": "macro"},
    {"source": "Mint Companies",      "url": "https://www.livemint.com/rss/companies",                                     "weight": 1.1, "category": "market"},
    {"source": "BS Markets",          "url": "https://www.business-standard.com/rss/markets-106.rss",                      "weight": 1.2, "category": "market"},
    {"source": "BS Economy",          "url": "https://www.business-standard.com/rss/economy-102.rss",                      "weight": 1.2, "category": "macro"},
    {"source": "Hindu BusinessLine",  "url": "https://www.thehindubusinessline.com/markets/feeder/default.rss",            "weight": 1.1, "category": "market"},

    # Global macro (lower weight)
    {"source": "Reuters Business",    "url": "https://feeds.reuters.com/reuters/businessNews",                             "weight": 0.9, "category": "global"},
    {"source": "BBC Business",        "url": "http://feeds.bbci.co.uk/news/business/rss.xml",                              "weight": 0.8, "category": "global"},
    {"source": "FT Markets",          "url": "https://www.ft.com/markets?format=rss",                                      "weight": 0.9, "category": "global"},
    {"source": "CNBC World",          "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html",                      "weight": 0.8, "category": "global"},
]

PER_FEED_TIMEOUT = 8       # seconds
MAX_ITEMS_PER_FEED = 30
MAX_AGE_HOURS = 48         # only keep items published in the last 48h

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean(text: str) -> str:
    if not text:
        return ""
    text = unescape(text)
    text = _TAG_RE.sub(" ", text)
    text = _WS_RE.sub(" ", text)
    return text.strip()


def _parse_ts(entry: Any) -> float:
    for key in ("published_parsed", "updated_parsed"):
        val = getattr(entry, key, None) or (isinstance(entry, dict) and entry.get(key))
        if val:
            try:
                return time.mktime(val)
            except Exception:
                pass
    return time.time()


def _fetch_one(feed_meta: dict) -> list[dict]:
    if feedparser is None:
        log.warning("feedparser not installed — skipping %s", feed_meta["source"])
        return []
    url = feed_meta["url"]
    try:
        # feedparser honours socket timeout via urllib
        import socket
        socket.setdefaulttimeout(PER_FEED_TIMEOUT)
        parsed = feedparser.parse(url, request_headers={
            "User-Agent": "Mozilla/5.0 (compatible; StockSageBot/1.0)"
        })
        if parsed.bozo and not parsed.entries:
            log.warning("feed bozo %s: %s", feed_meta["source"], str(parsed.bozo_exception)[:80])
            return []
        items = []
        cutoff = time.time() - MAX_AGE_HOURS * 3600
        for e in parsed.entries[:MAX_ITEMS_PER_FEED]:
            title = _clean(getattr(e, "title", "") or "")
            summary = _clean(getattr(e, "summary", "") or getattr(e, "description", "") or "")
            link = getattr(e, "link", "") or ""
            ts = _parse_ts(e)
            if ts < cutoff:
                continue
            if not title:
                continue
            items.append({
                "title":        title,
                "summary":      summary[:600],
                "link":         link,
                "published_ts": ts,
                "published_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "source":       feed_meta["source"],
                "weight":       feed_meta["weight"],
                "category":     feed_meta["category"],
            })
        return items
    except Exception as e:
        log.warning("feed failed %s: %s", feed_meta["source"], str(e)[:120])
        return []


def fetch_all_news(max_workers: int = 10) -> list[dict]:
    """Fetches every registered feed in parallel. Never raises."""
    if feedparser is None:
        log.error("feedparser not available — news layer disabled")
        return []

    all_items: list[dict] = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_fetch_one, f): f for f in FEEDS}
            for fut in as_completed(futs, timeout=PER_FEED_TIMEOUT * 3):
                try:
                    all_items.extend(fut.result() or [])
                except Exception as e:
                    log.warning("future failed: %s", e)
    except Exception as e:
        log.error("fetch_all_news top-level failure: %s", e)

    log.info("fetched %d news items across %d feeds", len(all_items), len(FEEDS))
    return all_items
