"""Multi-source news & OHLC fetching engine.

Primary: Polygon.io
Secondary: Finnhub (free tier, optional key)
Tertiary: Google News RSS (no key needed, broad coverage)

All news sources are fetched concurrently via ThreadPoolExecutor
for fast response times, especially for less famous tickers.
"""

import time
import json
import hashlib
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET
from email.utils import parsedate_to_datetime
import requests

from backend.config import settings

logger = logging.getLogger(__name__)

BASE = "https://api.polygon.io"

# ─── HTTP helpers ────────────────────────────────────────────────────────────

def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {settings.polygon_api_key}"}


def http_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 8,
    backoff: float = 2.0,
) -> requests.Response:
    """HTTP GET with exponential backoff and 429 handling."""
    for i in range(max_retries):
        try:
            resp = requests.get(
                url, params=params or {}, headers=_headers(), timeout=30
            )
        except requests.RequestException:
            time.sleep((backoff**i) + 0.5)
            if i == max_retries - 1:
                raise
            continue

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            wait = float(ra) if (ra and ra.isdigit()) else min((backoff**i) + 1.0, 60.0)
            time.sleep(wait)
            if i == max_retries - 1:
                resp.raise_for_status()
            continue

        if 500 <= resp.status_code < 600:
            time.sleep(min((backoff**i) + 1.0, 60.0))
            if i == max_retries - 1:
                resp.raise_for_status()
            continue

        resp.raise_for_status()
        return resp
    raise RuntimeError("Unreachable")


# ─── OHLC fetching ──────────────────────────────────────────────────────────

def _fetch_ohlc_yahoo(ticker: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Fallback OHLC fetch via Yahoo Finance chart API."""
    start_ts = int(datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp()) + 86400
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "period1": start_ts,
        "period2": end_ts,
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://finance.yahoo.com/",
    }
    payload = None
    for i in range(4):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                time.sleep(min(2 ** i, 8))
                continue
            resp.raise_for_status()
            payload = resp.json()
            break
        except requests.RequestException:
            time.sleep(min(2 ** i, 8))
    if payload is None:
        return []
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    if not result:
        return []

    timestamps = result.get("timestamp") or []
    indicators = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
    opens = indicators.get("open") or []
    highs = indicators.get("high") or []
    lows = indicators.get("low") or []
    closes = indicators.get("close") or []
    volumes = indicators.get("volume") or []

    rows: List[Dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        o = opens[i] if i < len(opens) else None
        h = highs[i] if i < len(highs) else None
        l = lows[i] if i < len(lows) else None
        c = closes[i] if i < len(closes) else None
        v = volumes[i] if i < len(volumes) else None
        if o is None or h is None or l is None or c is None:
            continue
        d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
        rows.append({"date": d, "open": o, "high": h, "low": l, "close": c, "volume": v, "vwap": None, "transactions": None})
    return rows


def _fetch_ohlc_stooq(ticker: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Second fallback via Stooq daily CSV."""
    url = "https://stooq.com/q/d/l/"
    params = {"s": f"{ticker.lower()}.us", "i": "d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    text = resp.text.strip()
    if not text or text.lower().startswith("no data"):
        return []

    rows: List[Dict[str, Any]] = []
    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()
    lines = text.splitlines()
    for line in lines[1:]:
        parts = line.split(',')
        if len(parts) < 6:
            continue
        ds, o, h, l, c, v = parts[:6]
        try:
            d = datetime.fromisoformat(ds).date()
            if d < start_d or d > end_d:
                continue
            rows.append({
                "date": d.isoformat(),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v) if v not in ('', '0') else 0.0,
                "vwap": None,
                "transactions": None,
            })
        except Exception:
            continue
    return rows


def fetch_ohlc(ticker: str, start: str, end: str) -> List[Dict[str, Any]]:
    """Fetch daily OHLC data from Polygon, with Yahoo fallback if needed."""
    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    try:
        resp = http_get(url, params=params)
        results = resp.json().get("results") or []
    except Exception:
        results = []

    rows = []
    for r in results:
        d = datetime.fromtimestamp(int(r["t"]) / 1000, tz=timezone.utc).date().isoformat()
        rows.append(
            {
                "date": d,
                "open": r.get("o"),
                "high": r.get("h"),
                "low": r.get("l"),
                "close": r.get("c"),
                "volume": r.get("v"),
                "vwap": r.get("vw"),
                "transactions": r.get("n"),
            }
        )

    if rows:
        return rows

    rows = _fetch_ohlc_yahoo(ticker, start, end)
    if rows:
        return rows

    return _fetch_ohlc_stooq(ticker, start, end)


# ─── NEWS SOURCE 1: Polygon.io ──────────────────────────────────────────────

def _fetch_polygon_news(
    ticker: str,
    start: str,
    end: str,
    per_page: int = 50,
    page_sleep: float = 1.2,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch news from Polygon.io (primary source)."""
    if not settings.polygon_api_key:
        return []
    url = f"{BASE}/v2/reference/news"
    params = {
        "ticker": ticker,
        "published_utc.gte": start,
        "published_utc.lte": end,
        "limit": per_page,
        "order": "asc",
    }
    all_articles: List[Dict[str, Any]] = []
    seen_ids: set = set()
    pages = 0
    next_url: Optional[str] = None

    while True:
        try:
            resp = http_get(next_url or url, params=None if next_url else params)
        except Exception as e:
            logger.warning("Polygon news fetch failed for %s: %s", ticker, e)
            break
        data = resp.json()
        for r in data.get("results", []) or []:
            rid = r.get("id")
            if rid and rid in seen_ids:
                continue
            article = {
                "id": rid,
                "publisher": (r.get("publisher") or {}).get("name"),
                "title": r.get("title"),
                "author": r.get("author"),
                "published_utc": r.get("published_utc"),
                "amp_url": r.get("amp_url"),
                "article_url": r.get("article_url"),
                "tickers": r.get("tickers"),
                "description": r.get("description"),
                "insights": r.get("insights"),
                "image_url": r.get("image_url"),
                "source": "polygon",
            }
            all_articles.append(article)
            if rid:
                seen_ids.add(rid)

        next_url = data.get("next_url")
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        if not next_url:
            break
        time.sleep(page_sleep)

    return all_articles


# ─── NEWS SOURCE 2: Finnhub ─────────────────────────────────────────────────

def _fetch_finnhub_news(
    ticker: str,
    start: str,
    end: str,
) -> List[Dict[str, Any]]:
    """Fetch company news from Finnhub.io (free tier: 60 calls/min).

    Returns articles in the same normalized format as other sources.
    Gracefully returns [] if no API key configured.
    """
    api_key = settings.finnhub_api_key
    if not api_key:
        return []

    url = "https://finnhub.io/api/v1/company-news"
    params = {
        "symbol": ticker.upper(),
        "from": start,
        "to": end,
        "token": api_key,
    }

    articles: List[Dict[str, Any]] = []
    try:
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code == 429:
            logger.warning("Finnhub rate limit hit for %s", ticker)
            return []
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
    except Exception as e:
        logger.warning("Finnhub fetch failed for %s: %s", ticker, e)
        return []

    for item in data:
        headline = item.get("headline") or ""
        summary = item.get("summary") or ""
        url_val = item.get("url") or ""
        source = item.get("source") or "Finnhub"
        ts = item.get("datetime")
        image = item.get("image") or None

        if not headline:
            continue

        try:
            published = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None
        except Exception:
            published = None

        aid = "fh_" + hashlib.sha1(f"{headline}|{url_val}".encode("utf-8")).hexdigest()[:24]
        articles.append({
            "id": aid,
            "publisher": source,
            "title": headline,
            "author": None,
            "published_utc": published,
            "amp_url": None,
            "article_url": url_val,
            "tickers": [ticker.upper()],
            "description": summary,
            "insights": None,
            "image_url": image,
            "source": "finnhub",
        })

    return articles


# ─── NEWS SOURCE 3: Google News RSS ─────────────────────────────────────────

def _fetch_google_news_rss(query: str, ticker: str, max_items: int = 50) -> List[Dict[str, Any]]:
    """Fetch news from Google News RSS feed."""
    url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception:
        return []

    articles: List[Dict[str, Any]] = []
    for item in root.findall('.//item')[:max_items]:
        title = item.findtext('title') or ''
        link = item.findtext('link') or ''
        desc = item.findtext('description') or ''
        pub = item.findtext('pubDate') or ''
        source = item.findtext('source') or 'Google News'
        try:
            published = parsedate_to_datetime(pub).astimezone(timezone.utc).isoformat()
        except Exception:
            published = datetime.now(timezone.utc).isoformat()
        aid = 'rss_' + hashlib.sha1(f'{title}|{link}'.encode('utf-8')).hexdigest()[:24]
        articles.append({
            'id': aid,
            'publisher': source,
            'title': title,
            'author': None,
            'published_utc': published,
            'amp_url': None,
            'article_url': link,
            'tickers': [ticker],
            'description': desc,
            'insights': None,
            'image_url': None,
            'source': 'google_rss',
        })
    return articles


def _build_google_queries(ticker: str, company_name: Optional[str] = None) -> List[str]:
    """Build a broad set of Google News search queries for better coverage.

    Uses multiple query strategies to maximize coverage for less famous tickers:
    - Exact ticker symbol + financial sites
    - Ticker + generic stock terms
    - Company name variations (if available)
    - Ticker-only search (catches more general mentions)
    """
    queries = [
        # Strategy 1: Targeted financial sites
        f'"{ticker}" stock site:finance.yahoo.com OR site:seekingalpha.com OR site:reuters.com',
        # Strategy 2: Broader stock news
        f'"{ticker}" stock market news',
        # Strategy 3: Ticker-only for broader coverage
        f'"{ticker}" stock price',
        # Strategy 4: Business news sites
        f'"{ticker}" site:cnbc.com OR site:bloomberg.com OR site:marketwatch.com OR site:benzinga.com',
    ]

    if company_name:
        # Strip common suffixes for cleaner searches
        clean_name = re.sub(
            r'\s*(Inc\.?|Corp\.?|Ltd\.?|LLC|PLC|Group|Holdings?|Co\.?|Corporation|Limited|Technologies|Technology)\s*$',
            '',
            company_name,
            flags=re.IGNORECASE,
        ).strip()
        if clean_name and clean_name.upper() != ticker.upper():
            queries.extend([
                # Strategy 5: Company name + financial sites
                f'"{clean_name}" stock site:finance.yahoo.com OR site:seekingalpha.com OR site:reuters.com',
                # Strategy 6: Company name + ticker combo
                f'"{clean_name}" "{ticker}" stock',
                # Strategy 7: Company name alone (for less famous tickers)
                f'"{clean_name}" stock market news',
                # Strategy 8: Company name + business news
                f'"{clean_name}" site:cnbc.com OR site:bloomberg.com OR site:benzinga.com',
            ])

    return queries


def _fetch_google_news_all(
    ticker: str,
    company_name: Optional[str],
    start: str,
    end: str,
) -> List[Dict[str, Any]]:
    """Fetch from Google News RSS using multiple query strategies concurrently."""
    queries = _build_google_queries(ticker, company_name)
    start_dt = datetime.fromisoformat(start).date()
    end_dt = datetime.fromisoformat(end).date()

    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()

    # Fetch all queries concurrently (they're independent HTTP calls)
    with ThreadPoolExecutor(max_workers=min(len(queries), 6)) as pool:
        futures = {
            pool.submit(_fetch_google_news_rss, q, ticker, 50): q
            for q in queries
        }
        for future in as_completed(futures):
            try:
                results = future.result()
            except Exception:
                continue
            for art in results:
                aid = art.get("id")
                if not aid or aid in seen:
                    continue
                # Date filter
                try:
                    d = datetime.fromisoformat(
                        art["published_utc"].replace("Z", "+00:00")
                    ).date()
                    if d < start_dt or d > end_dt:
                        continue
                except Exception:
                    pass  # Keep articles with unparseable dates
                seen.add(aid)
                merged.append(art)

    return merged


# ─── Cross-source deduplication ──────────────────────────────────────────────

def _normalize_title(title: str) -> str:
    """Normalize a title for comparison: lowercase, strip punctuation/whitespace."""
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", "", t)  # Remove punctuation
    t = re.sub(r"\s+", " ", t)     # Collapse whitespace
    return t


def _deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate articles across sources using title similarity.

    Uses SequenceMatcher for fuzzy matching — titles with >80% similarity
    are considered duplicates. Keeps the version with the longer description.
    """
    if not articles:
        return []

    # First pass: exact ID dedup
    seen_ids: set[str] = set()
    unique_by_id: List[Dict[str, Any]] = []
    for art in articles:
        aid = art.get("id") or ""
        if aid and aid in seen_ids:
            continue
        if aid:
            seen_ids.add(aid)
        unique_by_id.append(art)

    # Second pass: title similarity dedup
    result: List[Dict[str, Any]] = []
    normalized_titles: List[str] = []

    for art in unique_by_id:
        norm = _normalize_title(art.get("title") or "")
        if not norm or len(norm) < 10:
            result.append(art)
            normalized_titles.append("")
            continue

        is_dup = False
        for i, existing_norm in enumerate(normalized_titles):
            if not existing_norm:
                continue
            # Quick length check first (avoid expensive SequenceMatcher for obviously different titles)
            len_ratio = len(norm) / len(existing_norm) if existing_norm else 0
            if len_ratio < 0.5 or len_ratio > 2.0:
                continue
            ratio = SequenceMatcher(None, norm, existing_norm).ratio()
            if ratio > 0.80:
                # Keep the one with longer description
                existing_desc = len((result[i].get("description") or ""))
                new_desc = len((art.get("description") or ""))
                if new_desc > existing_desc:
                    result[i] = art
                    normalized_titles[i] = norm
                is_dup = True
                break

        if not is_dup:
            result.append(art)
            normalized_titles.append(norm)

    return result


# ─── Main fetch_news: concurrent multi-source ───────────────────────────────

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    per_page: int = 50,
    page_sleep: float = 1.2,
    max_pages: Optional[int] = None,
    company_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch news from ALL available sources concurrently, then deduplicate.

    Sources:
      1. Polygon.io (primary, if API key present)
      2. Finnhub (secondary, if API key present)
      3. Google News RSS (always available, no key needed)

    All sources are queried in parallel via ThreadPoolExecutor.
    Results are merged and deduplicated by title similarity.
    """
    all_articles: List[Dict[str, Any]] = []

    # Launch all sources concurrently
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {}

        # Source 1: Polygon
        if settings.polygon_api_key:
            futures[pool.submit(
                _fetch_polygon_news, ticker, start, end, per_page, page_sleep, max_pages
            )] = "polygon"

        # Source 2: Finnhub
        if settings.finnhub_api_key:
            futures[pool.submit(
                _fetch_finnhub_news, ticker, start, end
            )] = "finnhub"

        # Source 3: Google News RSS (always available)
        futures[pool.submit(
            _fetch_google_news_all, ticker, company_name, start, end
        )] = "google_rss"

        for future in as_completed(futures):
            source_name = futures[future]
            try:
                result = future.result()
                logger.info(
                    "Source %s returned %d articles for %s",
                    source_name, len(result), ticker,
                )
                all_articles.extend(result)
            except Exception as e:
                logger.warning(
                    "Source %s failed for %s: %s", source_name, ticker, e
                )

    # Deduplicate across sources
    deduped = _deduplicate_articles(all_articles)
    logger.info(
        "fetch_news(%s): %d raw → %d after dedup",
        ticker, len(all_articles), len(deduped),
    )
    return deduped


# ─── Legacy compatibility aliases ────────────────────────────────────────────

def _fetch_news_fallbacks(ticker: str, company_name: Optional[str], start: str, end: str) -> List[Dict[str, Any]]:
    """Legacy fallback function — now delegates to _fetch_google_news_all."""
    return _fetch_google_news_all(ticker, company_name, start, end)


# ─── Ticker lookup ───────────────────────────────────────────────────────────

def get_ticker_details(symbol: str) -> Optional[Dict[str, str]]:
    """Fetch a single exact ticker from Polygon."""
    url = f"{BASE}/v3/reference/tickers/{symbol.upper()}"
    try:
        resp = http_get(url, params={})
    except Exception:
        return None
    result = (resp.json() or {}).get("results") or {}
    ticker = result.get("ticker") or ""
    if not ticker:
        return None
    return {
        "symbol": ticker,
        "name": result.get("name", ""),
        "sector": result.get("sic_description", ""),
    }


def search_tickers(query: str, limit: int = 20) -> List[Dict[str, str]]:
    """Search tickers from Polygon reference endpoint, prioritizing exact symbol matches."""
    q = query.strip().upper()
    results: List[Dict[str, str]] = []
    seen: set[str] = set()

    # Exact ticker lookup first so short symbols like BE surface correctly.
    exact = get_ticker_details(q)
    if exact and exact["symbol"] not in seen:
        results.append(exact)
        seen.add(exact["symbol"])

    url = f"{BASE}/v3/reference/tickers"
    params = {"search": query, "active": "true", "limit": max(limit, 20), "market": "stocks"}
    resp = http_get(url, params=params)
    raw = resp.json().get("results") or []

    mapped = [
        {
            "symbol": r.get("ticker", ""),
            "name": r.get("name", ""),
            "sector": r.get("sic_description", ""),
        }
        for r in raw
        if r.get("ticker")
    ]

    def rank(item: Dict[str, str]) -> tuple:
        sym = item["symbol"].upper()
        name = (item.get("name") or "").upper()
        return (
            0 if sym == q else 1,
            0 if sym.startswith(q) else 1,
            0 if name.startswith(q) else 1,
            len(sym),
            sym,
        )

    for item in sorted(mapped, key=rank):
        sym = item["symbol"]
        if sym not in seen:
            results.append(item)
            seen.add(sym)
        if len(results) >= limit:
            break

    return results
