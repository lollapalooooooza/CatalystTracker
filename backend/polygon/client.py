import time
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import requests

from backend.config import settings

BASE = "https://api.polygon.io"


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


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    per_page: int = 50,
    page_sleep: float = 1.2,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch all news for a ticker from Polygon, with pagination."""
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
        resp = http_get(next_url or url, params=None if next_url else params)
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
