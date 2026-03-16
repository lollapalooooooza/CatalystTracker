import logging

from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

from backend.database import get_conn
from backend.pipeline.alignment import align_news_for_symbol
from backend.polygon.client import fetch_ohlc, fetch_news, search_tickers, get_ticker_details

router = APIRouter()


class AddTickerRequest(BaseModel):
    symbol: str
    name: Optional[str] = None


class PrefetchTickersRequest(BaseModel):
    symbols: list[str]


def _ensure_ohlc(symbol: str, start: Optional[str] = None, end: Optional[str] = None):
    symbol = symbol.upper()
    today = datetime.now(timezone.utc).date()
    fetch_start = start or (today - timedelta(days=2 * 366)).isoformat()
    fetch_end = end or today.isoformat()

    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM ohlc WHERE symbol = ?" +
        (" AND date >= ?" if start else "") +
        (" AND date <= ?" if end else "") +
        " ORDER BY date ASC",
        [symbol, *([start] if start else []), *([end] if end else [])],
    ).fetchall()
    conn.close()
    if rows:
        return [dict(r) for r in rows]

    ohlc_rows = fetch_ohlc(symbol, fetch_start, fetch_end)
    if not ohlc_rows:
        return []

    conn = get_conn()
    details = None
    try:
        details = get_ticker_details(symbol)
    except Exception:
        details = None
    conn.execute(
        "INSERT OR IGNORE INTO tickers (symbol, name, sector) VALUES (?, ?, ?)",
        (symbol, (details or {}).get('name') or symbol, (details or {}).get('sector') or ''),
    )
    for row in ohlc_rows:
        conn.execute(
            """INSERT OR IGNORE INTO ohlc
               (symbol, date, open, high, low, close, volume, vwap, transactions)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                symbol,
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["vwap"],
                row["transactions"],
            ),
        )
    conn.execute(
        "UPDATE tickers SET last_ohlc_fetch = ? WHERE symbol = ?",
        (fetch_end, symbol),
    )
    conn.commit()
    rows = conn.execute(
        "SELECT * FROM ohlc WHERE symbol = ?" +
        (" AND date >= ?" if start else "") +
        (" AND date <= ?" if end else "") +
        " ORDER BY date ASC",
        [symbol, *([start] if start else []), *([end] if end else [])],
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("")
def list_tickers():
    """List all tracked tickers."""
    conn = get_conn()
    rows = conn.execute("SELECT * FROM tickers ORDER BY symbol").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/search")
def search(q: str = Query(..., min_length=1)):
    """Fuzzy search tickers via Polygon."""
    # First check local DB
    conn = get_conn()
    local = conn.execute(
        "SELECT symbol, name, sector FROM tickers WHERE symbol LIKE ? OR name LIKE ? LIMIT 10",
        (f"%{q}%", f"%{q}%"),
    ).fetchall()
    conn.close()

    results = [dict(r) for r in local]

    # If few local results, also search Polygon
    if len(results) < 5:
        try:
            remote = search_tickers(q, limit=10)
            seen = {r["symbol"] for r in results}
            for r in remote:
                if r["symbol"] not in seen:
                    results.append(r)
        except Exception:
            logger.debug("Polygon search failed for query=%s", q)

    return results


@router.get("/{symbol}/status")
def get_ticker_status(symbol: str):
    """Return data readiness for a ticker."""
    symbol = symbol.upper()
    conn = get_conn()
    ohlc_count = conn.execute("SELECT COUNT(*) FROM ohlc WHERE symbol = ?", (symbol,)).fetchone()[0]
    particle_count = conn.execute("SELECT COUNT(*) FROM news_aligned WHERE symbol = ?", (symbol,)).fetchone()[0]
    news_count = conn.execute("SELECT COUNT(*) FROM news_ticker WHERE symbol = ?", (symbol,)).fetchone()[0]
    conn.close()

    from pathlib import Path
    models_dir = Path(__file__).resolve().parent.parent.parent / "ml" / "models"
    forecast_ready = (
        (models_dir / f"{symbol}_t1.joblib").exists() and (models_dir / f"{symbol}_t5.joblib").exists()
    ) or (
        (models_dir / "UNIFIED_t1.joblib").exists() and (models_dir / "UNIFIED_t5.joblib").exists()
    )

    return {
        "symbol": symbol,
        "has_ohlc": ohlc_count > 0,
        "has_news": news_count > 0,
        "has_aligned_news": particle_count > 0,
        "ohlc_count": ohlc_count,
        "news_count": news_count,
        "aligned_news_count": particle_count,
        "forecast_ready": forecast_ready,
        "ready": ohlc_count > 0,
    }


@router.get("/{symbol}/ohlc")
def get_ohlc(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """Get OHLC data for a symbol. Auto-backfill on first request if missing."""
    symbol = symbol.upper()
    conn = get_conn()

    query = "SELECT * FROM ohlc WHERE symbol = ?"
    params: list = [symbol]

    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)

    query += " ORDER BY date ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        today = datetime.now(timezone.utc).date()
        fetch_start = start or (today - timedelta(days=2 * 366)).isoformat()
        fetch_end = end or today.isoformat()
        try:
            ohlc_rows = fetch_ohlc(symbol, fetch_start, fetch_end)
            if ohlc_rows:
                conn = get_conn()
                conn.execute(
                    "INSERT OR IGNORE INTO tickers (symbol, name) VALUES (?, ?)",
                    (symbol, symbol),
                )
                for row in ohlc_rows:
                    conn.execute(
                        """INSERT OR IGNORE INTO ohlc
                           (symbol, date, open, high, low, close, volume, vwap, transactions)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            symbol,
                            row["date"],
                            row["open"],
                            row["high"],
                            row["low"],
                            row["close"],
                            row["volume"],
                            row["vwap"],
                            row["transactions"],
                        ),
                    )
                conn.execute(
                    "UPDATE tickers SET last_ohlc_fetch = ? WHERE symbol = ?",
                    (fetch_end, symbol),
                )
                conn.commit()
                rows = conn.execute(query, params).fetchall()
                conn.close()
        except Exception:
            logger.exception("Auto-backfill OHLC failed for %s", symbol)

    if not rows:
        raise HTTPException(status_code=404, detail=f"No OHLC data for {symbol}")

    return [dict(r) for r in rows]


@router.post("")
def add_ticker(req: AddTickerRequest, background_tasks: BackgroundTasks):
    """Add a new ticker and trigger background data fetch."""
    symbol = req.symbol.upper()
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO tickers (symbol, name) VALUES (?, ?)",
        (symbol, req.name or symbol),
    )
    conn.commit()
    conn.close()

    background_tasks.add_task(_fetch_ticker_data, symbol)
    return {"symbol": symbol, "status": "added", "message": "Data fetch started in background"}


def _fetch_ticker_data(symbol: str):
    """Background task to fetch OHLC and news for a ticker."""
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=2 * 366)).isoformat()
    end = today.isoformat()

    try:
        # Fetch OHLC
        ohlc_rows = fetch_ohlc(symbol, start, end)
        conn = get_conn()
        for row in ohlc_rows:
            conn.execute(
                """INSERT OR IGNORE INTO ohlc
                   (symbol, date, open, high, low, close, volume, vwap, transactions)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol,
                    row["date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["vwap"],
                    row["transactions"],
                ),
            )
        if ohlc_rows:
            conn.execute(
                "UPDATE tickers SET last_ohlc_fetch = ? WHERE symbol = ?",
                (end, symbol),
            )
        conn.commit()

        # Fetch news
        import json

        details = get_ticker_details(symbol) or {}
        articles = fetch_news(symbol, start, end, per_page=100, max_pages=8, company_name=details.get('name'))
        for art in articles:
            news_id = art.get("id")
            if not news_id:
                continue
            tickers = art.get("tickers") or []
            conn.execute(
                """INSERT OR IGNORE INTO news_raw
                   (id, title, description, publisher, author,
                    published_utc, article_url, amp_url, tickers_json, insights_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    news_id,
                    art.get("title"),
                    art.get("description"),
                    art.get("publisher"),
                    art.get("author"),
                    art.get("published_utc"),
                    art.get("article_url"),
                    art.get("amp_url"),
                    json.dumps(tickers),
                    json.dumps(art.get("insights")) if art.get("insights") else None,
                ),
            )
            for tk in tickers:
                conn.execute(
                    "INSERT OR IGNORE INTO news_ticker (news_id, symbol) VALUES (?, ?)",
                    (news_id, tk),
                )

        conn.execute(
            "UPDATE tickers SET last_news_fetch = ? WHERE symbol = ?",
            (end, symbol),
        )
        conn.commit()
        conn.close()

        # Align fetched news to trading days so chart particles / news panels work.
        align_news_for_symbol(symbol)
    except Exception:
        logger.exception("Error fetching data for %s", symbol)
