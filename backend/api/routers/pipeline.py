import logging

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

from backend.database import get_conn
from backend.polygon.client import fetch_ohlc, fetch_news
from backend.pipeline.layer0 import run_layer0
from backend.pipeline.layer1 import get_pending_articles, run_layer1, check_batch_status, collect_batch_results
from backend.pipeline.alignment import align_news_for_symbol

import json

router = APIRouter()


class FetchRequest(BaseModel):
    symbol: str
    start: Optional[str] = None
    end: Optional[str] = None


class ProcessRequest(BaseModel):
    symbol: str
    batch_size: int = 1000


@router.post("/fetch")
def trigger_fetch(req: FetchRequest, background_tasks: BackgroundTasks):
    """Trigger Polygon data fetch for a symbol."""
    symbol = req.symbol.upper()
    today = datetime.now(timezone.utc).date()
    start = req.start or (today - timedelta(days=2 * 366)).isoformat()
    end = req.end or today.isoformat()

    background_tasks.add_task(_do_fetch, symbol, start, end)
    return {"symbol": symbol, "status": "fetch_started", "start": start, "end": end}


def _do_fetch(symbol: str, start: str, end: str):
    """Background fetch of OHLC + news data."""
    try:
        # OHLC
        ohlc_rows = fetch_ohlc(symbol, start, end)
        conn = get_conn()
        for row in ohlc_rows:
            conn.execute(
                """INSERT OR IGNORE INTO ohlc
                   (symbol, date, open, high, low, close, volume, vwap, transactions)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (symbol, row["date"], row["open"], row["high"], row["low"],
                 row["close"], row["volume"], row["vwap"], row["transactions"]),
            )
        conn.execute(
            "UPDATE tickers SET last_ohlc_fetch = ? WHERE symbol = ?",
            (end, symbol),
        )
        conn.commit()

        # News
        articles = fetch_news(symbol, start, end)
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
                (news_id, art.get("title"), art.get("description"),
                 art.get("publisher"), art.get("author"), art.get("published_utc"),
                 art.get("article_url"), art.get("amp_url"),
                 json.dumps(tickers),
                 json.dumps(art.get("insights")) if art.get("insights") else None),
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

        # Run alignment
        align_news_for_symbol(symbol)
    except Exception:
        logger.exception("Fetch error for %s", symbol)


@router.post("/process")
def trigger_process(req: ProcessRequest):
    """Run Layer 0 filter, then submit Layer 1 batch for remaining articles."""
    symbol = req.symbol.upper()

    # Step 1: Alignment
    align_result = align_news_for_symbol(symbol)

    # Step 2: Layer 0
    l0_stats = run_layer0(symbol)

    # Step 3: Run Layer 1 (50 articles per API call)
    l1_stats = run_layer1(symbol, max_articles=req.batch_size)

    return {
        "symbol": symbol,
        "alignment": align_result,
        "layer0": l0_stats,
        "layer1": l1_stats,
    }


@router.get("/batch/{batch_id}")
def get_batch_status(batch_id: str):
    """Check status of a batch job."""
    status = check_batch_status(batch_id)

    # If ended, collect results
    if status["status"] == "ended":
        collect_stats = collect_batch_results(batch_id)
        status["collect_stats"] = collect_stats

    return status
