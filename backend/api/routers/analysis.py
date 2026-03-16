from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

from backend.pipeline.layer2 import analyze_article, generate_story, analyze_range
from backend.pipeline.similarity import find_similar
from backend.database import get_conn

router = APIRouter()


class DeepAnalysisRequest(BaseModel):
    news_id: str
    symbol: str


class RangeAnalysisRequest(BaseModel):
    symbol: str
    start_date: str
    end_date: str
    question: Optional[str] = None


class SimilarRequest(BaseModel):
    news_id: str
    symbol: str
    top_k: Optional[int] = 20


class StoryRequest(BaseModel):
    symbol: str


@router.post("/deep")
def deep_analysis(req: DeepAnalysisRequest):
    """Trigger Layer 2 deep analysis for a single article."""
    return analyze_article(req.news_id, req.symbol.upper())


@router.post("/story")
def create_story(req: StoryRequest):
    """Generate an AI story for a stock's price movements."""
    symbol = req.symbol.upper()
    conn = get_conn()

    # Build CSV-like content from OHLC + news
    ohlc_rows = conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlc WHERE symbol = ? ORDER BY date ASC",
        (symbol,),
    ).fetchall()

    news_map: dict = {}
    news_rows = conn.execute(
        """SELECT na.trade_date, l1.chinese_summary
           FROM news_aligned na
           JOIN layer1_results l1 ON na.news_id = l1.news_id AND l1.symbol = na.symbol
           WHERE na.symbol = ? AND l1.relevance = 'relevant'
           ORDER BY na.trade_date ASC""",
        (symbol,),
    ).fetchall()

    for nr in news_rows:
        d = nr["trade_date"]
        if d not in news_map:
            news_map[d] = []
        news_map[d].append(nr["chinese_summary"] or "")

    conn.close()

    # Build content string
    lines = ["date,open,high,low,close,volume,news"]
    for row in ohlc_rows:
        news_text = "; ".join(news_map.get(row["date"], []))
        lines.append(
            f"{row['date']},{row['open']},{row['high']},{row['low']},{row['close']},{row['volume']},\"{news_text}\""
        )

    csv_content = "\n".join(lines)
    story_html = generate_story(symbol, csv_content)
    return {"story": story_html}


@router.post("/range")
def range_analysis(req: RangeAnalysisRequest):
    """Analyze price movement drivers for a date range (AI-powered, costly).
    Kept for future use — currently the frontend uses /range-local instead."""
    return analyze_range(req.symbol.upper(), req.start_date, req.end_date, question=req.question)


@router.post("/range-local")
def range_analysis_local(req: RangeAnalysisRequest):
    """Analyze price movement drivers using local data only (no AI API call)."""
    symbol = req.symbol.upper()
    conn = get_conn()

    ohlc_rows = conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlc "
        "WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date ASC",
        (symbol, req.start_date, req.end_date),
    ).fetchall()

    if not ohlc_rows:
        conn.close()
        return {"error": "No OHLC data for this range"}

    open_price = ohlc_rows[0]["open"]
    close_price = ohlc_rows[-1]["close"]
    high_price = max(r["high"] for r in ohlc_rows)
    low_price = min(r["low"] for r in ohlc_rows)
    price_change_pct = round((close_price - open_price) / open_price * 100, 2)
    total_volume = sum(r["volume"] for r in ohlc_rows)

    # News breakdown
    news_rows = conn.execute(
        """SELECT nr.title, l1.sentiment, l1.chinese_summary,
                  na.trade_date, na.ret_t0
           FROM news_aligned na
           JOIN layer1_results l1 ON na.news_id = l1.news_id AND l1.symbol = na.symbol
           JOIN news_raw nr ON na.news_id = nr.id
           WHERE na.symbol = ? AND na.trade_date >= ? AND na.trade_date <= ?
             AND l1.relevance IN ('high', 'medium', 'relevant')
           ORDER BY ABS(COALESCE(na.ret_t0, 0)) DESC
           LIMIT 50""",
        (symbol, req.start_date, req.end_date),
    ).fetchall()
    conn.close()

    pos = [r for r in news_rows if r["sentiment"] == "positive"]
    neg = [r for r in news_rows if r["sentiment"] == "negative"]
    news_count = len(news_rows)

    # Build local analysis from data
    direction = "up" if price_change_pct > 0 else "down" if price_change_pct < 0 else "flat"
    summary = (
        f"{symbol} moved {direction} {abs(price_change_pct):.2f}% "
        f"from {req.start_date} to {req.end_date}, "
        f"over {len(ohlc_rows)} trading days with "
        f"{news_count} related news ({len(pos)} positive / {len(neg)} negative)."
    )

    key_events = []
    for r in news_rows[:8]:
        title = r["title"][:80] if r["title"] else ""
        ret = f" (same-day {r['ret_t0']*100:+.1f}%)" if r["ret_t0"] else ""
        key_events.append(f"[{r['trade_date']}] {title}{ret}")

    bullish = [r["chinese_summary"] or r["title"][:60] for r in pos[:5] if r["chinese_summary"] or r["title"]]
    bearish = [r["chinese_summary"] or r["title"][:60] for r in neg[:5] if r["chinese_summary"] or r["title"]]

    # Trend
    if len(ohlc_rows) >= 3:
        mid = len(ohlc_rows) // 2
        first_half = (ohlc_rows[mid]["close"] - ohlc_rows[0]["open"]) / ohlc_rows[0]["open"] * 100
        second_half = (ohlc_rows[-1]["close"] - ohlc_rows[mid]["open"]) / ohlc_rows[mid]["open"] * 100
        trend = (
            f"First half {'up' if first_half > 0 else 'down'} {abs(first_half):.1f}%, "
            f"second half {'up' if second_half > 0 else 'down'} {abs(second_half):.1f}%. "
            f"High ${high_price:.2f}, low ${low_price:.2f}, "
            f"range {(high_price - low_price) / low_price * 100:.1f}%."
        )
    else:
        trend = f"Short range, change {price_change_pct:+.2f}%."

    return {
        "symbol": symbol,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "price_change_pct": price_change_pct,
        "open_price": open_price,
        "close_price": close_price,
        "high_price": high_price,
        "low_price": low_price,
        "news_count": news_count,
        "trading_days": len(ohlc_rows),
        "question": req.question,
        "analysis": {
            "summary": summary,
            "key_events": key_events,
            "bullish_factors": bullish,
            "bearish_factors": bearish,
            "trend_analysis": trend,
        },
    }


@router.post("/similar")
def similar_news(req: SimilarRequest):
    """Find historically similar news articles across all tickers."""
    return find_similar(req.news_id, req.symbol.upper(), req.top_k or 20)
