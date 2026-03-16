"""Find historically similar trading days using ML feature vectors."""

import numpy as np
import pandas as pd
from backend.ml.features import build_features, FEATURE_COLS
from backend.database import get_conn


def find_similar_days(symbol: str, date: str, top_k: int = 10) -> dict:
    """Find days with the most similar feature vectors to the target date."""
    df = build_features(symbol)
    if df.empty:
        return {"error": f"No feature data for {symbol}"}

    df["date_str"] = df["trade_date"].dt.strftime("%Y-%m-%d")

    target_mask = df["date_str"] == date
    if not target_mask.any():
        # Find nearest date
        df["_dist"] = (df["trade_date"] - pd.Timestamp(date)).abs()
        nearest_idx = df["_dist"].idxmin()
        target_mask = df.index == nearest_idx
        df.drop(columns=["_dist"], inplace=True)

    target_idx = df[target_mask].index[0]
    target_row = df.loc[target_idx]

    X = df[FEATURE_COLS].values.astype(np.float64)
    np.nan_to_num(X, copy=False)

    # Normalize features (z-score)
    mean = np.mean(X, axis=0)
    std = np.std(X, axis=0)
    std[std < 1e-10] = 1.0
    X_norm = (X - mean) / std

    target_vec = X_norm[target_idx]

    # Cosine similarity
    norms = np.linalg.norm(X_norm, axis=1)
    norms[norms < 1e-10] = 1.0
    target_norm = np.linalg.norm(target_vec)
    if target_norm < 1e-10:
        target_norm = 1.0

    similarities = X_norm @ target_vec / (norms * target_norm)

    # Exclude the target itself and nearby days (within 5 days)
    for i in range(max(0, target_idx - 5), min(len(df), target_idx + 6)):
        similarities[i] = -999

    # Top K
    top_indices = np.argsort(similarities)[::-1][:top_k]

    # Get OHLC returns for context
    conn = get_conn()
    ohlc_rows = conn.execute(
        "SELECT date, close FROM ohlc WHERE symbol = ? ORDER BY date",
        (symbol,),
    ).fetchall()

    # Fetch news titles grouped by trade_date for similar days
    news_rows = conn.execute(
        """SELECT na.trade_date, nr.title, l1.sentiment
           FROM news_aligned na
           JOIN news_raw nr ON na.news_id = nr.id
           LEFT JOIN layer1_results l1 ON na.news_id = l1.news_id AND l1.symbol = ?
           WHERE na.symbol = ?
           ORDER BY na.trade_date, na.published_utc DESC""",
        (symbol, symbol),
    ).fetchall()
    conn.close()

    # Build lookup: date -> list of {title, sentiment}
    news_by_date: dict[str, list[dict]] = {}
    for r in news_rows:
        d = r["trade_date"]
        if d not in news_by_date:
            news_by_date[d] = []
        news_by_date[d].append({
            "title": (r["title"] or "")[:100],
            "sentiment": r["sentiment"],
        })

    close_by_date = {r["date"]: r["close"] for r in ohlc_rows}
    ohlc_dates = [r["date"] for r in ohlc_rows]

    def get_forward_return(d: str, days: int) -> float | None:
        if d not in ohlc_dates:
            return None
        idx = ohlc_dates.index(d)
        if idx + days >= len(ohlc_dates):
            return None
        return (close_by_date[ohlc_dates[idx + days]] / close_by_date[d] - 1) * 100

    # Build target day info
    target_date_str = df.loc[target_idx, "date_str"]
    target_features = {col: round(float(target_row[col]), 4) for col in
                       ["sentiment_score", "n_articles", "positive_ratio", "negative_ratio",
                        "ret_1d", "volatility_5d", "rsi_14"]}
    target_features["ret_t1_actual"] = get_forward_return(target_date_str, 1)
    target_features["ret_t5_actual"] = get_forward_return(target_date_str, 5)
    target_features["news"] = news_by_date.get(target_date_str, [])[:5]

    # Build similar days
    similar = []
    up_count_t1 = 0
    up_count_t5 = 0
    valid_t1 = 0
    valid_t5 = 0

    for idx in top_indices:
        row = df.iloc[idx]
        d = row["date_str"]
        sim_score = float(similarities[idx])

        ret_t1 = get_forward_return(d, 1)
        ret_t5 = get_forward_return(d, 5)

        if ret_t1 is not None:
            valid_t1 += 1
            if ret_t1 > 0:
                up_count_t1 += 1
        if ret_t5 is not None:
            valid_t5 += 1
            if ret_t5 > 0:
                up_count_t5 += 1

        similar.append({
            "date": d,
            "similarity": round(sim_score, 4),
            "sentiment_score": round(float(row["sentiment_score"]), 4),
            "n_articles": int(row["n_articles"]),
            "ret_1d": round(float(row["ret_1d"]), 4) if pd.notna(row["ret_1d"]) else None,
            "rsi_14": round(float(row["rsi_14"]), 1),
            "ret_t1_after": round(ret_t1, 2) if ret_t1 is not None else None,
            "ret_t5_after": round(ret_t5, 2) if ret_t5 is not None else None,
            "news": news_by_date.get(d, [])[:5],  # top 5 news for this day
        })

    # Aggregate stats
    avg_ret_t1 = np.mean([s["ret_t1_after"] for s in similar if s["ret_t1_after"] is not None]) if valid_t1 else None
    avg_ret_t5 = np.mean([s["ret_t5_after"] for s in similar if s["ret_t5_after"] is not None]) if valid_t5 else None

    return {
        "symbol": symbol,
        "target_date": target_date_str,
        "target_features": target_features,
        "similar_days": similar,
        "stats": {
            "up_ratio_t1": round(up_count_t1 / valid_t1, 2) if valid_t1 else None,
            "up_ratio_t5": round(up_count_t5 / valid_t5, 2) if valid_t5 else None,
            "avg_ret_t1": round(float(avg_ret_t1), 2) if avg_ret_t1 is not None else None,
            "avg_ret_t5": round(float(avg_ret_t5), 2) if avg_ret_t5 is not None else None,
            "count": len(similar),
        },
    }
