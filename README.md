# CatalystTracker 🔍📈

A news-driven stock research backend + frontend for understanding **why** a stock moved, not just **that** it moved.

This repo powers an event-focused workflow built around:
- candlestick charts with news particles
- range-based price move analysis
- news categorization and filtering
- similar-day lookup
- forecast generation with per-ticker or unified ML models

---

## ✨ What it does

### 📊 Chart + news timeline
- overlay news events directly on OHLC price charts
- inspect articles for a specific day
- click/lock a news event and follow its context

### 🧠 Explain moves
- select a date range and ask what drove the move
- summarize bullish vs bearish factors
- inspect key events in a rally or selloff

### 🗂️ Organize catalysts
- classify news into categories like:
  - market
  - policy
  - earnings
  - product / tech
  - competition
  - management

### 🔁 Similar-day analysis
- find historical periods with similar news/feature patterns
- compare what happened after those periods

### 🔮 Forecasts
- support per-ticker models when available
- automatically fall back to unified models when ticker-specific models are missing
- expose short-term and mid-term directional signals via API

---

## 🏗️ Architecture

```text
frontend (React + Vite + D3)
        ↓
FastAPI backend
        ↓
SQLite data store (pokieticker.db)
        ↓
Polygon data + aligned news + ML inference
```

Core pieces:
- `backend/api/` — FastAPI routes
- `backend/polygon/` — Polygon fetch/search client
- `backend/pipeline/` — filtering, alignment, AI/news processing
- `backend/ml/` — feature engineering, training, inference
- `frontend/` — standalone UI version of CatalystTracker

---

## 🚀 Main API capabilities

### Market + search
- `GET /api/stocks`
- `GET /api/stocks/search?q=NVDA`
- `GET /api/stocks/{symbol}/ohlc`
- `GET /api/stocks/{symbol}/status`
- `POST /api/stocks`

### News + catalyst views
- `GET /api/news/{symbol}`
- `GET /api/news/{symbol}/range`
- `GET /api/news/{symbol}/particles`
- `GET /api/news/{symbol}/categories`
- `GET /api/news/{symbol}/timeline`

### Analysis + prediction
- `POST /api/analysis/range`
- `GET /api/predict/{symbol}/forecast?window=7`
- `GET /api/predict/{symbol}/similar-days?date=YYYY-MM-DD`

### Health
- `GET /api/health`

---

## 🧪 Local development

The repo includes a compressed database archive:
- `pokieticker.db.gz`

To unpack locally:

```bash
gzip -dc pokieticker.db.gz > pokieticker.db
```

### Backend

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn backend.api.main:app --reload
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

---

## 🤖 Forecast models

Forecasts depend on model artifacts in:

```text
backend/ml/models/
```

Supported patterns:
- per-ticker models, e.g. `NVDA_t1.joblib`, `NVDA_t5.joblib`
- unified fallback models, e.g. `UNIFIED_t1.joblib`, `UNIFIED_t5.joblib`

### Train a single ticker

```bash
python -m backend.ml.train --symbol NVDA
```

### Train unified models

```bash
python - <<'PY'
from backend.ml.model import train_unified
print(train_unified('t1'))
print(train_unified('t5'))
PY
```

> Unified models are useful when you want broad coverage before training every ticker individually.

---

## 🌐 Deployment

This repo is set up for Render deployment.

Included files:
- `render.yaml`
- `Procfile`
- `start.sh`
- `DEPLOY.md`

### Important deployment behavior
- commit `pokieticker.db.gz`
- do **not** commit `pokieticker.db`
- `start.sh` will unpack the database on boot if needed

### Required / optional env vars
- `DATABASE_PATH`
- `POLYGON_API_KEY` *(recommended for ticker search + new ticker ingestion)*
- `ANTHROPIC_API_KEY` *(only needed for AI processing flows)*

Health check:

```text
/api/health
```

---

## 🔎 Current search behavior

Ticker search now:
- checks the local DB first
- tries exact Polygon ticker lookup
- falls back to broader Polygon search
- prioritizes exact/starts-with symbol matches

This is especially important for short symbols like:
- `BE`
- `C`
- `T`

---

## 📁 Project structure

```text
backend/
  api/
  ml/
  pipeline/
  polygon/
frontend/
render.yaml
start.sh
requirements.txt
pokieticker.db.gz
```

---

## ⚠️ Notes

- This is a research tool, not financial advice.
- Forecast quality depends on data coverage and model availability.
- New tickers may take time to fetch, align, and become fully visible in downstream views.

---

## 📜 License

MIT
