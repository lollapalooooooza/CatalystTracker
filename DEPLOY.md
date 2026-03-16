# CatalystTracker Backend Deployment

## Recommended: Render

This project exposes a FastAPI backend at `backend.api.main:app`.

### 1. Push this folder to GitHub
Create a dedicated repo for `CatalystTracker` and push the contents of this folder.

### 2. Include database
This backend expects a SQLite database file at project root:

- `pokieticker.db`

If you only have `pokieticker.db.gz`, unpack it first:

```bash
gzip -dc pokieticker.db.gz > pokieticker.db
```

### 3. Deploy on Render
Create a new **Web Service** from the repo.

Render should detect:
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn backend.api.main:app --host 0.0.0.0 --port $PORT`

Or use the included `render.yaml` via Blueprint deploy.

### 4. Environment variables
Optional unless you want live updates / AI processing:

- `POLYGON_API_KEY`
- `ANTHROPIC_API_KEY`
- `DATABASE_PATH` (default can stay as `/opt/render/project/src/pokieticker.db`)

The included startup script expects `pokieticker.db.gz` to exist in the repo root and will unpack it automatically if `pokieticker.db` is missing.

### 5. Verify
After deploy, open:

```text
https://YOUR-RENDER-APP.onrender.com/api/health
```

Expected:

```json
{"status":"ok"}
```

Then test:

- `/api/stocks`
- `/api/stocks/NVDA/ohlc`
- `/api/news/NVDA/particles`

### 6. Connect to stern-dashboard (Vercel)
In Vercel project settings for `stern-dashboard`, set:

```bash
CATALYST_API_URL=https://YOUR-RENDER-APP.onrender.com
```

Redeploy Vercel afterward.

## Important notes

### CORS
Backend has been relaxed to allow all origins temporarily so Vercel frontend can connect.
Tighten this later to your production domain(s).

### Prediction models
If `/api/predict/...` returns 404, it likely means trained model artifacts are missing from `backend/ml/models`.
Chart/news features can still work without them.
