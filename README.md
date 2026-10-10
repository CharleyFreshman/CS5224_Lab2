# CS5224 Lab 2 — Twitter Follower/Followee Analysis Pipeline

Full-stack data analytics pipeline: **Twitter dataset → Google Cloud Storage → Dataproc (PySpark) → FastAPI back-end → HTML/JS front-end**.

## Repository layout

```text
.
├── analysis/
│   └── twitter_analysis.py   # PySpark job: parse → dedupe → count → full outer join → CSV
├── backend/
│   ├── main.py               # FastAPI: GET /api/users/{id}, hosts the front-end
│   └── data/                 # Spark output CSV (synced from GCS)
├── frontend/
│   └── index.html            # fetch() → renders "UserID has N of followers/followees"
└── README.md
```

## Prerequisites

- Python 3.10+
- `pip install fastapi uvicorn`
- For re-running analysis: a GCP project with Dataproc + `gcloud` CLI (optional for serving precomputed results)

## Run the back-end and front-end

```bash
cd backend
uvicorn main:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000/ — the front-end fetches both required users automatically.

- API: `GET /api/users/{user_id}` → `{"user_id": "...", "followers": N, "followees": M}` (404 for unknown users)
- A search box on the page lets you query any of the 81,306 users in the dataset.

### Refresh data from GCS (optional)

The back-end loads `backend/data/*.csv` at startup. To re-sync the latest analysis output:

```bash
gcloud storage rsync gs://cs5224-twitter-ssf/outputs/twitter_stats backend/data --exclude "_SUCCESS"
```

## Re-run the data analysis (Dataproc / PySpark)

```bash
gsutil cp analysis/twitter_analysis.py gs://<bucket>/scripts/

gcloud dataproc jobs submit pyspark gs://<bucket>/scripts/twitter_analysis.py \
  --cluster=<cluster> \
  -- gs://<bucket>/raw/twitter_combined.txt \
     gs://<bucket>/outputs/twitter_stats \
     gs://<bucket>/outputs/twitter_summary \
     214328887,107830991
```

Local smoke test (no Spark needed):

```bash
python analysis/smoke_local.py
```

## Results

| UserID | followers | followees |
|---|---|---|
| 214328887 | 180 | 214 |
| 107830991 | 180 | 157 |

Dataset: 2,426,143 lines → 657,994 duplicates removed → 1,768,149 distinct edges → 81,306 users.
Invariant: Σfollowers = Σfollowees = distinct edges ✓
