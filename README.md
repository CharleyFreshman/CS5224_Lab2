# CS5224 Lab 2 — Twitter Follower/Followee Analysis Pipeline

Full-stack data analytics pipeline: **Twitter dataset → Google Cloud Storage → Dataproc (PySpark) → FastAPI back-end → HTML/JS front-end**.

## Repository layout

```text
.
├── analysis/
│   ├── twitter_analysis.py   # PySpark job: parse → dedupe → count → full outer join → CSV
│   ├── smoke_local.py        # Local pure-Python mirror of the Spark logic (tests + sample)
│   └── day1_smoke_test.py    # Dataproc smoke job (data validity check)
├── backend/
│   ├── main.py               # FastAPI: GET /api/users/{id}, hosts the front-end
│   └── data/part-*.csv       # Spark output CSV (synced from GCS)
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
# 0. create a transient cluster (then delete it when done)
gcloud dataproc clusters create cs5224-day3-final --region=asia-southeast1 \
  --image-version=2.2 --num-workers=2 \
  --master-machine-type=e2-standard-2 --worker-machine-type=e2-standard-2 \
  --master-boot-disk-size=50 --worker-boot-disk-size=50 --no-address

# 1. upload the job script
gcloud storage cp analysis/twitter_analysis.py gs://cs5224-twitter-ssf/scripts/

# 2. submit — args: <input> <output_csv> <summary> [ids]
#    (on PowerShell, wrap in a .bat file: gcloud.ps1 otherwise swallows "--")
gcloud dataproc jobs submit pyspark gs://cs5224-twitter-ssf/scripts/twitter_analysis.py \
  --cluster=cs5224-day3-final --region=asia-southeast1 \
  --properties spark:spark.sql.shuffle.partitions=16,spark:spark.default.parallelism=16 \
  -- gs://cs5224-twitter-ssf/raw/twitter_combined.txt \
     gs://cs5224-twitter-ssf/outputs/twitter_stats \
     gs://cs5224-twitter-ssf/outputs/twitter_summary \
     214328887,107830991
```

Local smoke test (no Spark needed — pass the raw dataset path):

```bash
python analysis/smoke_local.py --file /path/to/twitter_combined.txt
```

## Results

| UserID | followers | followees |
|---|---|---|
| 214328887 | 180 | 214 |
| 107830991 | 180 | 157 |

Dataset: 2,426,143 lines → 657,994 duplicates removed → 1,768,149 distinct edges → 81,306 users.
Invariant: Σfollowers = Σfollowees = distinct edges ✓
