#!/usr/bin/env python3
"""CS5224 Lab2 Back-End: FastAPI serving follower/followee stats.

Loads the PySpark output CSV (from GCS) into memory at startup,
serves GET /api/users/{user_id} as JSON, and hosts the static front-end.

Run: uvicorn main:app --host 127.0.0.1 --port 8000
"""
import csv
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

DATA_DIR = Path(__file__).parent / "data"
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

app = FastAPI(title="CS5224 Lab2 Back-End")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def load_stats() -> dict:
    """Read Spark CSV output (user_id,followers,followees) into a dict."""
    stats = {}
    for path in sorted(DATA_DIR.glob("part-*.csv")):
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                stats[row["user_id"]] = {
                    "followers": int(row["followers"]),
                    "followees": int(row["followees"]),
                }
    if not stats:
        raise RuntimeError(f"no stats CSV found in {DATA_DIR}")
    return stats


STATS = load_stats()


@app.get("/api/users/{user_id}")
def get_user(user_id: str):
    row = STATS.get(user_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")
    return {"user_id": user_id, **row}


@app.get("/api/stats")
def get_stats():
    """Dataset-level metadata for the front-end header."""
    return {"users": len(STATS)}


# Mount last so /api routes take precedence over static files
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
