#!/usr/bin/env python3
"""Day1 smoke test: verify Spark can read the Twitter dataset and write summary.

Usage:
  spark-submit day1_smoke_test.py <input_path> <output_summary_path>

input_path can be a local path or gs://bucket/raw/twitter_combined.txt.
Writes summary.json (as text file) with record counts and invalid-line samples.
"""
import json
import sys

from pyspark.sql import SparkSession, functions as F

NUM_RE = r"^\d+$"


def main(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("cs5224-day1-smoke").getOrCreate()

    raw = spark.read.text(input_path)
    total = raw.count()

    tokens = F.split(F.trim(F.col("value")), r"\s+")
    df = raw.select(
        F.col("value"),
        F.size(tokens).alias("n"),
        tokens.getItem(0).alias("src"),
        tokens.getItem(1).alias("dst"),
    )
    is_valid = (F.col("n") == 2) & F.col("src").rlike(NUM_RE) & F.col("dst").rlike(NUM_RE)

    n_valid = df.filter(is_valid).count()
    invalid_samples = [r["value"][:80] for r in df.filter(~is_valid).limit(5).collect()]
    valid_samples = [r["value"][:80] for r in df.filter(is_valid).limit(3).collect()]

    summary = {
        "input": input_path,
        "total_lines": total,
        "valid_lines": n_valid,
        "invalid_lines": total - n_valid,
        "invalid_samples": invalid_samples,
        "valid_samples": valid_samples,
    }
    spark.sparkContext.parallelize([json.dumps(summary)]).saveAsTextFile(output_path)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: spark-submit day1_smoke_test.py <input_path> <output_summary_path>")
    main(sys.argv[1], sys.argv[2])
