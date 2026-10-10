#!/usr/bin/env python3
"""CS5224 Lab2 Part2: follower/followee counts for the Twitter dataset.

Each line "A B" means user A follows user B. Data is dirty: duplicates and
malformed lines are handled here.

Usage:
  spark-submit twitter_analysis.py <input_path> <output_path> <summary_path> [user_id1,user_id2,...]

Outputs:
  <output_path>/part-*.csv   header: user_id,followers,followees (single file)
  <summary_path>/part-*      summary.json: cleaning metrics, invariants, target users
"""
import json
import sys

from pyspark.sql import SparkSession, functions as F

NUM_RE = r"^\d+$"


def main(input_path: str, output_path: str, summary_path: str, users: str = "") -> None:
    spark = SparkSession.builder.appName("cs5224-twitter-analysis").getOrCreate()

    raw = spark.read.text(input_path)
    total = raw.count()

    # Parse: tokens == 2 and both numeric => valid edge line
    tokens = F.split(F.trim(F.col("value")), r"\s+")
    df = raw.select(
        F.size(tokens).alias("n"),
        tokens.getItem(0).alias("src"),
        tokens.getItem(1).alias("dst"),
    )
    is_valid = (F.col("n") == 2) & F.col("src").rlike(NUM_RE) & F.col("dst").rlike(NUM_RE)
    valid = df.filter(is_valid).select("src", "dst")
    n_valid = valid.count()

    # Deduplicate directed edges before aggregation
    edges = valid.distinct().cache()
    n_edges = edges.count()

    # A follows B => +1 followee for A, +1 follower for B
    followers = edges.groupBy(F.col("dst").alias("user_id")).agg(
        F.count("*").alias("followers"))
    followees = edges.groupBy(F.col("src").alias("user_id")).agg(
        F.count("*").alias("followees"))

    # Full outer join: users may appear only as source or only as target
    stats = (
        followers.join(followees, "user_id", "full_outer")
        .select(
            "user_id",
            F.coalesce("followers", F.lit(0)).cast("long").alias("followers"),
            F.coalesce("followees", F.lit(0)).cast("long").alias("followees"),
        )
        .cache()
    )
    n_users = stats.count()
    sums = stats.agg(F.sum("followers").alias("sf"), F.sum("followees").alias("se")).first()

    summary = {
        "input": input_path,
        "total_lines": total,
        "valid_lines": n_valid,
        "invalid_lines": total - n_valid,
        "duplicates_removed": n_valid - n_edges,
        "distinct_edges": n_edges,
        "distinct_users": n_users,
        "invariants": {
            # sum(followers) == sum(followees) == distinct edges
            "sum_followers_eq_edges": sums["sf"] == n_edges,
            "sum_followees_eq_edges": sums["se"] == n_edges,
        },
    }

    if users:
        wanted = [u.strip() for u in users.split(",") if u.strip()]
        rows = (stats.filter(F.col("user_id").isin(wanted))
                .orderBy("user_id").collect())
        summary["target_users"] = {
            r["user_id"]: {"followers": r["followers"], "followees": r["followees"]}
            for r in rows
        }

    stats.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    spark.createDataFrame([(json.dumps(summary),)], ["value"]) \
        .write.mode("overwrite").text(summary_path)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        sys.exit("usage: spark-submit twitter_analysis.py <input> <output_csv> <summary> [ids]")
    main(*sys.argv[1:5])
