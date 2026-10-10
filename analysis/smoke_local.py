#!/usr/bin/env python3
"""Local small-sample smoke test (no Spark needed).

1) Synthetic edge cases: mirrors twitter_analysis.py semantics in pure Python.
2) Real-data sample: first N lines of twitter_combined.txt, verifies invariants
   and prints stats for the two required users.

Usage: python smoke_local.py [--file PATH] [--sample N]
"""
import argparse
import itertools
import sys

# mirror Java regex ^\d+$: ascii digits only (str.isdigit also accepts unicode digits)
NUM = lambda s: s.isascii() and s.isdigit()


def parse_and_count(lines):
    """Mirror of the PySpark logic: returns (edges, users, metrics)."""
    edges = set()
    total = invalid = 0
    users = {}  # uid -> [followers, followees]

    def touch(u):
        users.setdefault(u, [0, 0])

    for ln in lines:
        total += 1
        parts = ln.split()
        if len(parts) != 2 or not (NUM(parts[0]) and NUM(parts[1])):
            invalid += 1
            continue
        a, b = parts
        if (a, b) not in edges:
            edges.add((a, b))
            touch(a); touch(b)
            users[a][1] += 1  # a follows b -> followee
            users[b][0] += 1  # b followed by a -> follower
    return edges, users, {"total": total, "invalid": invalid}


def run_tests():
    lines = ["1 2", "1 2", "2 3", "3", "x 4", "", "4 2", "1 2 3"]
    edges, users, m = parse_and_count(lines)
    assert edges == {("1", "2"), ("2", "3"), ("4", "2")}, edges
    assert users == {"1": [0, 1], "2": [2, 1], "3": [1, 0], "4": [0, 1]}, users
    assert m == {"total": 8, "invalid": 4}, m
    sf = sum(v[0] for v in users.values()); se = sum(v[1] for v in users.values())
    assert sf == se == len(edges) == 3
    print("[PASS] synthetic cases: dup/direction/malformed/one-sided users/invariants")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=r"d:\TraeCN\Garth\Lab2\twitter_combined.txt")
    ap.add_argument("--sample", type=int, default=50000)
    args = ap.parse_args()

    run_tests()

    with open(args.file, "r", encoding="utf-8", errors="replace") as f:
        sample = list(itertools.islice(f, args.sample))
    edges, users, m = parse_and_count(sample)
    sf = sum(v[0] for v in users.values()); se = sum(v[1] for v in users.values())
    assert sf == se == len(edges), (sf, se, len(edges))

    print(f"[PASS] sample invariants: sum(followers)={sf} == sum(followees)={se} == edges={len(edges)}")
    print(f"sample lines={m['total']} invalid={m['invalid']} distinct_edges={len(edges)} users={len(users)}")
    for u in ("214328887", "107830991"):
        f_, e_ = users.get(u, [0, 0])
        print(f"user {u}: followers={f_} followees={e_}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
