#!/usr/bin/env python3
"""
Classifies DLQ messages into retriable vs permanently-failed.

Rules:
  - AttemptNumber >= MAX_ATTEMPTS: permanent
  - error kind in {network, 5xx, ratelimit}: retriable (bump AttemptNumber)
  - error kind in {4xx, schema}: permanent
  - everything else: retriable, conservative
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    inp = pathlib.Path(args.input)
    if not inp.exists() or inp.stat().st_size == 0:
        pathlib.Path(args.report).write_text(json.dumps({"retriable": 0, "permanent": 0}, indent=2))
        sys.exit(0)

    retriable_path = pathlib.Path(args.report).with_suffix("").as_posix() + "-retriable.jsonl"
    permanent_path = pathlib.Path(args.report).with_suffix("").as_posix() + "-permanent.jsonl"
    r = open(retriable_path, "w")
    p = open(permanent_path, "w")

    counts = {"retriable": 0, "permanent": 0, "bad": 0}
    with inp.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                counts["bad"] += 1
                continue
            attempt = msg.get("attempt_number", 1)
            kind = msg.get("last_error_kind", "unknown")
            if attempt >= args.max_attempts or kind in {"4xx", "schema"}:
                p.write(json.dumps(msg) + "\n")
                counts["permanent"] += 1
            else:
                msg["attempt_number"] = attempt + 1
                r.write(json.dumps(msg) + "\n")
                counts["retriable"] += 1
    r.close(); p.close()

    pathlib.Path(args.report).write_text(json.dumps(counts, indent=2))
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
