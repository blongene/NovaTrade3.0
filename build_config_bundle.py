#!/usr/bin/env python3
"""NovaTrade Config Bundle Builder

Reads a text file containing blocks like:

ALPHA_CONFIG_JSON
{ ... }

DB_READ_JSON
{ ... }

and outputs a single JSON object suitable for Render env var CONFIG_BUNDLE_JSON.

Usage:
  python build_config_bundle.py --in "2026-01-17 JSONs.txt" --version 2026-01-17

Notes:
- This tool never prints secrets unless they are present in the input file.
- It validates that each JSON block parses cleanly.
"""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

HEADER_RE = re.compile(r'^[A-Z0-9_]{3,}$')


def parse_blocks(text: str) -> dict:
    lines = text.splitlines()
    i = 0
    blocks = {}

    def skip_blanks(idx: int) -> int:
        while idx < len(lines) and lines[idx].strip() == "":
            idx += 1
        return idx

    i = skip_blanks(i)
    while i < len(lines):
        header = lines[i].strip()
        if not HEADER_RE.match(header):
            raise ValueError(f"Expected HEADER at line {i+1}, got: {lines[i]!r}")
        i += 1
        i = skip_blanks(i)
        if i >= len(lines) or not lines[i].lstrip().startswith("{"):
            raise ValueError(f"Expected JSON object after header {header} at line {i+1}")

        # Collect JSON lines until braces balance back to 0.
        buf = []
        depth = 0
        started = False
        while i < len(lines):
            line = lines[i]
            if not started and line.lstrip().startswith("{"):
                started = True
            if started:
                depth += line.count("{")
                depth -= line.count("}")
                buf.append(line)
                i += 1
                if depth == 0:
                    break
            else:
                i += 1

        raw_json = "\n".join(buf).strip()
        try:
            obj = json.loads(raw_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parse error in block {header}: {e}") from e

        blocks[header] = obj
        i = skip_blanks(i)

    return blocks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input .txt file containing JSON blocks")
    ap.add_argument("--version", default=None, help="Bundle version tag (e.g., 2026-01-17)")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = ap.parse_args()

    path = Path(args.inp)
    text = path.read_text(encoding="utf-8")

    vars_dict = parse_blocks(text)

    version = args.version or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    bundle = {
        "version": version,
        "updated_at": updated_at,
        "vars": vars_dict,
    }

    if args.pretty:
        out = json.dumps(bundle, indent=2, sort_keys=True)
    else:
        out = json.dumps(bundle, separators=(",", ":"), sort_keys=True)

    print(out)


if __name__ == "__main__":
    main()
