#!/usr/bin/env python3
"""Phase 26E smoketest.

This smoketest:
- Ensures the module imports
- Runs a single pass of the outbox enqueuer (dry-run order.place)

Run:
  cd /opt/render/project/src
  python phase26e_smoketest.py

It should print a summary line.
"""

from __future__ import annotations

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def main() -> None:
    from alpha_outbox_orderplace_dryrun import run

    res = run()
    logging.info("phase26e_smoketest: %s", res)


if __name__ == "__main__":
    main()
