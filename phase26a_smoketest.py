# phase26a_smoketest.py
"""
One-shot smoke test for Phase 26A preview proposals.

It runs:
- alpha_proposal_runner.run_alpha_proposal_runner()
- alpha_proposals_mirror.run_alpha_proposals_mirror()

Expected:
- If disabled, it will print "disabled" and exit cleanly.
- If enabled but schema missing, it will log clear warnings.
- If enabled and schema present, it will insert proposals and mirror them into Sheets.

Usage:
  python phase26a_smoketest.py
"""
from alpha_proposal_runner import run_alpha_proposal_runner
from alpha_proposals_mirror import run_alpha_proposals_mirror

if __name__ == "__main__":
    run_alpha_proposal_runner()
    run_alpha_proposals_mirror()
