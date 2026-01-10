# alpha_approvals_sync.py
"""Phase 26B — Sync Alpha approvals from Google Sheets → Postgres.

Reads the Google Sheet tab (default: Alpha_Approvals) and ingests decisions into
Postgres table alpha_approvals (append-only, idempotent via row_hash).

This is governance only:
- No command enqueue
- No trading

Enable:
- PREVIEW_ENABLED=1
- ALPHA_PREVIEW_PROPOSALS_ENABLED=1
- ALPHA_APPROVALS_SYNC_ENABLED=1 (default)

Sheet columns (header row, case-insensitive):
- ts (optional)
- proposal_id (UUID, recommended) OR idea_id
- proposal_hash (optional)
- token (optional)
- decision (APPROVE|DENY|HOLD)
- actor (optional; defaults to ALPHA_APPROVER_ACTOR or 'human')
- note (optional)

Idempotency:
- row_hash = sha256(proposal_id|proposal_hash|token|decision|actor|note)
  Insert is ON CONFLICT DO NOTHING.
"""

from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, List, Optional

try:
    from utils import info, warn, error, get_sheet
except Exception:  # pragma: no cover
    def info(msg: str):
        print(msg, flush=True)

    def warn(msg: str):
        print(f"WARNING: {msg}", flush=True)

    def error(msg: str):
        print(f"ERROR: {msg}", flush=True)

    def get_sheet():
        raise RuntimeError("get_sheet unavailable")

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None


def _truthy(v: Optional[str]) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _get_db_url() -> Optional[str]:
    return os.getenv("DB_URL") or os.getenv("DATABASE_URL")


def _connect():
    db_url = _get_db_url()
    if not db_url:
        warn("alpha_approvals_sync: DB_URL not set; skipping.")
        return None
    if not psycopg2:
        warn("alpha_approvals_sync: psycopg2 not available; skipping.")
        return None
    try:
        return psycopg2.connect(db_url, connect_timeout=10)
    except Exception as e:
        warn(f"alpha_approvals_sync: DB connect failed; skipping. err={e}")
        return None


def _normalize_decision(raw: str) -> Optional[str]:
    v = (raw or "").strip().upper()
    if not v:
        return None
    # allow friendly inputs
    aliases = {
        "APPROVED": "APPROVE",
        "YES": "APPROVE",
        "Y": "APPROVE",
        "DENIED": "DENY",
        "NO": "DENY",
        "N": "DENY",
        "HOLD": "HOLD",
        "WAIT": "HOLD",
    }
    v = aliases.get(v, v)
    if v not in ("APPROVE", "DENY", "HOLD"):
        return None
    return v


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _header_map(header: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for i, h in enumerate(header):
        key = (h or "").strip().lower()
        if key:
            out[key] = i
    return out


def _cell(row: List[str], h: Dict[str, int], *names: str) -> str:
    for n in names:
        idx = h.get(n)
        if idx is not None and idx < len(row):
            return (row[idx] or "").strip()
    return ""


def run_alpha_approvals_sync() -> None:
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        info("alpha_approvals_sync: disabled (PREVIEW_ENABLED and/or ALPHA_PREVIEW_PROPOSALS_ENABLED not set).")
        return

    if not _truthy(os.getenv("ALPHA_APPROVALS_SYNC_ENABLED", "1")):
        info("alpha_approvals_sync: ALPHA_APPROVALS_SYNC_ENABLED=0; skipping.")
        return

    conn = _connect()
    if conn is None:
        return

    tab = os.getenv("ALPHA_APPROVALS_SHEET_TAB", "Alpha_Approvals")
    default_actor = os.getenv("ALPHA_APPROVER_ACTOR", "human")

    try:
        sheet = get_sheet()
        try:
            ws = sheet.worksheet(tab)
        except Exception:
            warn(f"alpha_approvals_sync: sheet tab '{tab}' missing; creating.")
            ws = sheet.add_worksheet(title=tab, rows="500", cols="12")

        # Ensure header exists
        values = ws.get_all_values() or []
        if not values:
            ws.append_row(["ts", "proposal_id", "proposal_hash", "token", "decision", "actor", "note"])
            info("alpha_approvals_sync: initialized empty Alpha_Approvals tab.")
            return

        header = values[0]
        hmap = _header_map(header)
        if "decision" not in hmap:
            # Reset header to known schema
            ws.clear()
            ws.append_row(["ts", "proposal_id", "proposal_hash", "token", "decision", "actor", "note"])
            info("alpha_approvals_sync: repaired Alpha_Approvals header.")
            return

        rows = values[1:]

        cur = conn.cursor()
        # Ensure table exists
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name='alpha_approvals' LIMIT 1;")
        if cur.fetchone() is None:
            warn("alpha_approvals_sync: missing table alpha_approvals; run sql/alpha_approvals.sql first.")
            return

        inserted = 0
        processed = 0

        for r in rows:
            if not r or all((c or "").strip() == "" for c in r):
                continue

            decision = _normalize_decision(_cell(r, hmap, "decision"))
            if not decision:
                continue

            proposal_id = _cell(r, hmap, "proposal_id", "idea_id")
            proposal_hash = _cell(r, hmap, "proposal_hash")
            token = _cell(r, hmap, "token")
            actor = _cell(r, hmap, "actor") or default_actor
            note = _cell(r, hmap, "note")

            if not (proposal_id or proposal_hash or token):
                # not enough to link
                continue

            processed += 1

            key = "|".join([
                proposal_id.strip(),
                proposal_hash.strip(),
                token.strip().upper(),
                decision,
                actor.strip(),
                note.strip(),
            ])
            row_hash = _sha256(key)

            cur.execute(
                """
                INSERT INTO alpha_approvals (
                  agent_id, proposal_id, proposal_hash, token,
                  decision, actor, note, source, row_hash
                )
                VALUES (
                  %s,
                  NULLIF(%s,'')::uuid,
                  NULLIF(%s,''),
                  NULLIF(%s,''),
                  %s,
                  %s,
                  NULLIF(%s,''),
                  'sheet',
                  %s
                )
                ON CONFLICT (row_hash) DO NOTHING;
                """,
                (
                    os.getenv("AGENT_ID", "bus"),
                    proposal_id,
                    proposal_hash,
                    token.upper() if token else "",
                    decision,
                    actor,
                    note,
                    row_hash,
                ),
            )
            if cur.rowcount == 1:
                inserted += 1

        conn.commit()
        info(f"alpha_approvals_sync: processed={processed} inserted_new={inserted} (tab='{tab}')")

    except Exception as e:
        error(f"alpha_approvals_sync failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_alpha_approvals_sync()
