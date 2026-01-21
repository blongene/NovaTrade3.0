#!/usr/bin/env python3
"""alpha_wnh_mirror.py

Alpha-flavored "Why Nothing Happened" (WNH).

Writes *explanations of inaction* into the shared surface:
  - Sheets tab: Why_Nothing_Happened (presentation)
  - DB mirror: sheet_mirror_events (advisory shadow)

This module is:
  - Observation-safe (no enqueue, no trading)
  - Best-effort (never raises)
  - JSON-first (reads DB_READ_JSON for toggles; no new env vars)

Assumptions (Phase 26A/26B/26E)
------------------------------
DB tables/views expected (created by your alpha_sqls):
  - alpha_proposals
  - alpha_approvals_latest_v (view)

If these don't exist yet, the module will emit a single diagnostic WNH row.
"""

from __future__ import annotations

import json
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Tuple


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        # Support CONFIG_BUNDLE_JSON.vars.DB_READ_JSON fallback if present
        bundle = (os.getenv("CONFIG_BUNDLE_JSON") or "").strip()
        if not bundle:
            return {}
        try:
            b = json.loads(bundle)
            v = (b.get("vars") or {}).get("DB_READ_JSON") or {}
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg_get(cfg: Dict[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return default
        if part not in cur:
            return default
        cur = cur[part]
    return default if cur is None else cur


def _db_url() -> str:
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or ""


def _utc_day() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _now_utc() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _connect_pg():
    try:
        import psycopg2  # type: ignore

        url = _db_url()
        if not url:
            return None
        conn = psycopg2.connect(url)
        conn.autocommit = True
        return conn
    except Exception:
        return None


def _fetch_alpha_today(conn) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """Return (error, rows) where rows are latest proposal per token for current UTC day."""
    day = _utc_day()
    sql = """
    WITH p AS (
      SELECT
        ap.ts,
        ap.proposal_id,
        ap.proposal_hash,
        ap.agent_id,
        ap.token,
        ap.venue,
        ap.symbol,
        ap.action,
        ap.notional_usd,
        ap.confidence,
        ap.rationale,
        ap.gates,
        ap.payload,
        ROW_NUMBER() OVER (PARTITION BY ap.token ORDER BY ap.ts DESC) AS rn
      FROM alpha_proposals ap
      WHERE to_char((ap.ts AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') = %s
    )
    SELECT
      p.ts,
      p.proposal_id,
      p.proposal_hash,
      p.agent_id,
      p.token,
      p.venue,
      p.symbol,
      p.action,
      p.notional_usd,
      p.confidence,
      p.rationale,
      p.gates,
      p.payload,
      a.decision AS approval_decision,
      a.actor    AS approval_actor,
      a.note     AS approval_note,
      a.ts       AS approval_ts
    FROM p
    LEFT JOIN alpha_approvals_latest_v a
      ON (a.proposal_hash = p.proposal_hash)
      OR (a.proposal_id = p.proposal_id)
      OR (a.token = p.token)
    WHERE p.rn = 1
    ORDER BY p.ts DESC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (day,))
            cols = [d[0] for d in cur.description]
            out = []
            for row in cur.fetchall():
                rec = {cols[i]: row[i] for i in range(len(cols))}
                out.append(rec)
        return None, out
    except Exception as e:
        return f"{e.__class__.__name__}:{e}", []


def _headers() -> List[str]:
    # Match your WNH sheet layout (includes Signature).
    return [
        "Timestamp",
        "Token",
        "Stage",
        "Outcome",
        "Primary_Reason",
        "Secondary_Reasons",
        "Limits_Applied",
        "Autonomy",
        "Decision_ID",
        "Story",
        "Decision_JSON",
        "Intent_JSON",
        "Signature",
    ]


def _ensure_sheet_headers(tab: str) -> None:
    # Prefer your guarded helper; fall back to direct worksheet creation if needed.
    try:
        from utils import ensure_sheet_headers  # type: ignore

        ensure_sheet_headers(tab, _headers())
        return
    except Exception:
        pass

    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            return
        svc = (
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            or os.getenv("GOOGLE_CREDENTIALS_JSON")
            or os.getenv("SVC_JSON")
            or "sentiment-log-service.json"
        )
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(sheet_url)
        try:
            ws = sh.worksheet(tab)
        except Exception:
            ws = sh.add_worksheet(title=tab, rows=4000, cols=30)
            ws.append_row(_headers(), value_input_option="USER_ENTERED")
        # If sheet exists but has no headers, attempt to write them once
        try:
            vals = ws.get_all_values()
            if not vals:
                ws.append_row(_headers(), value_input_option="USER_ENTERED")
        except Exception:
            pass
    except Exception:
        return


def _append_row(tab: str, row: List[Any]) -> None:
    try:
        from utils import get_ws_cached, ws_append_row  # type: ignore

        ws = get_ws_cached(tab, ttl_s=60)
        ws_append_row(ws, row)
        return
    except Exception:
        pass

    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            return
        svc = (
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            or os.getenv("GOOGLE_CREDENTIALS_JSON")
            or os.getenv("SVC_JSON")
            or "sentiment-log-service.json"
        )
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(sheet_url)
        ws = sh.worksheet(tab)
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        return


def _mirror_db(tab: str, row: List[Any]) -> None:
    try:
        from db_mirror import mirror_append  # type: ignore

        mirror_append(tab, [row])
    except Exception:
        pass


def _make_story(token: str, outcome: str, reason: str, extra: str = "") -> str:
    base = f"ALPHA {token} {outcome}. {reason}".strip()
    if extra:
        return f"{base} ({extra})"
    return base


def _signature(token: str, stage: str, outcome: str, primary_reason: str, secondary: str = "") -> str:
    # Short stable signature for dedupe + human diffing
    s = f"{token}|{stage}|{outcome}|{primary_reason}|{secondary}".upper().encode("utf-8")
    return hashlib.sha256(s).hexdigest()[:16]


def _dedupe_signature(token: str, stage: str, outcome: str, primary_reason: str) -> str:
    # in-memory dedupe key (coarser than Signature column)
    return f"{token}|{stage}|{outcome}|{primary_reason}".upper()


_RECENT: Dict[str, float] = {}


def _deduped(sig: str, ttl: int) -> bool:
    now = time.time()
    for k, ts in list(_RECENT.items())[:200]:
        if (now - ts) > ttl:
            _RECENT.pop(k, None)
    ts = _RECENT.get(sig)
    if ts is not None and (now - ts) < ttl:
        return True
    _RECENT[sig] = now
    return False


def run_alpha_wnh_mirror() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    wnh = cfg.get("wnh") or {}
    if not isinstance(wnh, dict):
        wnh = {}
    if not _truthy(wnh.get("enabled", 0)):
        return {"ok": False, "skipped": True, "reason": "wnh.disabled"}

    tab = str(wnh.get("tab") or "Why_Nothing_Happened")
    ttl = int(wnh.get("dedupe_ttl_sec") or 3600)

    alpha_enabled = _truthy(_cfg_get(cfg, "phases.phase26.enabled", 0))
    planning_enabled = _truthy(_cfg_get(cfg, "phases.phase26.alpha.planning_enabled", 0))
    exec_enabled = _truthy(_cfg_get(cfg, "phases.phase26.alpha.execution_enabled", 0))
    mode = str(_cfg_get(cfg, "phases.phase26.mode", "")).strip()

    autonomy = f"mode={mode} exec_enabled={1 if exec_enabled else 0}"

    if not (alpha_enabled and planning_enabled):
        stage = "ALPHA"
        outcome = "NOOP"
        primary_reason = "ALPHA_PLANNING_DISABLED"
        sig = _dedupe_signature("(all)", stage, outcome, primary_reason)
        if not _deduped(sig, ttl):
            _ensure_sheet_headers(tab)
            signature = _signature("", stage, outcome, primary_reason)
            row = [
                _now_utc(),
                "",
                stage,
                outcome,
                primary_reason,
                "",
                "",
                autonomy,
                "",
                _make_story("(all)", outcome, "Alpha planning is disabled in DB_READ_JSON."),
                _safe_json({"mode": mode}),
                _safe_json({}),
                signature,
            ]
            _append_row(tab, row)
            _mirror_db(tab, row)
        return {"ok": True, "rows": 1, "note": "alpha planning disabled"}

    conn = _connect_pg()
    if conn is None:
        stage = "ALPHA"
        outcome = "BLOCKED"
        primary_reason = "DB_UNAVAILABLE"
        sig = _dedupe_signature("(all)", stage, outcome, primary_reason)
        if not _deduped(sig, ttl):
            _ensure_sheet_headers(tab)
            signature = _signature("", stage, outcome, primary_reason)
            row = [
                _now_utc(),
                "",
                stage,
                outcome,
                primary_reason,
                "",
                "",
                autonomy,
                "",
                _make_story("(all)", outcome, "DATABASE_URL/DB_URL not configured or DB connect failed."),
                _safe_json({"mode": mode}),
                _safe_json({}),
                signature,
            ]
            _append_row(tab, row)
            _mirror_db(tab, row)
        return {"ok": False, "rows": 1, "reason": "db_unavailable"}

    err, rows = _fetch_alpha_today(conn)
    try:
        conn.close()
    except Exception:
        pass

    _ensure_sheet_headers(tab)

    written = 0
    if err:
        stage = "ALPHA"
        outcome = "BLOCKED"
        primary_reason = "ALPHA_QUERY_FAILED"
        sig = _dedupe_signature("(all)", stage, outcome, primary_reason)
        if not _deduped(sig, ttl):
            signature = _signature("", stage, outcome, primary_reason, err)
            row = [
                _now_utc(),
                "",
                stage,
                outcome,
                primary_reason,
                err,
                "",
                autonomy,
                "",
                _make_story("(all)", outcome, "Failed to query alpha_proposals.", extra=err),
                _safe_json({"error": err}),
                _safe_json({}),
                signature,
            ]
            _append_row(tab, row)
            _mirror_db(tab, row)
            written += 1
        return {"ok": False, "rows": written, "reason": err}

    if not rows:
        stage = "ALPHA"
        outcome = "NOOP"
        primary_reason = "NO_PROPOSALS"
        sig = _dedupe_signature("(all)", stage, outcome, primary_reason)
        if not _deduped(sig, ttl):
            signature = _signature("", stage, outcome, primary_reason)
            row = [
                _now_utc(),
                "",
                stage,
                outcome,
                primary_reason,
                "",
                "",
                autonomy,
                "",
                _make_story("(all)", outcome, "Alpha ran, but produced no proposals for today (UTC)."),
                _safe_json({"utc_day": _utc_day()}),
                _safe_json({}),
                signature,
            ]
            _append_row(tab, row)
            _mirror_db(tab, row)
            written += 1
        return {"ok": True, "rows": written, "note": "no proposals"}

    for r in rows:
        token = str(r.get("token") or "").upper().strip()
        if not token:
            continue

        action = str(r.get("action") or "").upper().strip()
        gates = r.get("gates")
        if isinstance(gates, str):
            try:
                gates = json.loads(gates)
            except Exception:
                gates = {}
        if not isinstance(gates, dict):
            gates = {}

        primary_blocker = str(gates.get("primary_blocker") or "").upper().strip()
        blockers = gates.get("blockers")
        if not isinstance(blockers, list):
            blockers = []
        blockers_s = ",".join(str(b) for b in blockers if b)

        approval_decision = str(r.get("approval_decision") or "").upper().strip()

        stage = "ALPHA"
        decision_id = str(r.get("proposal_id") or "")

        outcome = "NOOP"
        primary_reason = ""
        secondary = ""

        if action == "WOULD_SKIP":
            outcome = "BLOCKED"
            primary_reason = primary_blocker or "WOULD_SKIP"
            secondary = blockers_s
        elif action == "WOULD_WATCH":
            outcome = "DEFERRED"
            primary_reason = primary_blocker or "IMMATURE"
            secondary = blockers_s
        elif action == "WOULD_TRADE":
            if approval_decision in {"", "NONE"}:
                outcome = "DEFERRED"
                primary_reason = "AWAITING_APPROVAL"
            elif approval_decision == "HOLD":
                outcome = "DEFERRED"
                primary_reason = "HUMAN_HOLD"
                secondary = str(r.get("approval_note") or "")
            elif approval_decision == "DENY":
                outcome = "BLOCKED"
                primary_reason = "HUMAN_DENY"
                secondary = str(r.get("approval_note") or "")
            elif approval_decision == "APPROVE":
                if not exec_enabled:
                    outcome = "BLOCKED"
                    primary_reason = "ENQUEUE_DISABLED"
                else:
                    if "dryrun" in mode.lower():
                        outcome = "DEFERRED"
                        primary_reason = "APPROVED_DRYRUN"
                    else:
                        continue
        else:
            continue

        if not primary_reason:
            continue

        sig = _dedupe_signature(token, stage, outcome, primary_reason)
        if _deduped(sig, ttl):
            continue

        story = _make_story(token, outcome, primary_reason, extra=(secondary or ""))
        decision_json = {
            "proposal_id": r.get("proposal_id"),
            "proposal_hash": r.get("proposal_hash"),
            "action": action,
            "rationale": r.get("rationale"),
            "gates": gates,
            "approval": {
                "decision": approval_decision,
                "actor": r.get("approval_actor"),
                "note": r.get("approval_note"),
                "ts": str(r.get("approval_ts") or ""),
            },
        }
        intent_json = {
            "token": token,
            "venue": r.get("venue"),
            "symbol": r.get("symbol"),
            "notional_usd": float(r.get("notional_usd") or 0) if r.get("notional_usd") is not None else 0,
            "confidence": float(r.get("confidence") or 0) if r.get("confidence") is not None else 0,
            "utc_day": _utc_day(),
        }

        signature = _signature(token, stage, outcome, primary_reason, secondary)

        row = [
            _now_utc(),
            token,
            stage,
            outcome,
            primary_reason,
            secondary,
            "",
            autonomy,
            decision_id,
            story,
            _safe_json(decision_json),
            _safe_json(intent_json),
            signature,
        ]

        _append_row(tab, row)
        _mirror_db(tab, row)
        written += 1

    return {"ok": True, "rows": written}


if __name__ == "__main__":
    out = run_alpha_wnh_mirror()
    print(_safe_json(out))
