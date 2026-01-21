#!/usr/bin/env python3
"""
wnh_logger.py — "Why Nothing Happened" (WNH) compiler + logger (DB + Sheet mirror)

Goal:
- Persist a normalized explanation record any time a decision results in:
  - blocked / denied
  - hold / noop / deferred / skipped
- Keep this SAFE in observation mode: write-only, tolerant of missing DB/Sheets,
  and aggressively de-duped to avoid noise.

Integration point:
- policy_logger.log_decision() calls wnh_logger.maybe_log_wnh(decision, intent, when=ts)

Notes:
- DB-first: attempts DB insert if db_write_adapter is available; always best-effort.
- Sheet mirror: appends to WNH worksheet when SHEET_URL is configured.
- De-dupe: in-memory TTL de-dupe + optional sheet tail check (lightweight).

Phase: 29+ runway enablement (observation compatible)
"""

from __future__ import annotations

import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from decision_story import generate_decision_story

try:
    from utils import ensure_sheet_headers, get_records_cached, get_ws_cached, ws_append_row
except Exception:  # pragma: no cover
    ensure_sheet_headers = None  # type: ignore
    get_records_cached = None  # type: ignore
    get_ws_cached = None  # type: ignore
    ws_append_row = None  # type: ignore


SHEET_URL = os.getenv("SHEET_URL")
WNH_WS = os.getenv("WNH_WS", "Why_Nothing_Happened")
WNH_ENABLED = os.getenv("WNH_ENABLE", "1").lower() in ("1", "true", "yes", "on")

# De-dupe window: suppress identical explanations (same token/outcome/primary_reason) inside this TTL
WNH_DEDUPE_TTL_SEC = int(os.getenv("WNH_DEDUPE_TTL_SEC", "3600"))  # 1 hour default
# Sheet tail check depth (avoid reading entire tab)
WNH_SHEET_TAIL_N = int(os.getenv("WNH_SHEET_TAIL_N", "80"))


# -----------------------------
# Helpers
# -----------------------------

_DEDUPE: Dict[str, float] = {}

def _now() -> float:
    return time.time()

def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return "{}"

def _norm_token(intent: Dict[str, Any], decision: Dict[str, Any]) -> str:
    tok = (
        (intent.get("token") or intent.get("asset") or intent.get("base") or "")
        or (decision.get("token") or decision.get("base") or "")
    )
    return str(tok).strip().upper()

def _norm_venue(intent: Dict[str, Any], decision: Dict[str, Any]) -> str:
    return str(intent.get("venue") or decision.get("venue") or "").strip().upper()

def _norm_quote(intent: Dict[str, Any], decision: Dict[str, Any]) -> str:
    return str(intent.get("quote") or decision.get("quote") or "").strip().upper()

def _pull_decision_id(decision: Dict[str, Any]) -> str:
    return str(decision.get("decision_id") or decision.get("id") or "").strip()

def _pull_limits_applied(decision: Dict[str, Any]) -> List[str]:
    # support both policy_decision.py meta.limits_applied and ad-hoc decision dicts
    meta = decision.get("meta") or {}
    out = decision.get("limits_applied") or meta.get("limits_applied") or []
    if isinstance(out, str):
        out = [out]
    if isinstance(out, list):
        return [str(x) for x in out if str(x).strip()]
    return []

def _pull_council_trace(decision: Dict[str, Any]) -> Dict[str, Any]:
    meta = decision.get("meta") or {}
    ct = decision.get("council_trace") or meta.get("council_trace") or decision.get("council") or {}
    return ct if isinstance(ct, dict) else {}

def _pull_autonomy_context(decision: Dict[str, Any]) -> Dict[str, Any]:
    # Many flows embed this differently; tolerate all.
    out: Dict[str, Any] = {}
    for k in ("autonomy", "autonomy_mode", "mode", "edge_mode"):
        v = decision.get(k)
        if v:
            out[k] = v
    holds = decision.get("holds") or (decision.get("autonomy_state") or {}).get("holds") or {}
    if isinstance(holds, dict):
        active = [name for name, on in holds.items() if _truthy(on)]
        if active:
            out["holds"] = active
    return out

def _classify_outcome(decision: Dict[str, Any]) -> Tuple[str, str, List[str]]:
    """
    Returns (outcome, primary_reason, secondary_reasons[])
    outcome in: blocked | deferred | noop | hold | resized | unknown
    """
    ok = bool(decision.get("ok", True))
    skipped = _truthy(decision.get("skipped"))
    recommendation = str(decision.get("recommendation") or "").strip().upper()
    status = str(decision.get("status") or "").strip()
    reason = str(decision.get("reason") or status or "").strip()

    secondary: List[str] = []

    # Many decision-only flows use "reasons": [...]
    rs = decision.get("reasons")
    if isinstance(rs, list):
        secondary.extend([str(x) for x in rs if str(x).strip()])

    # Signals error breadcrumbs, etc.
    if decision.get("signals_error"):
        secondary.append(f"signals_error={decision.get('signals_error')}")

    # Generic flags/applied limits, etc.
    flags = decision.get("flags") or decision.get("applied") or []
    if isinstance(flags, list):
        secondary.extend([str(x) for x in flags if str(x).strip()])

    # Determine outcome
    if not ok:
        outcome = "blocked"
    elif skipped:
        outcome = "deferred"
    elif recommendation in {"HOLD", "STOP", "PAUSE"}:
        outcome = "hold"
    elif recommendation in {"NOOP", "NONE", "SKIP"}:
        outcome = "noop"
    else:
        # Detect resize (patched intent amount differs)
        try:
            intent = decision.get("intent") or {}
            patched = decision.get("patched") or decision.get("patched_intent") or {}
            req = float(intent.get("amount_usd")) if intent.get("amount_usd") is not None else None
            appr = float(patched.get("amount_usd")) if patched.get("amount_usd") is not None else None
            if req is not None and appr is not None and abs(req - appr) > 1e-6:
                outcome = "resized"
            else:
                outcome = "unknown"
        except Exception:
            outcome = "unknown"

    if not reason:
        # Make sure primary reason is never empty
        if outcome == "hold":
            reason = "HOLD"
        elif outcome == "noop":
            reason = "NOOP"
        elif outcome == "deferred":
            reason = "DEFERRED"
        elif outcome == "blocked":
            reason = "DENIED"
        else:
            reason = "UNSPECIFIED"

    # De-dup secondary
    seen=set()
    sec2=[]
    for s in secondary:
        s=str(s).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        sec2.append(s)
    return outcome, reason, sec2

def _should_log(outcome: str) -> bool:
    # WNH is about non-actions. We log these outcomes.
    return outcome in {"blocked", "deferred", "noop", "hold"}


def _headers() -> List[str]:
    return [
        "Timestamp",
        "Token",
        "Stage",
        "Outcome",
        "Primary_Reason",
        "Secondary_Reasons",
        "Limits_Applied",
        "Autonomy",
        "Venue",
        "Quote",
        "Agent_ID",
        "Decision_IDs",
        "Story",
        "Decision_JSON",
        "Intent_JSON",
    ]


def _dedupe_key(ts: str, token: str, stage: str, outcome: str, primary_reason: str) -> str:
    # We intentionally do NOT include ts in the key.
    return "|".join([token, stage, outcome, primary_reason])[:400]


def _dedupe_hit(key: str) -> bool:
    # in-memory TTL de-dupe
    now = _now()
    # purge
    for k, t in list(_DEDUPE.items()):
        if now - t > WNH_DEDUPE_TTL_SEC:
            _DEDUPE.pop(k, None)
    t = _DEDUPE.get(key)
    if t is not None and (now - t) <= WNH_DEDUPE_TTL_SEC:
        return True
    _DEDUPE[key] = now
    return False


def _sheet_tail_has(key: str) -> bool:
    """
    Optional extra de-dupe: look at last N rows in the sheet and suppress duplicates.
    Safe if Sheets is down (returns False).
    """
    if not SHEET_URL or not get_records_cached:
        return False
    try:
        rows = get_records_cached(WNH_WS, ttl_s=60) or []
        tail = rows[-WNH_SHEET_TAIL_N:] if len(rows) > WNH_SHEET_TAIL_N else rows
        for r in tail:
            tok = str(r.get("Token","")).strip().upper()
            stage = str(r.get("Stage","")).strip()
            outcome = str(r.get("Outcome","")).strip()
            pr = str(r.get("Primary_Reason","")).strip()
            k = _dedupe_key("", tok, stage, outcome, pr)
            if k == key:
                return True
    except Exception:
        return False
    return False


# -----------------------------
# Core: compile + log
# -----------------------------

def compile_wnh_record(decision: Dict[str, Any], intent: Dict[str, Any], when: str) -> Optional[Dict[str, Any]]:
    if not WNH_ENABLED:
        return None
    if not isinstance(decision, dict) or not isinstance(intent, dict):
        return None

    token = _norm_token(intent, decision)
    venue = _norm_venue(intent, decision)
    quote = _norm_quote(intent, decision)

    outcome, primary_reason, secondary = _classify_outcome(decision)
    if not _should_log(outcome):
        return None

    stage = str(decision.get("phase") or decision.get("stage") or intent.get("stage") or "policy").strip() or "policy"

    limits = _pull_limits_applied(decision)
    autonomy = _pull_autonomy_context(decision)
    council_trace = _pull_council_trace(decision)

    # Story (human)
    try:
        story = generate_decision_story(intent=intent, decision=decision, autonomy_state=autonomy or None)
    except Exception:
        story = str(decision.get("reason") or decision.get("status") or "")

    agent_id = str(decision.get("agent_id") or intent.get("agent_id") or "").strip()

    decision_ids = []
    did = _pull_decision_id(decision)
    if did:
        decision_ids.append(did)

    record = {
        "Timestamp": when,
        "Token": token,
        "Stage": stage,
        "Outcome": outcome,
        "Primary_Reason": primary_reason,
        "Secondary_Reasons": "; ".join(secondary[:12]),
        "Limits_Applied": "; ".join(limits[:12]),
        "Autonomy": _safe_json({"autonomy": autonomy, "council_trace": council_trace}) if (autonomy or council_trace) else "",
        "Venue": venue,
        "Quote": quote,
        "Agent_ID": agent_id,
        "Decision_IDs": ";".join(decision_ids),
        "Story": story,
        "Decision_JSON": _safe_json(decision),
        "Intent_JSON": _safe_json(intent),
    }
    return record


def _write_db(record: Dict[str, Any]) -> None:
    """
    Best-effort DB insert using db_mirror (append-only event mirror).

    We intentionally re-use the existing mirror schema (sheet_mirror_events)
    so this stays DB-first without requiring a new migration.
    """
    try:
        from db_mirror import mirror_append  # type: ignore
    except Exception:
        return
    try:
        # Mirror tab name matches the Sheet tab to keep parity simple.
        mirror_append(WNH_WS, [record])
    except Exception:
        return


def _write_sheet(record: Dict[str, Any]) -> None:
(record: Dict[str, Any]) -> None:
    if not SHEET_URL or not ensure_sheet_headers or not get_ws_cached or not ws_append_row:
        return
    try:
        ensure_sheet_headers(WNH_WS, _headers())
        ws = get_ws_cached(WNH_WS, ttl_s=60)
        row = [record.get(h, "") for h in _headers()]
        ws_append_row(ws, row)
    except Exception:
        return


def maybe_log_wnh(decision: Any, intent: Dict[str, Any], when: str) -> None:
    """
    Main entrypoint. Safe: never raises.
    """
    if not WNH_ENABLED:
        return
    if not isinstance(decision, dict):
        try:
            decision = dict(decision)  # type: ignore
        except Exception:
            return

    rec = compile_wnh_record(decision, intent, when=when)
    if not rec:
        return

    key = _dedupe_key(when, rec.get("Token",""), rec.get("Stage",""), rec.get("Outcome",""), rec.get("Primary_Reason",""))
    if _dedupe_hit(key) or _sheet_tail_has(key):
        return

    # DB + Sheet mirror (best-effort, never block)
    _write_db(rec)
    _write_sheet(rec)
