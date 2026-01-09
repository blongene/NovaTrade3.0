# alpha_proposal_runner.py
"""
Phase 26A — Preview Proposals (DB → alpha_proposals)

This module is *preview-only* by design:
- It NEVER enqueues commands.
- It NEVER calls any executor.
- It ONLY writes proposals into Postgres table `alpha_proposals` (append-only, deduped by proposal_hash).

It is safe to leave deployed continuously. When disabled, it becomes a no-op.

Enable flags (all must be truthy):
- PREVIEW_ENABLED=1
- ALPHA_PREVIEW_PROPOSALS_ENABLED=1

Optional:
- ALPHA_AGENT_ID (default: AGENT_ID or 'edge-primary')
- ALPHA_DEFAULT_TRADE_NOTIONAL_USD (default: 25)
- ALPHA_DEFAULT_TRADE_CONFIDENCE (default: 0.10)
- ALPHA_DEFAULT_WATCH_CONFIDENCE (default: 0.06)

DB:
- DB_URL must be set (standard Postgres URL)
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

# Logging helpers (fall back to print)
try:
    from utils import info, warn, error
except Exception:  # pragma: no cover
    def info(msg: str): print(msg, flush=True)
    def warn(msg: str): print(f"WARNING: {msg}", flush=True)
    def error(msg: str): print(f"ERROR: {msg}", flush=True)

# psycopg2 is optional in some deployments; we degrade safely.
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
        warn("alpha_proposal_runner: DB_URL not set; skipping.")
        return None
    if not psycopg2:
        warn("alpha_proposal_runner: psycopg2 not available; skipping.")
        return None
    try:
        return psycopg2.connect(db_url, connect_timeout=10)
    except Exception as e:
        warn(f"alpha_proposal_runner: DB connect failed; skipping. err={e}")
        return None


def _ensure_alpha_proposals_table(cur) -> None:
    """
    Create alpha_proposals table if missing.
    This is safe and idempotent. It does NOT create alpha_readiness_v (view), because that depends
    on your broader Phase 25/26 schema (alpha_ideas, alpha_policy_blocks, etc.).
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alpha_proposals (
          id              BIGSERIAL PRIMARY KEY,
          proposal_id     UUID        NOT NULL,
          ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          agent_id        TEXT        NOT NULL DEFAULT 'edge-primary',

          token           TEXT        NOT NULL,
          venue           TEXT,
          symbol          TEXT,

          action          TEXT        NOT NULL,  -- WOULD_TRADE / WOULD_WATCH / WOULD_SKIP
          notional_usd    NUMERIC,
          confidence      NUMERIC,

          rationale       TEXT,
          gates           JSONB,
          payload         JSONB,

          proposal_hash   TEXT        NOT NULL
        );
        """
    )
    # Optional index for dedupe speed
    cur.execute(
        "CREATE INDEX IF NOT EXISTS alpha_proposals_hash_idx ON alpha_proposals (proposal_hash);"
    )


def _have_view(cur, view_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.views WHERE table_name = %s LIMIT 1;",
        (view_name,),
    )
    return cur.fetchone() is not None


def _run_generator_insert(cur, params: Dict[str, Any]) -> int:
    """
    Insert proposals using alpha_readiness_v. Dedupe via proposal_hash.
    Returns number of inserted rows (best-effort; may be -1 if driver doesn't report).
    """
    # NOTE: This SQL body is adapted from sql/alpha_proposal_generator.sql
    # and is intentionally parameterized for psycopg2.
    sql = r"""
WITH params AS (
  SELECT
    %(preview_enabled)s::int AS preview_enabled,
    %(agent_id)s::text AS agent_id,
    %(default_trade_notional_usd)s::numeric AS default_trade_notional_usd,
    %(default_trade_confidence)s::numeric AS default_trade_confidence,
    %(default_watch_confidence)s::numeric AS default_watch_confidence,
    %(confidence_cap)s::numeric AS confidence_cap,
    to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD')::text AS utc_day
),
r AS (
  SELECT * FROM alpha_readiness_v
),
norm AS (
  SELECT
    n.*,
    (lower(coalesce(r.gate_a_memory_maturity::text,'')) IN ('1','t','true','y','yes','on')) AS gate_a,
    (lower(coalesce(r.gate_b_venue_feasible::text,'')) IN ('1','t','true','y','yes','on')) AS gate_b,
    (lower(coalesce(r.gate_c_fresh_enough::text,'')) IN ('1','t','true','y','yes','on')) AS gate_c,
    (lower(coalesce(r.gate_d_policy_clear::text,'')) IN ('1','t','true','y','yes','on')) AS gate_d
  FROM norm n
),
classified AS (
  SELECT
    n.*,

    ARRAY_REMOVE(ARRAY[
      CASE WHEN NOT n.gate_b THEN 'NO_TRADABLE_VENUE' END,
      CASE WHEN NOT n.gate_d THEN 'POLICY_BLOCK' END,
      CASE WHEN NOT n.gate_c THEN 'STALE' END,
      CASE WHEN NOT n.gate_a THEN 'IMMATURE' END
    ], NULL) AS blockers,

    CASE
      WHEN NOT n.gate_b THEN 'WOULD_SKIP'
      WHEN NOT n.gate_d THEN 'WOULD_SKIP'
      WHEN NOT n.gate_c THEN 'WOULD_WATCH'
      WHEN NOT n.gate_a THEN 'WOULD_WATCH'
      WHEN (n.gate_a AND n.gate_b AND n.gate_c AND n.gate_d) THEN 'WOULD_TRADE'
      ELSE 'WOULD_WATCH'
    END AS action,

    CASE
      WHEN (n.gate_a AND n.gate_b AND n.gate_c AND n.gate_d) THEN (SELECT default_trade_notional_usd FROM params)
      ELSE NULL
    END AS notional_usd,

    CASE
      WHEN (n.gate_a AND n.gate_b AND n.gate_c AND n.gate_d) THEN (SELECT default_trade_confidence FROM params)
      WHEN (NOT n.gate_c OR NOT n.gate_a) THEN (SELECT default_watch_confidence FROM params)
      ELSE (SELECT default_watch_confidence FROM params)
    END AS confidence,

    (
      WITH blk AS (
        SELECT COALESCE(
          ARRAY_TO_JSON(ARRAY_REMOVE(ARRAY[
            CASE WHEN NOT n.gate_b THEN 'NO_TRADABLE_VENUE' END,
            CASE WHEN NOT n.gate_d THEN 'POLICY_BLOCK' END,
            CASE WHEN NOT n.gate_c THEN 'STALE' END,
            CASE WHEN NOT n.gate_a THEN 'IMMATURE' END
          ], NULL))::jsonb,
          '[]'::jsonb
        ) AS blockers
      )
      SELECT jsonb_build_object(
        'schema', 'Alpha_Ideas.v1',
        'idea_id', gen_random_uuid(),
        'ts', to_char((NOW() AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        'agent_id', (SELECT agent_id FROM params),

        'phase', '25',
        'mode', 'observation_only',

        'signal_type', 'ALPHA_IDEA',
        'source', 'AlphaProposalGenerator',
        'source_ref', (SELECT utc_day FROM params),

        'token', n.token,
        'see', NULL,
        'venue_hint', NULLIF(n.venue,''),
        'symbol_hint', NULLIF(n.symbol,''),

        'novelty_reason', COALESCE(n.rationale,''),
        'thesis', COALESCE(n.rationale,''),

        'confidence', COALESCE(n.confidence, 0),
        'confidence_cap', %(confidence_cap)s::numeric,
        'signal_strength', CASE WHEN COALESCE(r.confidence,0) >= 0.15 THEN 'MEDIUM' ELSE 'LOW' END,

        'execution_allowed', 0,
        'blocked_by', (blk.blockers || '["alpha_execution_disabled"]'::jsonb),

        'facts', jsonb_build_object(
          'mentions_24h', 0,
          'rank_delta_24h', 0,
          'listing_signal', 0,
          'sentiment_score', 0,
          'liquidity_usd_est', 0
        ),

        'tags', jsonb_build_array('alpha','exploratory'),

        'why', jsonb_build_object(
          'decision', 'NOOP',
          'because', jsonb_build_object(
            'primary', 'alpha_observation_only',
            'details', 'Phase 25: alpha produces ideas only; no execution.',
            'blocked_by', (blk.blockers || '["alpha_execution_disabled"]'::jsonb)
          ),
          'to_change_this', jsonb_build_object(
            'counterfactual',
              CASE
                WHEN n.action = 'WOULD_TRADE' THEN 'WOULD_TRADE if gate_ready=true AND execution_enabled=true'
                WHEN n.action = 'WOULD_WATCH' THEN 'WOULD_WATCH if idea repeats >= 2 times in 24h AND data_fresh=true'
                ELSE 'WOULD_SKIP unless gates improve'
              END,
            'min_conditions', jsonb_build_array('execution_enabled=true','gate_ready=true')
          ),
          'next_check', jsonb_build_object('type','SCHEDULED','when','next_cycle')
        )
      )
      FROM blk
    ) AS payload,

    jsonb_build_object(
      'A', n.gate_a,
      'B', n.gate_b,
      'C', n.gate_c,
      'D', n.gate_d,
      'A_raw', n.gate_a_memory_maturity,
      'B_raw', n.gate_b_venue_feasible,
      'C_raw', n.gate_c_fresh_enough,
      'D_raw', n.gate_d_policy_clear
    ) AS gates

  FROM r
),
to_insert AS (
  SELECT
    gen_random_uuid() AS proposal_id,
    (SELECT agent_id FROM params) AS agent_id,
    token,
    venue,
    symbol,
    action,
    notional_usd,
    confidence,

    -- rationale: compact, operator-friendly
    CASE
      WHEN action = 'WOULD_TRADE' THEN
        'CLEAR: all gates pass (A-D).'
      WHEN action = 'WOULD_WATCH' THEN
        'WATCH: ' || COALESCE(array_to_string(blockers, ','), 'needs review')
      ELSE
        'SKIP: ' || COALESCE(array_to_string(blockers, ','), 'blocked')
    END AS rationale,

    gates,
    payload,

    -- dedupe per token/day/action/venue/symbol
    (token || '|' || COALESCE(venue,'') || '|' || COALESCE(symbol,'') || '|' || action || '|' ||
     (SELECT utc_day FROM params))::text AS proposal_hash
  FROM classified
)
INSERT INTO alpha_proposals (
  proposal_id, agent_id, token, venue, symbol, action,
  notional_usd, confidence, rationale, gates, payload, proposal_hash
)
SELECT
  t.proposal_id, t.agent_id, t.token, t.venue, t.symbol, t.action,
  t.notional_usd, t.confidence, t.rationale, t.gates, t.payload, t.proposal_hash
FROM to_insert t
JOIN params p ON 1=1
WHERE p.preview_enabled = 1
  AND NOT EXISTS (SELECT 1 FROM alpha_proposals ap WHERE ap.proposal_hash = t.proposal_hash);
"""
    cur.execute(sql, params)
    try:
        return int(cur.rowcount or 0)
    except Exception:
        return -1


def run_alpha_proposal_runner() -> None:
    """
    Public entrypoint for scheduler.
    """
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        info("alpha_proposal_runner: disabled (PREVIEW_ENABLED and/or ALPHA_PREVIEW_PROPOSALS_ENABLED not set).")
        return

    conn = _connect()
    if conn is None:
        return

    agent_id = os.getenv("ALPHA_AGENT_ID") or os.getenv("AGENT_ID") or "edge-primary"
    params: Dict[str, Any] = {
        "preview_enabled": 1,
        "agent_id": agent_id,
        "default_trade_notional_usd": float(os.getenv("ALPHA_DEFAULT_TRADE_NOTIONAL_USD", "25")),
        "default_trade_confidence": float(os.getenv("ALPHA_DEFAULT_TRADE_CONFIDENCE", "0.10")),
        "default_watch_confidence": float(os.getenv("ALPHA_DEFAULT_WATCH_CONFIDENCE", "0.06")),
        "confidence_cap": float(os.getenv("ALPHA_CONFIDENCE_CAP", "0.25")),
    }

    try:
        cur = conn.cursor()
        _ensure_alpha_proposals_table(cur)

        if not _have_view(cur, "alpha_readiness_v"):
            warn("alpha_proposal_runner: missing view alpha_readiness_v; cannot generate proposals yet.")
            warn("alpha_proposal_runner: deploy the Phase 26 SQL views (alpha_tools / readiness view) first.")
            conn.commit()
            return

        inserted = _run_generator_insert(cur, params)
        conn.commit()

        if inserted == 0:
            info("alpha_proposal_runner: no new proposals inserted (dedupe or empty universe).")
        elif inserted > 0:
            info(f"alpha_proposal_runner: inserted {inserted} proposal rows into alpha_proposals.")
        else:
            info("alpha_proposal_runner: proposal run complete (rowcount unavailable).")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        error(f"alpha_proposal_runner failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
