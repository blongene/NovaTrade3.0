-- ============================================================
-- alpha_tools.sql  (Phase 25 Safe)
-- Helpers (Gate D) + Preview-only Alpha Proposal Generator
--
-- SAFE DEFAULT: preview_enabled=0 (no proposal generation)
-- Enable one run: psql ... -v preview_enabled=1 -f alpha_tools.sql
-- ============================================================

\set ON_ERROR_STOP on
\set preview_enabled 0
\set default_notional_usd 25
\set default_confidence 0.10
\set agent_id 'edge-primary'

-- ---------- Prereqs ----------
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- Option 2: Helper View + Helper Functions (Policy Blocks)
-- ============================================================

-- Active policy blocks view (used by readiness/proposal logic)
CREATE OR REPLACE VIEW alpha_policy_blocks_active AS
SELECT
  id,
  block_id,
  ts,
  token,
  block_code,
  severity,
  source,
  details,
  expires_at
FROM alpha_policy_blocks
WHERE cleared = 0
  AND (expires_at IS NULL OR expires_at > NOW())
ORDER BY ts DESC;

-- Block a token (returns block_id)
CREATE OR REPLACE FUNCTION alpha_block_token(
  p_token TEXT,
  p_block_code TEXT,
  p_source TEXT DEFAULT 'human',
  p_severity TEXT DEFAULT 'BLOCK',
  p_note TEXT DEFAULT '',
  p_ttl_hours INT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_id UUID := gen_random_uuid();
  v_expires TIMESTAMPTZ := NULL;
BEGIN
  IF p_ttl_hours IS NOT NULL THEN
    v_expires := NOW() + make_interval(hours => p_ttl_hours);
  END IF;

  INSERT INTO alpha_policy_blocks (
    block_id, token, block_code, severity, source, details, expires_at
  )
  VALUES (
    v_id,
    UPPER(TRIM(p_token)),
    UPPER(TRIM(p_block_code)),
    UPPER(TRIM(p_severity)),
    LOWER(TRIM(p_source)),
    CASE
      WHEN p_note IS NULL OR p_note = '' THEN '{}'::jsonb
      ELSE jsonb_build_object('note', p_note)
    END,
    v_expires
  );

  RETURN v_id;
END;
$$;

-- Unblock most recent active block for a token (optionally by code)
-- Returns rows updated (0 or 1 typically)
CREATE OR REPLACE FUNCTION alpha_unblock_token(
  p_token TEXT,
  p_block_code TEXT DEFAULT NULL,
  p_cleared_by TEXT DEFAULT 'human',
  p_reason TEXT DEFAULT 'manual clear'
) RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows INT := 0;
BEGIN
  WITH latest AS (
    SELECT id
    FROM alpha_policy_blocks
    WHERE token = UPPER(TRIM(p_token))
      AND cleared = 0
      AND (expires_at IS NULL OR expires_at > NOW())
      AND (p_block_code IS NULL OR block_code = UPPER(TRIM(p_block_code)))
    ORDER BY ts DESC
    LIMIT 1
  )
  UPDATE alpha_policy_blocks b
  SET
    cleared = 1,
    cleared_ts = NOW(),
    cleared_by = p_cleared_by,
    clear_reason = p_reason
  FROM latest
  WHERE b.id = latest.id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

-- ============================================================
-- Option 3: Preview-only Alpha Proposals
-- ============================================================

-- Ensure proposals table exists (you already created it, so this is safe)
CREATE TABLE IF NOT EXISTS alpha_proposals (
  id              BIGSERIAL PRIMARY KEY,
  proposal_id     UUID        NOT NULL,
  ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  agent_id        TEXT        NOT NULL DEFAULT 'edge-primary',

  token           TEXT        NOT NULL,
  venue           TEXT        NOT NULL DEFAULT '',
  symbol          TEXT        NOT NULL DEFAULT '',
  action          TEXT        NOT NULL DEFAULT 'WOULD_TRADE', -- WOULD_TRADE | WOULD_WATCH | WOULD_SKIP

  notional_usd    NUMERIC     NOT NULL DEFAULT 0,
  confidence      NUMERIC     NOT NULL DEFAULT 0,
  rationale       TEXT        NOT NULL DEFAULT '',

  gates           JSONB       NOT NULL DEFAULT '{}'::jsonb,
  payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,

  proposal_hash   TEXT        NOT NULL DEFAULT '',

  UNIQUE (proposal_id)
);

CREATE INDEX IF NOT EXISTS idx_alpha_proposals_ts ON alpha_proposals (ts DESC);
CREATE INDEX IF NOT EXISTS idx_alpha_proposals_token ON alpha_proposals (token);
CREATE INDEX IF NOT EXISTS idx_alpha_proposals_hash ON alpha_proposals (proposal_hash);

-- ------------------------------------------------------------
-- Preview Proposal Generator
-- (NO-OP unless :preview_enabled = 1)
-- ------------------------------------------------------------
WITH params AS (
  SELECT
    :preview_enabled::int           AS preview_enabled,
    :default_notional_usd::numeric  AS default_notional_usd,
    :default_confidence::numeric    AS default_confidence,
    :'agent_id'::text              AS agent_id
),

-- Readiness components
universe AS (
  SELECT DISTINCT token FROM alpha_ideas
  UNION
  SELECT DISTINCT token FROM alpha_memory
),

last_idea AS (
  SELECT DISTINCT ON (token)
    token,
    ts AS last_idea_ts,
    source AS last_source,
    payload->>'novelty_reason' AS novelty_reason
  FROM alpha_ideas
  ORDER BY token, ts DESC
),

gateB AS (
  SELECT
    token,
    MAX(CASE WHEN tradable = 1 THEN 1 ELSE 0 END) AS venue_feasible,
    STRING_AGG(venue || ':' || symbol, ', ' ORDER BY venue) AS venue_symbols
  FROM alpha_symbol_map
  GROUP BY token
),

gateD AS (
  SELECT
    token,
    CASE WHEN SUM((severity='BLOCK')::int) > 0 THEN 0 ELSE 1 END AS policy_clear,
    STRING_AGG(
      -- IMPORTANT: force everything to TEXT to avoid any JSON parsing/casting issues
      (block_code::text) ||
      CASE
        WHEN COALESCE(details->>'note','') <> '' THEN '(' || (details->>'note')::text || ')'
        ELSE ''
      END,
      ', ' ORDER BY block_code
    ) AS policy_note
  FROM alpha_policy_blocks_active
  GROUP BY token
),

m AS (
  SELECT
    token,
    SUM((event='SEEN' AND ts >= NOW()-INTERVAL '7 days')::int) AS seen_7d,
    COUNT(DISTINCT CASE
      WHEN event='SEEN' AND ts >= NOW()-INTERVAL '7 days'
      THEN (ts AT TIME ZONE 'UTC')::date
    END) AS distinct_seen_days_7d,
    SUM((event='CONFIRMED' AND ts >= NOW()-INTERVAL '30 days')::int) AS confirmed_30d,
    SUM((event='EXPIRED' AND ts >= NOW()-INTERVAL '30 days')::int) AS expired_30d,
    MAX(CASE WHEN event='SEEN' THEN ts END) AS last_seen_ts
  FROM alpha_memory
  GROUP BY token
),

readiness AS (
  SELECT
    u.token,
    COALESCE(li.last_source,'') AS last_source,
    COALESCE(li.novelty_reason,'') AS novelty_reason,

    -- Gate A: memory maturity (tune later; conservative by design)
    CASE
      WHEN COALESCE(m.seen_7d,0) >= 5
       AND COALESCE(m.distinct_seen_days_7d,0) >= 3
       AND COALESCE(m.confirmed_30d,0) >= 2
       AND COALESCE(m.expired_30d,0) = 0
      THEN 1 ELSE 0
    END AS gate_A,

    -- Gate B
    COALESCE(gb.venue_feasible,0) AS gate_B,
    COALESCE(gb.venue_symbols,'NONE') AS gate_B_note,

    -- Gate C: freshness
    CASE
      WHEN m.last_seen_ts IS NOT NULL
       AND m.last_seen_ts >= NOW() - INTERVAL '7 days'
      THEN 1 ELSE 0
    END AS gate_C,

    -- Gate D: policy clear
    COALESCE(gd.policy_clear,1) AS gate_D,
    COALESCE(gd.policy_note,'CLEAR') AS gate_D_note
  FROM universe u
  LEFT JOIN last_idea li USING (token)
  LEFT JOIN m USING (token)
  LEFT JOIN gateB gb USING (token)
  LEFT JOIN gateD gd USING (token)
  WHERE u.token IS NOT NULL AND u.token <> ''
),

-- Choose preferred venue+symbol (COINBASE > BINANCEUS > any tradable)
pick_symbol AS (
  SELECT
    r.token,
    COALESCE(
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='COINBASE'  AND s.tradable=1 LIMIT 1),
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='BINANCEUS' AND s.tradable=1 LIMIT 1),
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=r.token AND s.tradable=1 ORDER BY venue LIMIT 1),
      ''
    ) AS symbol,
    COALESCE(
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='COINBASE'  AND s.tradable=1 LIMIT 1),
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='BINANCEUS' AND s.tradable=1 LIMIT 1),
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=r.token AND s.tradable=1 ORDER BY venue LIMIT 1),
      ''
    ) AS venue
  FROM readiness r
),

eligible AS (
  SELECT
    r.*,
    ps.venue,
    ps.symbol
  FROM readiness r
  JOIN pick_symbol ps USING (token)
  WHERE r.gate_A=1 AND r.gate_B=1 AND r.gate_C=1 AND r.gate_D=1
),

to_insert AS (
  SELECT
    gen_random_uuid() AS proposal_id,
    (SELECT agent_id FROM params) AS agent_id,
    e.token,
    e.venue,
    e.symbol,
    'WOULD_TRADE'::text AS action,

    (SELECT default_notional_usd FROM params) AS notional_usd,
    (SELECT default_confidence FROM params) AS confidence,

    ('Preview-only: all readiness gates passed. Source=' || e.last_source)::text AS rationale,

    jsonb_build_object(
      'A', e.gate_A,
      'B', e.gate_B,
      'C', e.gate_C,
      'D', e.gate_D,
      'B_note', e.gate_B_note,
      'D_note', e.gate_D_note
    ) AS gates,

    jsonb_build_object(
      'token', e.token,
      'venue', e.venue,
      'symbol', e.symbol,
      'notional_usd', (SELECT default_notional_usd FROM params),
      'confidence', (SELECT default_confidence FROM params),
      'novelty_reason', e.novelty_reason
    ) AS payload,

    (e.token || '|' || e.venue || '|' || e.symbol || '|WOULD_TRADE|' ||
     to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD'))::text AS proposal_hash
  FROM eligible e
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
  AND NOT EXISTS (
    SELECT 1 FROM alpha_proposals ap
    WHERE ap.proposal_hash = t.proposal_hash
  );

-- Optional: show a tiny status line when preview_enabled=1
-- (psql prints INSERT count anyway)
