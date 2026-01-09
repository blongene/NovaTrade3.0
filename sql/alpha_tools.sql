-- ============================================================
-- alpha_tools.sql (Phase 25/26 SAFE)
-- - Gate D helpers (policy blocks) + active view
-- - Readiness view: alpha_readiness_v  (queryable)
-- - Proposal generator: alpha_proposals (WOULD_TRADE/WATCH/SKIP)
--
-- SAFE DEFAULT: preview_enabled=0 (NO inserts into alpha_proposals)
-- To generate proposals intentionally:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -v preview_enabled=1 -f alpha_tools.sql
-- ============================================================

\set ON_ERROR_STOP on
\set preview_enabled 0
\set agent_id 'edge-primary'
\set default_trade_notional_usd 25
\set default_trade_confidence 0.10
\set default_watch_confidence 0.06

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- Gate D: Policy blocks (active view + helpers)
-- ============================================================

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
  expires_at,
  cleared
FROM alpha_policy_blocks
WHERE cleared = 0
  AND (expires_at IS NULL OR expires_at > NOW())
ORDER BY ts DESC;

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
    block_id, token, block_code, severity, source, details, expires_at, cleared
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
    v_expires,
    0
  );

  RETURN v_id;
END;
$$;

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
-- Option 3 foundation: Readiness view (queryable anytime)
-- ============================================================

CREATE OR REPLACE VIEW alpha_readiness_v AS
WITH
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
    SUM((event='SEEN' AND ts >= NOW()-INTERVAL '24 hours')::int) AS seen_24h,
    SUM((event='SEEN' AND ts >= NOW()-INTERVAL '7 days')::int) AS seen_7d,
    COUNT(DISTINCT CASE
      WHEN event='SEEN' AND ts >= NOW()-INTERVAL '7 days'
      THEN (ts AT TIME ZONE 'UTC')::date
    END) AS distinct_seen_days_7d,
    SUM((event='CONFIRMED' AND ts >= NOW()-INTERVAL '7 days')::int) AS confirmed_7d,
    SUM((event='CONFIRMED' AND ts >= NOW()-INTERVAL '30 days')::int) AS confirmed_30d,
    SUM((event='PROMOTED_TO_WATCH' AND ts >= NOW()-INTERVAL '7 days')::int) AS watch_7d,
    SUM((event='EXPIRED' AND ts >= NOW()-INTERVAL '30 days')::int) AS expired_30d,
    SUM((event='DEMOTED' AND ts >= NOW()-INTERVAL '30 days')::int) AS demoted_30d,
    MAX(CASE WHEN event='SEEN' THEN ts END) AS last_seen_ts
  FROM alpha_memory
  GROUP BY token
),
pick_symbol AS (
  SELECT
    u.token,
    COALESCE(
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=u.token AND s.venue='COINBASE'  AND s.tradable=1 LIMIT 1),
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=u.token AND s.venue='BINANCEUS' AND s.tradable=1 LIMIT 1),
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=u.token AND s.tradable=1 ORDER BY venue LIMIT 1),
      ''
    ) AS symbol,
    COALESCE(
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=u.token AND s.venue='COINBASE'  AND s.tradable=1 LIMIT 1),
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=u.token AND s.venue='BINANCEUS' AND s.tradable=1 LIMIT 1),
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=u.token AND s.tradable=1 ORDER BY venue LIMIT 1),
      ''
    ) AS venue
  FROM universe u
)
SELECT
  u.token,

  -- stage hint (for dashboards)
  CASE
    WHEN COALESCE(m.confirmed_30d,0) > 0 THEN 'CONFIRMED'
    WHEN COALESCE(m.watch_7d,0) > 0 THEN 'WATCH'
    WHEN COALESCE(m.seen_7d,0) > 0 THEN 'SEEN'
    ELSE 'NEW'
  END AS alpha_stage,

  -- activity
  COALESCE(m.seen_24h,0) AS seen_24h,
  COALESCE(m.seen_7d,0) AS seen_7d,
  COALESCE(m.distinct_seen_days_7d,0) AS distinct_seen_days_7d,
  COALESCE(m.confirmed_7d,0) AS confirmed_7d,
  COALESCE(m.confirmed_30d,0) AS confirmed_30d,
  COALESCE(m.watch_7d,0) AS watch_7d,
  COALESCE(m.expired_30d,0) AS expired_30d,
  COALESCE(m.demoted_30d,0) AS demoted_30d,

  -- freshness
  m.last_seen_ts,
  CASE WHEN m.last_seen_ts IS NULL THEN NULL ELSE (NOW() - m.last_seen_ts) END AS age_since_last_seen,

  -- idea context
  li.last_idea_ts,
  COALESCE(li.last_source,'') AS last_source,
  COALESCE(li.novelty_reason,'') AS novelty_reason,

  -- pick venue/symbol
  ps.venue,
  ps.symbol,

  -- Gate A (strict, conservative by design; tune later)
  CASE
    WHEN COALESCE(m.seen_7d,0) >= 5
     AND COALESCE(m.distinct_seen_days_7d,0) >= 3
     AND COALESCE(m.confirmed_30d,0) >= 2
     AND COALESCE(m.expired_30d,0) = 0
    THEN 1 ELSE 0
  END AS gate_a_memory_maturity,

  -- Gate B (real)
  COALESCE(gb.venue_feasible,0) AS gate_b_venue_feasible,
  COALESCE(gb.venue_symbols,'NONE') AS gate_b_note,

  -- Gate C (fresh enough)
  CASE
    WHEN m.last_seen_ts IS NOT NULL
     AND m.last_seen_ts >= NOW() - INTERVAL '7 days'
    THEN 1 ELSE 0
  END AS gate_c_fresh_enough,

  -- Gate D (real)
  COALESCE(gd.policy_clear,1) AS gate_d_policy_clear,
  COALESCE(gd.policy_note,'CLEAR') AS gate_d_note

FROM universe u
LEFT JOIN m USING (token)
LEFT JOIN last_idea li USING (token)
LEFT JOIN gateB gb USING (token)
LEFT JOIN gateD gd USING (token)
LEFT JOIN pick_symbol ps USING (token)
WHERE u.token IS NOT NULL AND u.token <> '';

-- ============================================================
-- Option 2: Proposal table (you already created; safe to re-run)
-- ============================================================

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

CREATE INDEX IF NOT EXISTS idx_alpha_proposals_ts   ON alpha_proposals (ts DESC);
CREATE INDEX IF NOT EXISTS idx_alpha_proposals_token ON alpha_proposals (token);
CREATE INDEX IF NOT EXISTS idx_alpha_proposals_hash  ON alpha_proposals (proposal_hash);

-- ============================================================
-- Proposal Generator (runs only when preview_enabled=1)
-- Inserts one proposal per token per day (dedup via proposal_hash)
-- ============================================================

WITH params AS (
  SELECT
    :preview_enabled::int AS preview_enabled,
    :'agent_id'::text     AS agent_id,
    :default_trade_notional_usd::numeric AS trade_notional_usd,
    :default_trade_confidence::numeric   AS trade_confidence,
    :default_watch_confidence::numeric   AS watch_confidence
),
r AS (
  SELECT * FROM alpha_readiness_v
),
classified AS (
  SELECT
    r.*,

    -- derive blockers list
    ARRAY_REMOVE(ARRAY[
      CASE WHEN r.gate_b_venue_feasible = 0 THEN 'NO_TRADABLE_VENUE' END,
      CASE WHEN r.gate_d_policy_clear  = 0 THEN 'POLICY_BLOCK' END,
      CASE WHEN r.gate_c_fresh_enough  = 0 THEN 'STALE' END,
      CASE WHEN r.gate_a_memory_maturity = 0 THEN 'IMMATURE' END
    ], NULL) AS blockers,

    CASE
      WHEN r.gate_b_venue_feasible = 0 THEN 'NO_TRADABLE_VENUE'
      WHEN r.gate_d_policy_clear  = 0 THEN 'POLICY_BLOCK'
      WHEN r.gate_c_fresh_enough  = 0 THEN 'STALE'
      WHEN r.gate_a_memory_maturity = 0 THEN 'IMMATURE'
      ELSE 'CLEAR'
    END AS primary_blocker,

    CASE
      WHEN r.gate_a_memory_maturity=1 AND r.gate_b_venue_feasible=1 AND r.gate_c_fresh_enough=1 AND r.gate_d_policy_clear=1
        THEN 'WOULD_TRADE'
      WHEN r.gate_b_venue_feasible=1 AND r.gate_c_fresh_enough=1 AND r.gate_d_policy_clear=1
        THEN 'WOULD_WATCH'
      ELSE 'WOULD_SKIP'
    END AS action
  FROM r
),
to_insert AS (
  SELECT
    gen_random_uuid() AS proposal_id,
    (SELECT agent_id FROM params) AS agent_id,

    token,
    COALESCE(venue,'') AS venue,
    COALESCE(symbol,'') AS symbol,
    action,

    CASE WHEN action='WOULD_TRADE' THEN (SELECT trade_notional_usd FROM params) ELSE 0 END AS notional_usd,
    CASE
      WHEN action='WOULD_TRADE' THEN (SELECT trade_confidence FROM params)
      WHEN action='WOULD_WATCH' THEN (SELECT watch_confidence FROM params)
      ELSE 0
    END AS confidence,

    CASE
      WHEN action='WOULD_TRADE' THEN ('All gates passed. Source=' || NULLIF(last_source,'') )::text
      WHEN action='WOULD_WATCH' THEN ('Promising but not mature yet (Gate A). Source=' || NULLIF(last_source,'') )::text
      ELSE ('Blocked: ' || primary_blocker)::text
    END AS rationale,

    jsonb_build_object(
      'A', gate_a_memory_maturity,
      'B', gate_b_venue_feasible,
      'C', gate_c_fresh_enough,
      'D', gate_d_policy_clear,
      'B_note', gate_b_note,
      'D_note', gate_d_note,
      'primary_blocker', primary_blocker,
      'blockers', blockers
    ) AS gates,

    jsonb_build_object(
      'token', token,
      'venue', venue,
      'symbol', symbol,
      'alpha_stage', alpha_stage,
      'last_source', last_source,
      'novelty_reason', novelty_reason,
      'seen_24h', seen_24h,
      'seen_7d', seen_7d,
      'distinct_seen_days_7d', distinct_seen_days_7d,
      'confirmed_30d', confirmed_30d,
      'watch_7d', watch_7d
    ) AS payload,

    (token || '|' || COALESCE(venue,'') || '|' || COALESCE(symbol,'') || '|' || action || '|' ||
     to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD'))::text AS proposal_hash
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

-- ============================================================
-- End
-- ============================================================
