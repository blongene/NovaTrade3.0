-- ============================================================
-- Alpha Tools (Phase 25 Safe)
-- Helpers + Preview-only Proposal Generator
-- ============================================================

-- ---------- Prereqs ----------
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- Option 2: Helper Functions (Policy Blocks)
-- ============================================================

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

-- Active policy blocks view
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

-- ============================================================
-- Option 3: Preview-only Alpha Proposal Generator
-- ============================================================

-- SAFE DEFAULTS:
--   preview_enabled = 0  (generator does NOTHING)
--   flip to 1 only for intentional preview runs

WITH params AS (
  SELECT
    0::int      AS preview_enabled,     -- <<< SET TO 1 TO GENERATE
    25::numeric AS default_notional_usd,
    0.10::numeric AS default_confidence
),

-- ---------------- Readiness ----------------
readiness AS (
  WITH universe AS (
    SELECT DISTINCT token FROM alpha_ideas
    UNION
    SELECT DISTINCT token FROM alpha_memory
  ),
  last_idea AS (
    SELECT DISTINCT ON (token)
      token,
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
        block_code ||
        COALESCE(
          CASE WHEN details ? 'note'
            THEN '(' || details->>'note' || ')'
            ELSE ''
          END,
          ''
        ),
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
  )
  SELECT
    u.token,
    COALESCE(li.last_source,'') AS last_source,
    COALESCE(li.novelty_reason,'') AS novelty_reason,

    -- Gate A
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

    -- Gate C
    CASE
      WHEN m.last_seen_ts IS NOT NULL
       AND m.last_seen_ts >= NOW() - INTERVAL '7 days'
      THEN 1 ELSE 0
    END AS gate_C,

    -- Gate D
    COALESCE(gd.policy_clear,1) AS gate_D,
    COALESCE(gd.policy_note,'CLEAR') AS gate_D_note

  FROM universe u
  LEFT JOIN last_idea li USING (token)
  LEFT JOIN m USING (token)
  LEFT JOIN gateB gb USING (token)
  LEFT JOIN gateD gd USING (token)
  WHERE u.token IS NOT NULL AND u.token <> ''
),

-- ---------------- Pick venue/symbol ----------------
pick_symbol AS (
  SELECT
    r.token,
    COALESCE(
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='COINBASE'  AND s.tradable=1 LIMIT 1),
      (SELECT symbol FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='BINANCEUS' AND s.tradable=1 LIMIT 1),
      ''
    ) AS symbol,
    COALESCE(
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='COINBASE'  AND s.tradable=1 LIMIT 1),
      (SELECT venue FROM alpha_symbol_map s WHERE s.token=r.token AND s.venue='BINANCEUS' AND s.tradable=1 LIMIT 1),
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
    'edge-primary'::text AS agent_id,
    e.token,
    e.venue,
    e.symbol,
    'WOULD_TRADE'::text AS action,

    (SELECT default_notional_usd FROM params) AS notional_usd,
    (SELECT default_confidence FROM params) AS confidence,

    ('Preview-only: all readiness gates passed. Source=' || e.last_source) AS rationale,

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
     to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD')) AS proposal_hash
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
