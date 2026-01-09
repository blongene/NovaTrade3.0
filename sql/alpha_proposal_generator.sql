-- ============================================================
-- alpha_proposal_generator.sql  (Phase 26A, preview-only)
--
-- Purpose:
--   Generate daily Alpha proposals (WOULD_TRADE / WOULD_WATCH / WOULD_SKIP)
--   from alpha_readiness_v. Never executes trades. Pure observation/planning.
--
-- Safety:
--   preview_enabled defaults to 0 => NO INSERTS
--   You must explicitly run with -v preview_enabled=1 to write proposals.
--
-- Example:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f alpha_proposal_generator.sql
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -v preview_enabled=1 -f alpha_proposal_generator.sql
-- ============================================================

\set ON_ERROR_STOP on
\set preview_enabled 0
\set agent_id 'edge-primary'
\set default_trade_notional_usd 25
\set default_trade_confidence 0.10
\set default_watch_confidence 0.06

-- ------------------------------------------------------------
-- 0) Preconditions (friendly checks; will error if missing)
-- ------------------------------------------------------------
DO $$
BEGIN
  -- readiness view must exist
  PERFORM 1 FROM information_schema.views WHERE table_name = 'alpha_readiness_v';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Missing view alpha_readiness_v. Deploy alpha_tools.sql v2 first.';
  END IF;

  -- proposals table must exist
  PERFORM 1 FROM information_schema.tables WHERE table_name = 'alpha_proposals';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Missing table alpha_proposals. Create it (alpha_tools.sql) before running generator.';
  END IF;
END
$$;

-- ------------------------------------------------------------
-- 1) Generate proposals (one per token/day via proposal_hash)
-- ------------------------------------------------------------
WITH params AS (
  SELECT
    :preview_enabled::int AS preview_enabled,
    :'agent_id'::text     AS agent_id,
    :default_trade_notional_usd::numeric AS trade_notional_usd,
    :default_trade_confidence::numeric   AS trade_confidence,
    :default_watch_confidence::numeric   AS watch_confidence,
    to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD')::text AS utc_day
),
r AS (
  SELECT * FROM alpha_readiness_v
),
classified AS (
  SELECT
    r.*,

    -- Blockers (ordered by severity / actionability)
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

    -- Action policy (Phase 26A preview)
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
    c.token,
    COALESCE(c.venue,'')  AS venue,
    COALESCE(c.symbol,'') AS symbol,
    c.action,

    CASE WHEN c.action='WOULD_TRADE' THEN (SELECT trade_notional_usd FROM params) ELSE 0 END AS notional_usd,
    CASE
      WHEN c.action='WOULD_TRADE' THEN (SELECT trade_confidence FROM params)
      WHEN c.action='WOULD_WATCH' THEN (SELECT watch_confidence FROM params)
      ELSE 0
    END AS confidence,

    CASE
      WHEN c.action='WOULD_TRADE' THEN
        ('All gates passed. source=' || COALESCE(NULLIF(c.last_source,''),'unknown') ||
         ' stage=' || COALESCE(NULLIF(c.alpha_stage,''),'unknown'))::text
      WHEN c.action='WOULD_WATCH' THEN
        ('Watch: gates B/C/D pass; Gate A immature. source=' || COALESCE(NULLIF(c.last_source,''),'unknown') ||
         ' stage=' || COALESCE(NULLIF(c.alpha_stage,''),'unknown'))::text
      ELSE
        ('Skip: ' || c.primary_blocker ||
         CASE WHEN c.primary_blocker='POLICY_BLOCK' THEN ' (' || COALESCE(c.gate_d_note,'') || ')' ELSE '' END ||
         CASE WHEN c.primary_blocker='NO_TRADABLE_VENUE' THEN ' (' || COALESCE(c.gate_b_note,'') || ')' ELSE '' END
        )::text
    END AS rationale,

    jsonb_build_object(
      'A', c.gate_a_memory_maturity,
      'B', c.gate_b_venue_feasible,
      'C', c.gate_c_fresh_enough,
      'D', c.gate_d_policy_clear,
      'B_note', c.gate_b_note,
      'D_note', c.gate_d_note,
      'primary_blocker', c.primary_blocker,
      'blockers', c.blockers
    ) AS gates,

    jsonb_build_object(
      'schema', 'Alpha_Proposals.v1',
      'utc_day', (SELECT utc_day FROM params),
      'token', c.token,
      'venue', c.venue,
      'symbol', c.symbol,
      'alpha_stage', c.alpha_stage,
      'last_source', c.last_source,
      'novelty_reason', c.novelty_reason,
      'seen_24h', c.seen_24h,
      'seen_7d', c.seen_7d,
      'distinct_seen_days_7d', c.distinct_seen_days_7d,
      'confirmed_7d', c.confirmed_7d,
      'confirmed_30d', c.confirmed_30d,
      'watch_7d', c.watch_7d,
      'expired_30d', c.expired_30d,
      'demoted_30d', c.demoted_30d,
      'last_seen_ts', c.last_seen_ts
    ) AS payload,

    -- Dedup key: token+venue+symbol+action+UTC day
    (c.token || '|' || COALESCE(c.venue,'') || '|' || COALESCE(c.symbol,'') || '|' || c.action || '|' ||
     (SELECT utc_day FROM params))::text AS proposal_hash
  FROM classified c
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

-- ------------------------------------------------------------
-- 2) Quick report: what would we propose today?
-- ------------------------------------------------------------
WITH today AS (
  SELECT to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD') AS utc_day
),
rows_today AS (
  SELECT *
  FROM alpha_proposals
  WHERE proposal_hash LIKE '%' || (SELECT utc_day FROM today)
)
SELECT
  action,
  COUNT(*) AS n
FROM rows_today
GROUP BY action
ORDER BY n DESC;

SELECT
  ts, token, venue, symbol, action, notional_usd, confidence, rationale
FROM alpha_proposals
ORDER BY ts DESC
LIMIT 25;
