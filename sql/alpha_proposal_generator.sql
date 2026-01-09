-- ============================================================
-- alpha_proposal_generator.sql  (Phase 26A, preview-only)
-- Generates DAILY proposals for ALL tokens:
--   WOULD_TRADE / WOULD_WATCH / WOULD_SKIP
--
-- SAFE DEFAULT: preview_enabled=0 => NO INSERTS
-- To write proposals: -v preview_enabled=1
-- ============================================================

\set ON_ERROR_STOP on
\if :{?preview_enabled}
  \echo 'preview_enabled provided: ' :preview_enabled
\else
  \set preview_enabled 0
  \echo 'preview_enabled defaulting to 0'
\endif
\set agent_id 'edge-primary'
\set default_trade_notional_usd 25
\set default_trade_confidence 0.10
\set default_watch_confidence 0.06

DO $$
BEGIN
  PERFORM 1 FROM information_schema.views WHERE table_name = 'alpha_readiness_v';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Missing view alpha_readiness_v. Deploy alpha_tools.sql first.';
  END IF;

  PERFORM 1 FROM information_schema.tables WHERE table_name = 'alpha_proposals';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Missing table alpha_proposals. Create it before running generator.';
  END IF;
END
$$;

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
  WHERE r.token IS NOT NULL AND r.token <> ''
),
to_insert AS (
  SELECT
    gen_random_uuid() AS proposal_id,
    (SELECT agent_id FROM params) AS agent_id,

    token,
    COALESCE(venue,'')  AS venue,
    COALESCE(symbol,'') AS symbol,
    action,

    CASE WHEN action='WOULD_TRADE' THEN (SELECT trade_notional_usd FROM params) ELSE 0 END AS notional_usd,
    CASE
      WHEN action='WOULD_TRADE' THEN (SELECT trade_confidence FROM params)
      WHEN action='WOULD_WATCH' THEN (SELECT watch_confidence FROM params)
      ELSE 0
    END AS confidence,

    CASE
      WHEN action='WOULD_TRADE' THEN
        ('All gates passed. source=' || COALESCE(NULLIF(last_source,''),'unknown') || ' stage=' || alpha_stage)::text
      WHEN action='WOULD_WATCH' THEN
        ('Watch: B/C/D pass; Gate A immature. source=' || COALESCE(NULLIF(last_source,''),'unknown') || ' stage=' || alpha_stage)::text
      ELSE
        ('Skip: ' || primary_blocker ||
          CASE WHEN primary_blocker='POLICY_BLOCK' THEN ' (' || COALESCE(gate_d_note,'') || ')' ELSE '' END ||
          CASE WHEN primary_blocker='NO_TRADABLE_VENUE' THEN ' (' || COALESCE(gate_b_note,'') || ')' ELSE '' END
        )::text
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
      'schema', 'Alpha_Proposals.v1',
      'utc_day', (SELECT utc_day FROM params),
      'token', token,
      'venue', venue,
      'symbol', symbol,
      'alpha_stage', alpha_stage,
      'last_source', last_source,
      'novelty_reason', novelty_reason,
      'seen_24h', seen_24h,
      'seen_7d', seen_7d,
      'distinct_seen_days_7d', distinct_seen_days_7d,
      'confirmed_7d', confirmed_7d,
      'confirmed_30d', confirmed_30d,
      'watch_7d', watch_7d,
      'expired_30d', expired_30d,
      'demoted_30d', demoted_30d,
      'last_seen_ts', last_seen_ts
    ) AS payload,

    -- Dedup key (one proposal per token+action per UTC day)
    (token || '|' || action || '|' || (SELECT utc_day FROM params))::text AS proposal_hash
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
  AND NOT EXISTS (
    SELECT 1 FROM alpha_proposals ap
    WHERE ap.proposal_hash = t.proposal_hash
  );

-- Quick summary (today)
WITH today AS (
  SELECT to_char((NOW() AT TIME ZONE 'UTC')::date,'YYYY-MM-DD') AS utc_day
)
SELECT action, COUNT(*) AS n
FROM alpha_proposals
WHERE proposal_hash LIKE '%|' || (SELECT utc_day FROM today)
GROUP BY action
ORDER BY n DESC;

SELECT ts, token, venue, symbol, action, notional_usd, confidence, rationale
FROM alpha_proposals
ORDER BY ts DESC
LIMIT 25;
