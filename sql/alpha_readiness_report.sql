WITH universe AS (
  SELECT DISTINCT token FROM alpha_ideas
  UNION
  SELECT DISTINCT token FROM alpha_memory
),

last_idea AS (
  SELECT DISTINCT ON (token)
    token,
    idea_id AS last_idea_id,
    ts AS last_idea_ts,
    source AS last_source,
    payload->>'novelty_reason' AS novelty_reason
  FROM alpha_ideas
  ORDER BY token, ts DESC
),

/* Gate B: Venue feasibility (real) */
gateB AS (
  SELECT
    token,
    MAX(CASE WHEN tradable = 1 THEN 1 ELSE 0 END) AS venue_feasible,
    STRING_AGG(venue || ':' || symbol, ', ' ORDER BY venue) AS venue_symbols
  FROM alpha_symbol_map
  GROUP BY token
),

/* Aggregate memory metrics */
m AS (
  SELECT
    token,

    -- activity
    SUM((event='SEEN' AND ts >= NOW()-INTERVAL '24 hours')::int) AS seen_24h,
    SUM((event='SEEN' AND ts >= NOW()-INTERVAL '7 days')::int)   AS seen_7d,
    SUM((event='SEEN' AND ts >= NOW()-INTERVAL '30 days')::int)  AS seen_30d,

    COUNT(DISTINCT CASE
      WHEN event='SEEN' AND ts >= NOW()-INTERVAL '7 days'
      THEN (ts AT TIME ZONE 'UTC')::date
      END
    ) AS distinct_seen_days_7d,

    -- confirmations / watch / decay
    SUM((event='CONFIRMED' AND ts >= NOW()-INTERVAL '7 days')::int)  AS confirmed_7d,
    SUM((event='CONFIRMED' AND ts >= NOW()-INTERVAL '30 days')::int) AS confirmed_30d,
    SUM((event='PROMOTED_TO_WATCH' AND ts >= NOW()-INTERVAL '7 days')::int) AS watch_7d,
    SUM((event='EXPIRED' AND ts >= NOW()-INTERVAL '30 days')::int) AS expired_30d,
    SUM((event='DEMOTED' AND ts >= NOW()-INTERVAL '30 days')::int) AS demoted_30d,

    -- timestamps
    MAX(CASE WHEN event='SEEN' THEN ts END) AS last_seen_ts,
    MAX(CASE WHEN event='CONFIRMED' THEN ts END) AS last_confirmed_ts,
    MAX(CASE WHEN event='PROMOTED_TO_WATCH' THEN ts END) AS last_watch_ts,
    MAX(CASE WHEN event='EXPIRED' THEN ts END) AS last_expired_ts

  FROM alpha_memory
  GROUP BY token
),

base AS (
  SELECT
    u.token,
    li.last_idea_id,
    li.last_idea_ts,
    li.last_source,
    li.novelty_reason,

    COALESCE(m.seen_24h,0) AS seen_24h,
    COALESCE(m.seen_7d,0) AS seen_7d,
    COALESCE(m.seen_30d,0) AS seen_30d,
    COALESCE(m.distinct_seen_days_7d,0) AS distinct_seen_days_7d,

    COALESCE(m.confirmed_7d,0) AS confirmed_7d,
    COALESCE(m.confirmed_30d,0) AS confirmed_30d,
    COALESCE(m.watch_7d,0) AS watch_7d,
    COALESCE(m.expired_30d,0) AS expired_30d,
    COALESCE(m.demoted_30d,0) AS demoted_30d,

    m.last_seen_ts,
    (NOW() - m.last_seen_ts) AS age_since_last_seen,
    m.last_confirmed_ts,
    m.last_watch_ts,
    m.last_expired_ts
  FROM universe u
  LEFT JOIN last_idea li USING (token)
  LEFT JOIN m USING (token)
  WHERE u.token IS NOT NULL AND u.token <> ''
),

gates AS (
  SELECT
    b.*,

    /* Gate A: memory maturity (tune thresholds as desired) */
    CASE
      WHEN b.seen_7d >= 5
       AND b.distinct_seen_days_7d >= 3
       AND b.confirmed_30d >= 2
       AND b.expired_30d = 0
      THEN 1 ELSE 0
    END AS gate_A_memory_maturity,

    /* Gate B: venue feasibility (REAL now) */
    COALESCE(gb.venue_feasible, 0) AS gate_B_venue_feasible,
    COALESCE(gb.venue_symbols, 'NONE')::text AS gate_B_note,

    /* Gate C: data freshness (simple: has been seen within 7d) */
    CASE
      WHEN b.last_seen_ts IS NOT NULL
       AND b.last_seen_ts >= NOW() - INTERVAL '7 days'
      THEN 1 ELSE 0
    END AS gate_C_fresh_enough,

    /* Gate D: policy safety (still placeholder until we add policy-by-token blocks) */
    0 AS gate_D_policy_clear,
    'UNKNOWN'::text AS gate_D_note,

    /* Gate E: capital discipline (preview-only; never enables execution here) */
    1 AS gate_E_capital_preview_ok,

    /* Gate F: human sovereignty (always required) */
    1 AS gate_F_human_required

  FROM base b
  LEFT JOIN gateB gb USING (token)
),

stage AS (
  SELECT
    g.*,

    CASE
      WHEN g.expired_30d > 0 THEN 'BLOCKED'
      WHEN g.confirmed_30d > 0 THEN 'CONFIRMED'
      WHEN g.watch_7d > 0 THEN 'WATCH'
      WHEN g.seen_7d > 0 THEN 'IDEA'
      ELSE 'IDEA'
    END AS alpha_stage,

    CASE
      WHEN g.gate_A_memory_maturity = 1
       AND g.gate_B_venue_feasible = 1
       AND g.gate_C_fresh_enough = 1
       AND g.gate_D_policy_clear = 1
      THEN 1 ELSE 0
    END AS ready_preview
  FROM gates g
)

SELECT
  token,
  alpha_stage,
  ready_preview,

  -- key metrics
  seen_24h,
  seen_7d,
  distinct_seen_days_7d,
  confirmed_7d,
  confirmed_30d,
  watch_7d,
  expired_30d,
  demoted_30d,
  last_seen_ts,
  age_since_last_seen,
  last_source,
  novelty_reason,

  -- gates
  gate_A_memory_maturity,
  gate_B_venue_feasible,
  gate_B_note,
  gate_C_fresh_enough,
  gate_D_policy_clear,
  gate_D_note,
  gate_E_capital_preview_ok,
  gate_F_human_required

FROM stage
ORDER BY
  ready_preview DESC,
  confirmed_30d DESC,
  watch_7d DESC,
  seen_24h DESC,
  COALESCE(last_seen_ts, 'epoch'::timestamptz) DESC
LIMIT 50;
