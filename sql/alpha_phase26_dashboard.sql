WITH readiness AS (
  SELECT
    token,
    alpha_stage,
    ready_preview,

    gate_A_memory_maturity AS gate_A,
    gate_B_venue_feasible AS gate_B,
    gate_C_fresh_enough    AS gate_C,
    gate_D_policy_clear   AS gate_D,

    gate_B_note,
    gate_D_note,

    seen_7d,
    distinct_seen_days_7d,
    confirmed_30d,
    watch_7d,
    expired_30d,
    demoted_30d,

    last_seen_ts,
    age_since_last_seen
  FROM (
    -- reuse the existing readiness report as a subquery
    SELECT * FROM (
      SELECT * FROM stage
    ) r
  ) t
)

SELECT
  token,
  alpha_stage,
  ready_preview,

  -- Gate summary
  gate_A,
  gate_B,
  gate_C,
  gate_D,

  -- Why blocked (human readable)
  CASE
    WHEN gate_A = 0 THEN 'A: insufficient memory maturity'
    WHEN gate_B = 0 THEN 'B: no tradable venue'
    WHEN gate_C = 0 THEN 'C: stale signal'
    WHEN gate_D = 0 THEN 'D: policy block'
    ELSE 'READY'
  END AS primary_blocker,

  gate_B_note,
  gate_D_note,

  -- Progress indicators
  seen_7d,
  distinct_seen_days_7d,
  confirmed_30d,
  watch_7d,

  last_seen_ts,
  age_since_last_seen

FROM readiness
ORDER BY
  ready_preview DESC,
  gate_A DESC,
  gate_B DESC,
  gate_C DESC,
  gate_D DESC,
  confirmed_30d DESC,
  seen_7d DESC,
  COALESCE(last_seen_ts, 'epoch'::timestamptz) DESC
LIMIT 25;
