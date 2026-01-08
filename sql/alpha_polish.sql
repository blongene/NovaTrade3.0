-- Daily Alpha Digest (Phase 25 safe)
-- Shows: top SEEN tokens (24h/7d), promotions in last 24h, expirations in last 7d

WITH
seen_24h AS (
  SELECT token, COUNT(*) AS seen_24h
  FROM alpha_memory
  WHERE event = 'SEEN'
    AND ts >= NOW() - INTERVAL '24 hours'
  GROUP BY token
),
seen_7d AS (
  SELECT token, COUNT(*) AS seen_7d
  FROM alpha_memory
  WHERE event = 'SEEN'
    AND ts >= NOW() - INTERVAL '7 days'
  GROUP BY token
),
confirmed_7d AS (
  SELECT token, COUNT(*) AS confirmed_7d
  FROM alpha_memory
  WHERE event = 'CONFIRMED'
    AND ts >= NOW() - INTERVAL '7 days'
  GROUP BY token
),
watch_24h AS (
  SELECT token, COUNT(*) AS promoted_24h, MAX(ts) AS last_promoted_ts
  FROM alpha_memory
  WHERE event = 'PROMOTED_TO_WATCH'
    AND ts >= NOW() - INTERVAL '24 hours'
  GROUP BY token
),
expired_7d AS (
  SELECT token, COUNT(*) AS expired_7d, MAX(ts) AS last_expired_ts
  FROM alpha_memory
  WHERE event = 'EXPIRED'
    AND ts >= NOW() - INTERVAL '7 days'
  GROUP BY token
),
last_idea AS (
  SELECT DISTINCT ON (token)
    token,
    ts AS last_idea_ts,
    source AS last_source,
    payload->>'novelty_reason' AS novelty_reason
  FROM alpha_ideas
  ORDER BY token, ts DESC
)

-- 1) Summary table: top tokens by activity (24h priority)
SELECT
  COALESCE(s24.token, s7.token, c7.token, w24.token, e7.token, li.token) AS token,
  COALESCE(s24.seen_24h, 0) AS seen_24h,
  COALESCE(s7.seen_7d, 0) AS seen_7d,
  COALESCE(c7.confirmed_7d, 0) AS confirmed_7d,
  COALESCE(w24.promoted_24h, 0) AS promoted_24h,
  COALESCE(e7.expired_7d, 0) AS expired_7d,
  li.last_idea_ts,
  li.last_source,
  li.novelty_reason
FROM seen_24h s24
FULL OUTER JOIN seen_7d s7 USING (token)
FULL OUTER JOIN confirmed_7d c7 USING (token)
FULL OUTER JOIN watch_24h w24 USING (token)
FULL OUTER JOIN expired_7d e7 USING (token)
FULL OUTER JOIN last_idea li USING (token)
ORDER BY
  COALESCE(s24.seen_24h, 0) DESC,
  COALESCE(w24.promoted_24h, 0) DESC,
  COALESCE(s7.seen_7d, 0) DESC,
  COALESCE(c7.confirmed_7d, 0) DESC,
  COALESCE(li.last_idea_ts, 'epoch'::timestamptz) DESC
LIMIT 25;

-- 2) Promotions in the last 24 hours (detail)
SELECT
  ts,
  token,
  reason_code,
  facts
FROM alpha_memory
WHERE event = 'PROMOTED_TO_WATCH'
  AND ts >= NOW() - INTERVAL '24 hours'
ORDER BY ts DESC
LIMIT 50;

-- 3) Expired in the last 7 days (detail)
SELECT
  ts,
  token,
  reason_code,
  facts
FROM alpha_memory
WHERE event = 'EXPIRED'
  AND ts >= NOW() - INTERVAL '7 days'
ORDER BY ts DESC
LIMIT 50;
