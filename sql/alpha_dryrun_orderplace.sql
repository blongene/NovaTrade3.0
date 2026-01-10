-- alpha_dryrun_orderplace.sql — Phase 26E (dryrun order.place BUY/SELL)
-- Records which translations have been enqueued as dryrun order.place commands.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS alpha_dryrun_orderplace_outbox (
  id              BIGSERIAL PRIMARY KEY,
  outbox_id       UUID        NOT NULL DEFAULT gen_random_uuid(),
  ts              TIMESTAMPTZ NOT NULL DEFAULT now(),

  translation_id  UUID        NOT NULL,
  proposal_id     UUID        NOT NULL,

  token           TEXT        NOT NULL DEFAULT '',
  venue           TEXT        NOT NULL DEFAULT '',
  symbol          TEXT        NOT NULL DEFAULT '',
  side            TEXT        NOT NULL DEFAULT '',

  cmd_id          BIGINT      NOT NULL,
  intent_hash     TEXT        NOT NULL DEFAULT '',
  intent          JSONB       NOT NULL DEFAULT '{}'::jsonb,

  note            TEXT        NOT NULL DEFAULT ''
);

-- Prevent duplicate enqueues for the same translation
CREATE UNIQUE INDEX IF NOT EXISTS alpha_dryrun_orderplace_outbox_translation_uq
  ON alpha_dryrun_orderplace_outbox(translation_id);

CREATE INDEX IF NOT EXISTS idx_alpha_dryrun_orderplace_outbox_ts
  ON alpha_dryrun_orderplace_outbox(ts DESC);

CREATE INDEX IF NOT EXISTS idx_alpha_dryrun_orderplace_outbox_cmd_id
  ON alpha_dryrun_orderplace_outbox(cmd_id);

CREATE OR REPLACE VIEW alpha_dryrun_orderplace_outbox_v AS
SELECT
  ts, translation_id, proposal_id, token, venue, symbol, side,
  cmd_id, intent_hash, note, intent
FROM alpha_dryrun_orderplace_outbox
ORDER BY ts DESC;
