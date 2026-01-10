-- alpha_command_previews.sql — Phase 26D-preview (Outbox enqueue in DRYRUN mode)
-- Logs each translation row_hash -> outbox command id mapping so we never enqueue the same preview twice.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS alpha_command_previews (
  id             BIGSERIAL PRIMARY KEY,
  preview_id     UUID        NOT NULL DEFAULT gen_random_uuid(),
  ts             TIMESTAMPTZ NOT NULL DEFAULT now(),

  translation_id UUID        NOT NULL,
  proposal_id    UUID        NOT NULL,

  token          TEXT        NOT NULL DEFAULT '',
  venue          TEXT        NOT NULL DEFAULT '',
  symbol         TEXT        NOT NULL DEFAULT '',
  action         TEXT        NOT NULL DEFAULT '',

  row_hash       TEXT        NOT NULL,         -- from alpha_translations.row_hash
  outbox_cmd_id  BIGINT      NOT NULL,         -- commands.id (bus outbox)
  intent_hash    TEXT        NOT NULL DEFAULT '',

  intent         JSONB       NOT NULL DEFAULT '{}'::jsonb,
  note           TEXT        NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS alpha_command_previews_row_hash_uq
  ON alpha_command_previews(row_hash);

CREATE INDEX IF NOT EXISTS idx_alpha_command_previews_ts
  ON alpha_command_previews(ts DESC);

CREATE INDEX IF NOT EXISTS idx_alpha_command_previews_cmd_id
  ON alpha_command_previews(outbox_cmd_id);

CREATE OR REPLACE VIEW alpha_command_previews_latest_v AS
SELECT DISTINCT ON (row_hash)
  row_hash, preview_id, ts, translation_id, proposal_id,
  token, venue, symbol, action,
  outbox_cmd_id, intent_hash, intent, note
FROM alpha_command_previews
ORDER BY row_hash, ts DESC;
