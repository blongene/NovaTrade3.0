-- alpha_translations.sql — Phase 26C (Translation Preview, preview-only)
-- Creates append-only translation preview artifacts that resemble commands, but are NOT enqueued.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS alpha_translations (
  id              BIGSERIAL PRIMARY KEY,
  translation_id  UUID        NOT NULL DEFAULT gen_random_uuid(),
  ts              TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Source proposal linkage
  proposal_id     UUID        NOT NULL,
  proposal_hash   TEXT        NOT NULL DEFAULT '',

  -- Human approval linkage (latest decision at time of translation)
  approval_decision TEXT      NOT NULL DEFAULT '',
  approval_actor    TEXT      NOT NULL DEFAULT '',
  approval_note     TEXT      NOT NULL DEFAULT '',

  -- Copy-forward proposal basics
  agent_id        TEXT        NOT NULL DEFAULT 'edge-primary',
  token           TEXT        NOT NULL DEFAULT '',
  venue           TEXT        NOT NULL DEFAULT '',
  symbol          TEXT        NOT NULL DEFAULT '',
  action          TEXT        NOT NULL DEFAULT '',  -- e.g. WOULD_WATCH / WOULD_TRADE
  notional_usd    NUMERIC     NOT NULL DEFAULT 0,
  confidence      NUMERIC     NOT NULL DEFAULT 0,
  rationale       TEXT        NOT NULL DEFAULT '',

  gates           JSONB       NOT NULL DEFAULT '{}'::jsonb,
  payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,

  -- The preview "command-like" artifact (never executed here)
  command_preview JSONB       NOT NULL DEFAULT '{}'::jsonb,

  -- Idempotency (same source+decision yields same row_hash)
  row_hash        TEXT        NOT NULL DEFAULT ''
);

-- Uniqueness: allow re-translation if inputs change; prevent dup inserts for exact same translation intent
CREATE UNIQUE INDEX IF NOT EXISTS alpha_translations_row_hash_uq ON alpha_translations(row_hash);
CREATE INDEX IF NOT EXISTS idx_alpha_translations_ts ON alpha_translations(ts DESC);
CREATE INDEX IF NOT EXISTS idx_alpha_translations_proposal_id ON alpha_translations(proposal_id);
CREATE INDEX IF NOT EXISTS idx_alpha_translations_token ON alpha_translations(token);

-- Latest view per proposal (most recent translation row)
CREATE OR REPLACE VIEW alpha_translations_latest_v AS
SELECT DISTINCT ON (proposal_id)
  proposal_id, translation_id, ts,
  approval_decision, approval_actor, approval_note,
  agent_id, token, venue, symbol, action, notional_usd, confidence, rationale,
  gates, payload, command_preview, row_hash
FROM alpha_translations
ORDER BY proposal_id, ts DESC;
