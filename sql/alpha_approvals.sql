-- ============================================================
-- alpha_approvals.sql (Phase 26B)
--
-- Purpose:
--   Capture human approvals/denials/holds for Alpha proposals (preview-only).
--   Proposals remain immutable in alpha_proposals; approvals are append-only.
--
-- Notes:
--   - No trading / no command enqueue.
--   - Designed to ingest from Google Sheet tab "Alpha_Approvals".
-- ============================================================

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS alpha_approvals (
  approval_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  ts            TIMESTAMPTZ NOT NULL    DEFAULT NOW(),
  agent_id      TEXT        NOT NULL    DEFAULT 'bus',
  proposal_id   UUID        NULL,
  proposal_hash TEXT        NULL,
  token         TEXT        NULL,
  decision      TEXT        NOT NULL,   -- APPROVE | DENY | HOLD
  actor         TEXT        NOT NULL,   -- who approved
  note          TEXT        NULL,
  source        TEXT        NOT NULL    DEFAULT 'sheet',
  row_hash      TEXT        NOT NULL
);

-- Ensure idempotent ingest from sheets
CREATE UNIQUE INDEX IF NOT EXISTS alpha_approvals_row_hash_uq
  ON alpha_approvals(row_hash);

CREATE INDEX IF NOT EXISTS alpha_approvals_proposal_id_idx
  ON alpha_approvals(proposal_id);

CREATE INDEX IF NOT EXISTS alpha_approvals_token_idx
  ON alpha_approvals(token);

-- Convenience view: latest decision per proposal (by ts)
CREATE OR REPLACE VIEW alpha_approvals_latest_v AS
SELECT DISTINCT ON (COALESCE(proposal_hash, proposal_id::text, token))
  approval_id,
  ts,
  agent_id,
  proposal_id,
  proposal_hash,
  token,
  decision,
  actor,
  note,
  source,
  row_hash
FROM alpha_approvals
ORDER BY COALESCE(proposal_hash, proposal_id::text, token), ts DESC;
