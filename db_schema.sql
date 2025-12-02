-- 7C Outbox schema (Postgres)
create table if not exists commands (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  agent_id text not null,
  intent jsonb not null,                 -- {symbol, venue, side, amount/notional_usd, ...}
  intent_hash text not null,             -- HMAC-safe hash for idempotency/dedup
  status text not null default 'queued', -- queued|leased|done|canceled|error
  leased_by text,                        -- edge agent id
  lease_at timestamptz,
  lease_expires_at timestamptz,
  attempts int not null default 0,
  dedup_ttl_seconds int not null default 900, -- 15m default
  unique (intent_hash)                   -- idempotent enqueue
);

create index if not exists idx_commands_status on commands(status);
create index if not exists idx_commands_lease_expires on commands(lease_expires_at);

create table if not exists receipts (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  agent_id text not null,
  cmd_id bigint,
  receipt jsonb not null,                -- normalized receipt payload
  ok boolean not null default true
);

create table if not exists telemetry (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  agent_id text not null,
  payload jsonb not null
);

-- Trades: normalized view of fills, backed by Edge receipts
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    cmd_id INTEGER REFERENCES commands(id) ON DELETE SET NULL,
    receipt_id INTEGER REFERENCES receipts(id) ON DELETE SET NULL,

    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT,           -- 'BUY' / 'SELL' / etc.
    base_qty NUMERIC,    -- filled base amount
    quote_qty NUMERIC,   -- filled quote amount
    price NUMERIC,       -- effective fill price
    status TEXT,         -- 'filled', 'partial', 'error', etc.

    raw_payload JSONB,   -- full edge/bus receipt payload for forensics
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trades_cmd_id
    ON trades(cmd_id);

CREATE INDEX IF NOT EXISTS idx_trades_receipt_id
    ON trades(receipt_id);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
    ON trades(venue, symbol, created_at);
