                          ┌───────────────────────────────────────────────────┐
                          │                 THE COUNCIL                      │
                          │  Soul (Brett) • Nova (Heart) • Ash (Mind) • Orion│
                          └───────────────┬──────────────────────────────────┘
                                          │ governance + policy YAML
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CLOUD ORCHESTRATOR (BUS)                            │
│  Render (uvicorn wsgi)                                                      │
│                                                                             │
│  ┌───────────────┐      ┌────────────────────┐      ┌───────────────────┐   │
│  │ Telegram / tg │◀────▶│  Policy Engine     │◀────▶│ Vault Intelligence│   │
│  │  alerts+input │      │  (yaml loader)     │      │ (scores/eligibility)│ │
│  └───────────────┘      └─────────┬──────────┘      └─────────┬─────────┘   │
│                                   │                             │            │
│                         ┌─────────▼─────────┐                   │            │
│                         │  Rebuy Driver     │  (decides trades) │            │
│                         │  (Rotation Engine)│──────────────┐    │            │
│                         └─────────┬─────────┘              │    │            │
│                                   │   enqueue intent       │    │            │
│        ┌──────────────┐           ▼                        │    ▼            │
│        │ Sheets Mirror│◀───data──┤   Outbox (SQLite→PG)   │  Metrics        │
│        │ + Dashboards │          └────────┬───────────────┘   (telemetry)   │
│        └─────▲────────┘                   │                                │
│              │                             ▼ pull/ack                      │
│        ┌─────┴────────┐        ┌────────────────────────┐                  │
│        │ Daily Digest │◀──────▶│ Receipts Bridge        │◀─── Edge ACKs    │
│        └──────────────┘        └────────────────────────┘                  │
│                                                                             │
│  Kill Switch #1: CLOUD_HOLD  (halts enqueue/dispatch)                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                          │ long-poll (HMAC)
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EDGE AGENT (ORION / HANDS)                         │
│  Local/Render worker                                                          │
│                                                                             │
│  ┌───────────┐  ┌────────────┐  ┌───────────┐                               │
│  │ Coinbase  │  │ BinanceUS  │  │  Kraken   │   … venue routers             │
│  └────▲──────┘  └────▲───────┘  └────▲──────┘                               │
│       │ price/exec        │             │                                   │
│       └───────────┬───────┴─────────────┴───────────┐                       │
│                   │   execute (dry/live) + receipts │                       │
│                   ▼                                  ▼                       │
│            Balance/Price Pollers ─────────► Telemetry → Bus                 │
│                                                                             │
│  Kill Switch #2: EDGE_HOLD (blocks order placement, still polls/ACKs)       │
└─────────────────────────────────────────────────────────────────────────────┘

                 Google Sheets  ⇄  Dashboards / Logs / Memory / Triggers
                 Telegram       ⇄  Operator prompts + daily/health reports
