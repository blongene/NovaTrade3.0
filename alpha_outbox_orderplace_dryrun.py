#!/usr/bin/env python3
import os
import json
import time
import hashlib
import logging
from typing import Any, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras


LOG = logging.getLogger("alpha_outbox_orderplace_dryrun")


def _canon_order_place_payload(payload: dict) -> dict:
    """Normalize order.place payload fields so Edge always sees non-zero sizing.

    BUY sizing: amount_quote preferred (quote currency). If only amount_usd is present,
    we mirror it into amount_quote.

    SELL sizing: amount_base required (base currency).
    """
    try:
        side = (payload.get("side") or "").upper()
        if side == "BUY":
            amt = payload.get("amount_quote")
            if amt is None:
                amt = payload.get("quote_amount") or payload.get("amount_usd") or payload.get("amount")
            try:
                amt_f = float(amt)
            except Exception:
                amt_f = 0.0
            if amt_f <= 0:
                amt_f = 1.0
            payload["amount_quote"] = amt_f
            payload["amount_usd"] = float(payload.get("amount_usd") or amt_f)
        elif side == "SELL":
            amt = payload.get("amount_base")
            if amt is None:
                amt = payload.get("base_amount") or payload.get("amount")
            if amt is not None:
                try:
                    payload["amount_base"] = float(amt)
                except Exception:
                    pass
    except Exception:
        pass
    return payload
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="[%(asctime)s] %(levelname)s  %(message)s",
)

DEFAULT_AGENT_ID = os.getenv("ALPHA26E_AGENT_ID", "edge-primary")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _load_alpha_config() -> Dict[str, Any]:
    """
    Optional: Parse ALPHA_CONFIG_JSON for sizing + feature gates.
    If missing/invalid, returns empty dict.
    """
    raw = os.getenv("ALPHA_CONFIG_JSON", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception as e:
        LOG.warning("ALPHA_CONFIG_JSON invalid JSON (%s); ignoring", e)
        return {}


def _cfg_get(cfg: Dict[str, Any], path: Tuple[str, ...], default: Any) -> Any:
    cur: Any = cfg
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _stable_intent_hash(intent: Dict[str, Any], include_idem: bool) -> str:
    """
    Compute intent_hash for commands table. Must be deterministic.
    If include_idem=True, includes idempotency_key so repeated tests won't dedupe.
    """
    obj = {
        "type": intent.get("type"),
        "payload": intent.get("payload", {}),
    }
    if include_idem:
        obj["idempotency_key"] = (intent.get("payload") or {}).get("idempotency_key", "")
    return _sha256_hex(json.dumps(obj, sort_keys=True, separators=(",", ":")))


def _connect_db() -> psycopg2.extensions.connection:
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DB_URL (or DATABASE_URL) is not set")
    return psycopg2.connect(db_url)


def _pick_latest_translation(cur) -> Optional[Dict[str, Any]]:
    """
    Picks the most recent translation row. This keeps it simple + reliable.
    """
    cur.execute(
        """
        select translation_id, proposal_id, token, venue, symbol, action, ts
        from alpha_translations
        order by ts desc
        limit 1;
        """
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _already_outboxed(cur, translation_id: str) -> bool:
    cur.execute(
        "select 1 from alpha_dryrun_orderplace_outbox where translation_id=%s limit 1;",
        (translation_id,),
    )
    return cur.fetchone() is not None


def _command_exists_by_hash(cur, intent_hash: str) -> Optional[int]:
    cur.execute("select id from commands where intent_hash=%s order by id desc limit 1;", (intent_hash,))
    r = cur.fetchone()
    return int(r["id"]) if r else None


def _insert_command(cur, agent_id: str, intent: Dict[str, Any], intent_hash: str) -> int:
    cur.execute(
        """
        insert into commands (created_at, status, agent_id, intent, intent_hash)
        values (now(), 'queued', %s, %s::jsonb, %s)
        returning id;
        """,
        (agent_id, json.dumps(intent), intent_hash),
    )
    return int(cur.fetchone()["id"])


def _insert_outbox(cur, outbox_row: Dict[str, Any]) -> None:
    cur.execute(
        """
        insert into alpha_dryrun_orderplace_outbox
        (translation_id, proposal_id, token, venue, symbol, side, cmd_id, intent_hash, intent, note)
        values
        (%(translation_id)s, %(proposal_id)s, %(token)s, %(venue)s, %(symbol)s, %(side)s,
         %(cmd_id)s, %(intent_hash)s, %(intent)s::jsonb, %(note)s)
        on conflict (translation_id) do nothing;
        """,
        {
            **outbox_row,
            "intent": json.dumps(outbox_row["intent"]),
        },
    )


def run() -> None:
    """
    Env controls:
      - ALPHA26E_TEST_SIDE=BUY|SELL (optional, default derived from translation action)
      - ALPHA26E_IDEM_PREFIX=... (optional, affects idempotency_key)
      - ALPHA26E_FORCE_REPROCESS=1 (ignore UNIQUE translation_id block)
      - ALPHA26E_FORCE_REQUEUE=1 (ignore intent_hash dedupe)
      - ALPHA26E_HASH_INCLUDE_IDEM=1 (make intent_hash unique per idem key for repeated tests)
    """
    cfg = _load_alpha_config()

    dryrun_enabled = bool(_cfg_get(cfg, ("phase26", "dryrun", "enabled"), True))
    allow_order_place = bool(_cfg_get(cfg, ("phase26", "dryrun", "allow_order_place"), True))

    if not dryrun_enabled or not allow_order_place:
        LOG.warning("Phase26 dryrun disabled (enabled=%s allow_order_place=%s) -> skipping",
                    dryrun_enabled, allow_order_place)
        return

    buy_max_usd = float(_cfg_get(cfg, ("phase26", "dryrun", "buy_max_usd"), 10.0))
    sell_base_amount = float(_cfg_get(cfg, ("phase26", "dryrun", "sell_base_amount"), 0.00005))

    force_reprocess = _env_bool("ALPHA26E_FORCE_REPROCESS", False)
    force_requeue = _env_bool("ALPHA26E_FORCE_REQUEUE", False)
    hash_include_idem = _env_bool("ALPHA26E_HASH_INCLUDE_IDEM", True)

    processed = 0
    enqueued_new = 0
    skipped = 0

    with _connect_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            t = _pick_latest_translation(cur)
            if not t:
                LOG.warning("No rows in alpha_translations -> nothing to do")
                return

            translation_id = str(t["translation_id"])
            proposal_id = str(t["proposal_id"])
            token = str(t["token"] or "").upper()
            venue = str(t["venue"] or "").upper()
            symbol = str(t["symbol"] or "")
            action = str(t["action"] or "").upper()

            # Choose side
            side_env = (os.getenv("ALPHA26E_TEST_SIDE") or "").strip().upper()
            if side_env in ("BUY", "SELL"):
                side = side_env
            else:
                # derive from action when possible
                if "SELL" in action:
                    side = "SELL"
                else:
                    side = "BUY"

            processed += 1

            if not force_reprocess and _already_outboxed(cur, translation_id):
                skipped += 1
                LOG.info("skip: translation_id already in outbox (UNIQUE). Set ALPHA26E_FORCE_REPROCESS=1 to override. translation_id=%s",
                         translation_id)
                return

            idem_prefix = os.getenv("ALPHA26E_IDEM_PREFIX", "alpha26e")
            # include a timestamp to make repeated tests unique if you want
            idem_key = f"{idem_prefix}:{_sha256_hex(translation_id + '|' + side + '|' + str(time.time_ns()))}"

            payload: Dict[str, Any] = {
                "meta": {"phase": "26E", "proposal_id": proposal_id, "translation_id": translation_id},
                "side": side,
                "token": token,
                "venue": venue,
                "symbol": symbol,
                "dry_run": True,
                "idempotency_key": idem_key,
            }

            # BUY = quote sizing (USD), SELL = base sizing
            if side == "BUY":
                payload["amount_usd"] = float(buy_max_usd)
                payload["amount_quote"] = float(buy_max_usd)
            else:
                payload["amount_base"] = float(sell_base_amount)

            payload = _canon_order_place_payload(payload)
            intent = {"type": "order.place", "payload": payload}

            intent_hash = _stable_intent_hash(intent, include_idem=hash_include_idem)

            existing_cmd = _command_exists_by_hash(cur, intent_hash)
            if existing_cmd and not force_requeue:
                skipped += 1
                LOG.info(
                    "skip: command already exists for intent_hash=%s (cmd_id=%s). "
                    "Set ALPHA26E_FORCE_REQUEUE=1 or ALPHA26E_HASH_INCLUDE_IDEM=1 to override.",
                    intent_hash, existing_cmd
                )
                return

            cmd_id = _insert_command(cur, DEFAULT_AGENT_ID, intent, intent_hash)

            _insert_outbox(
                cur,
                {
                    "translation_id": translation_id,
                    "proposal_id": proposal_id,
                    "token": token,
                    "venue": venue,
                    "symbol": symbol,
                    "side": side,
                    "cmd_id": cmd_id,
                    "intent_hash": intent_hash,
                    "intent": intent,
                    "note": f"Phase26E dryrun order.place ({side}) from translation {translation_id}",
                },
            )

            enqueued_new += 1
            conn.commit()

    LOG.info(
        "alpha_outbox_orderplace_dryrun: processed=%s enqueued_new=%s skipped=%s (side=%s venue=%s symbol=%s)",
        processed, enqueued_new, skipped, os.getenv("ALPHA26E_TEST_SIDE", ""), venue, symbol
    )


if __name__ == "__main__":
    run()
