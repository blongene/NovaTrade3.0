import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import ping_webhook_debug
from nova_heartbeat import log_heartbeat

# === Legacy defaults (used if env not configured) ===
TOKEN = "MIND"
WALLET_BALANCE = 296139.94  # legacy single-token principal
SHEET_NAME = "Rotation_Log"
SHEET_URL = os.getenv("SHEET_URL")

# New: comma-separated list of staking tokens, e.g. "MIND,XYZ,FOO".
# For each token T, we expect an env var STAKING_WALLET_BALANCE_T with the principal.
TOKENS_ENV = os.getenv("STAKING_TOKENS", "").strip()


def _load_token_configs():
    """Return list of (token, wallet_balance) from env, with sane fallback.

    If STAKING_TOKENS is unset or invalid, we fall back to the legacy
    single-token (TOKEN, WALLET_BALANCE) so behavior remains unchanged
    unless the user explicitly configures multi-token tracking.
    """
    tokens = []

    if TOKENS_ENV:
        for raw in TOKENS_ENV.split(","):
            sym = raw.strip().upper()
            if not sym:
                continue
            env_key = f"STAKING_WALLET_BALANCE_{sym}"
            bal_raw = os.getenv(env_key)
            if bal_raw is None:
                ping_webhook_debug(
                    f"⚠️ Staking Tracker: {env_key} not set; skipping {sym}."
                )
                continue
            try:
                bal = float(str(bal_raw).replace(",", "").strip())
            except Exception:
                ping_webhook_debug(
                    f"⚠️ Staking Tracker: invalid {env_key} value {bal_raw!r}; skipping {sym}."
                )
                continue
            tokens.append((sym, bal))

    if not tokens:
        # Fallback: legacy single-token behavior
        tokens.append((TOKEN.upper(), float(WALLET_BALANCE)))

    return tokens


def run_staking_yield_tracker():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        svc_path = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/etc/secrets/sentiment-log-service.json",
        )
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc_path, scope)
        client = gspread.authorize(creds)
        ws = client.open_by_url(SHEET_URL).worksheet(SHEET_NAME)
        data = ws.get_all_records()

        token_configs = _load_token_configs()

        for token_sym, wallet_balance in token_configs:
            updated = False
            for i, row in enumerate(data, start=2):  # row offset
                token = str(row.get("Token", "")).strip().upper()
                if token != token_sym:
                    continue

                val = row.get("Initial Claimed", "")

                # Guard: skip datetime-like strings
                if isinstance(val, str) and "-" in val and ":" in val:
                    msg = f"⚠️ Skipping {token_sym} – looks like datetime in Initial Claimed: {val}"
                    print(msg)
                    ping_webhook_debug(msg)
                    continue

                try:
                    initial_claimed = float(str(val).replace("%", "").strip())
                except Exception:
                    msg = f"⚠️ Skipping {token_sym} – invalid Initial Claimed value: {val}"
                    print(msg)
                    ping_webhook_debug(msg)
                    continue

                last_balance = float(wallet_balance)
                if initial_claimed == 0:
                    msg = f"⚠️ Skipping {token_sym} – Initial Claimed is 0; cannot compute yield."
                    print(msg)
                    ping_webhook_debug(msg)
                    continue

                yield_percent = round(
                    ((last_balance - initial_claimed) / initial_claimed) * 100, 4
                )
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                ws.update_acell(f"K{i}", f"{yield_percent}%")
                ws.update_acell(f"N{i}", timestamp)

                log_heartbeat(
                    "Staking Tracker",
                    f"{token_sym} Yield = {yield_percent}% (balance={last_balance}, initial={initial_claimed})",
                )
                if yield_percent == 0:
                    ping_webhook_debug(
                        f"⚠️ {token_sym} staking yield is 0%. Verify staking is active."
                    )
                updated = True
                # assume one row per token; break once we've updated it
                break

            if not updated:
                log_heartbeat(
                    "Staking Tracker",
                    f"{token_sym}: token not found in {SHEET_NAME}",
                )

    except Exception as e:
        ping_webhook_debug(f"❌ Staking Yield Tracker Error: {str(e)}")
        print(f"❌ Error: {e}")
