# wallet_monitor.py — Zapper (header auth) + Covalent fallback + Sheet alerts
import os
import time
import requests
from datetime import datetime
from utils import send_telegram_message, get_gspread_client

# --- Env ---
ZAPPER_API_KEY = os.getenv("ZAPPER_API_KEY")  # header: x-api-key
COVALENT_API_KEY = os.getenv("COVALENT_API_KEY")  # optional fallback

# Your wallet addresses
METAMASK_ADDRESS = os.getenv("WALLET_METAMASK", "0x980032AAB743379a99C4Fd18A4538c8A5DCF47d6")
BESTWALLET_ADDRESS = os.getenv("WALLET_BEST", "0x71197A977c905e54b159D8154a69c6948e3Fd880")

# Sheets
SHEET_URL = os.getenv("SHEET_URL")
CLAIM_TAB = os.getenv("CLAIM_TAB", "Claim_Tracker")
SCOUT_TAB = os.getenv("SCOUT_TAB", "Scout Decisions")

# Controls
WALLET_MONITOR_ENABLED = (os.getenv("WALLET_MONITOR_ENABLED", "1").lower() in {"1", "true", "yes"})
# Comma list of Covalent chains to check as fallback (lowest-cost single is 'eth-mainnet')
WALLET_CHAINS = [c.strip() for c in (os.getenv("WALLET_CHAINS", "eth-mainnet").split(",")) if c.strip()]

# ---------- Providers ----------
def _fetch_zapper_tokens(address: str):
    """Return a set of token symbols using Zapper (header-based API key)."""
    if not ZAPPER_API_KEY:
        return set(), "no-key"

    url = f"https://api.zapper.xyz/v2/balances/tokens?addresses[]={address}"
    headers = {"x-api-key": ZAPPER_API_KEY, "accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            return set(), f"{r.status_code}:{r.text[:200]}"
        data = r.json() or {}
        acct = (data.get(address.lower()) or {})
        syms = set()
        for product in (acct.get("products") or []):
            for asset in (product.get("assets") or []):
                sym = (asset.get("symbol") or "").upper()
                bal = float(asset.get("balance") or 0.0)
                if sym and bal > 0:
                    syms.add(sym)
        return syms, None
    except Exception as e:
        return set(), f"zapper-exc:{e}"

def _fetch_covalent_tokens(address: str, chains: list[str]):
    """Return a set of token symbols using Covalent (balances_v2) across the given chains."""
    if not COVALENT_API_KEY or not chains:
        return set(), "no-fallback"

    out = set()
    err_agg = []
    for chain in chains:
        try:
            # Example: https://api.covalenthq.com/v1/eth-mainnet/address/<addr>/balances_v2/?key=...
            url = f"https://api.covalenthq.com/v1/{chain}/address/{address}/balances_v2/?key={COVALENT_API_KEY}"
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                err_agg.append(f"{chain}:{r.status_code}")
                continue
            j = r.json() or {}
            items = ((j.get("data") or {}).get("items") or [])
            for it in items:
                sym = (it.get("contract_ticker_symbol") or "").upper()
                # Convert raw integer balance using decimals (if present)
                raw = it.get("balance")
                dec = it.get("contract_decimals") or 0
                bal = 0.0
                try:
                    bal = float(raw) / (10 ** int(dec)) if raw is not None else 0.0
                except Exception:
                    pass
                if sym and bal > 0:
                    out.add(sym)
        except Exception as e:
            err_agg.append(f"{chain}:exc:{e}")
    return out, (";".join(err_agg) if err_agg else None)

def fetch_wallet_tokens(address: str):
    """
    Try Zapper first (header auth). If forbidden/empty, fall back to Covalent on configured chains.
    Returns a deduped sorted list of symbols.
    """
    # 1) Zapper
    z_syms, z_err = _fetch_zapper_tokens(address)
    if z_syms:
        return sorted(z_syms)

    # 2) Covalent fallback
    c_syms, c_err = _fetch_covalent_tokens(address, WALLET_CHAINS)
    if c_syms:
        print(f"ℹ️ Zapper failed ({z_err}); used Covalent fallback for {address[:6]}…{address[-4:]}")
        return sorted(c_syms)

    # 3) Nothing worked
    # Be explicit in logs one time
    print(f"⚠️ Wallet fetch 403/empty for {address[:6]}…{address[-4:]}. "
          f"Zapper={z_err} Covalent={c_err}. Check API keys / allow-list.")
    return []

# ---------- Monitor ----------
def run_wallet_monitor():
    if not WALLET_MONITOR_ENABLED:
        print("🔕 Wallet Monitor disabled (WALLET_MONITOR_ENABLED=0).")
        return

    print("🔍 Running Wallet Monitor...")
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEET_URL)
        claim_ws = sheet.worksheet(CLAIM_TAB)
        decisions_ws = sheet.worksheet(SCOUT_TAB)

        claim_data = claim_ws.get_all_records()
        decision_data = decisions_ws.get_all_records()

        claimed_tokens = {
            (row.get("Token") or "").strip().upper()
            for row in claim_data
            if (row.get("Claimed?", "") or "").strip().lower() == "claimed"
        }

        pending_claims = {
            (row.get("Token") or "").strip().upper()
            for row in claim_data
            if (row.get("Claimed?", "") or "").strip().lower() != "claimed"
        }

        all_approved = {
            (row.get("Token") or "").strip().upper()
            for row in decision_data
            if (row.get("Decision") or "").strip().upper() == "YES"
        }

        all_wallet_tokens = set()
        for addr in (METAMASK_ADDRESS, BESTWALLET_ADDRESS):
            toks = fetch_wallet_tokens(addr)
            all_wallet_tokens.update(toks)

        print(f"🧾 Wallet Tokens: {all_wallet_tokens}")
        print(f"📋 Pending Claim Tokens: {pending_claims}")

        if not all_wallet_tokens:
            print("⚠️ Wallet providers returned no tokens; check ZAPPER_API_KEY and/or COVALENT_API_KEY (or allow-list).")

        # Alert for arrivals: approved & not yet marked as claimed
        unknown_arrivals = [
            t for t in all_wallet_tokens
            if t in all_approved and t not in claimed_tokens
        ]

        for token in unknown_arrivals:
            msg = (
                f"⚠️ *{token}* has arrived in your wallet,\n"
                f"but is *not marked as claimed* in the sheet.\n\n"
                f"Would you like to mark it as claimed?"
            )
            send_telegram_message(msg)
            print(f"🔔 Alert sent for token: {token}")

            # Auto-mark 'Status' to Resolved if present
            for i, row in enumerate(claim_data, start=2):  # row 2 = first data row
                if (row.get('Token') or '').strip().upper() == token:
                    try:
                        claim_ws.update_acell(f"I{i}", "Resolved")  # assumes Status col is I
                        print(f"✅ Status for {token} set to Resolved")
                    except Exception as e:
                        print(f"⚠️ Could not update sheet status for {token}: {e}")

        print("✅ Wallet monitor complete.")

    except Exception as e:
        print(f"❌ Error in run_wallet_monitor: {e}")

# For ad-hoc local test:
if __name__ == "__main__":
    run_wallet_monitor()
