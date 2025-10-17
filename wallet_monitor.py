# wallet_monitor.py  — resilient wallet watcher with Zapper + Covalent fallback
import os
import time
import requests
from datetime import datetime
from typing import Dict, List, Set

from utils import send_telegram_message, get_gspread_client

# ========= ENV =========
SHEET_URL         = os.getenv("SHEET_URL", "")
METAMASK_ADDRESS  = os.getenv("WALLET_ADDR_METAMASK", "0x980032AAB743379a99C4Fd18A4538c8A5DCF47d6")
BESTWALLET_ADDRESS= os.getenv("WALLET_ADDR_BEST",     "0x71197A977c905e54b159D8154a69c6948e3Fd880")

# Provider 1 (primary)
ZAPPER_API_KEY    = os.getenv("ZAPPER_API_KEY", "").strip()
ZAPPER_BASE       = os.getenv("ZAPPER_BASE_URL", "https://api.zapper.xyz").rstrip("/")

# Provider 2 (fallback)
COVALENT_API_KEY  = os.getenv("COVALENT_API_KEY", "").strip()
COVALENT_BASE     = os.getenv("COVALENT_BASE_URL", "https://api.covalenthq.com").rstrip("/")
COVALENT_CHAIN_ID = int(os.getenv("COVALENT_CHAIN_ID", "1"))  # 1 = Ethereum mainnet

HTTP_TIMEOUT_S    = float(os.getenv("WALLET_HTTP_TIMEOUT", "15"))
HTTP_RETRIES      = int(os.getenv("WALLET_HTTP_RETRIES", "2"))
BACKOFF_S         = float(os.getenv("WALLET_BACKOFF_S", "0.6"))

ADDRESSES: List[str] = [METAMASK_ADDRESS, BESTWALLET_ADDRESS]

# ========= HTTP helpers =========
def _get(url: str, *, headers: Dict[str, str] | None = None, params: Dict | None = None):
    last_exc = None
    for i in range(HTTP_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers or {}, params=params or {}, timeout=HTTP_TIMEOUT_S)
            # Stop retrying on client errors except 429
            if r.status_code in (401, 403, 400):
                return r
            if r.status_code == 429:
                time.sleep(BACKOFF_S * (i + 1))
                continue
            if r.status_code >= 500:
                time.sleep(BACKOFF_S * (i + 1))
                last_exc = f"HTTP {r.status_code}"
                continue
            return r
        except Exception as e:
            last_exc = e
            time.sleep(BACKOFF_S * (i + 1))
    raise RuntimeError(f"wallet HTTP error: {last_exc}")

# ========= Providers =========
def fetch_wallet_tokens_zapper(address: str) -> Set[str]:
    """
    Uses Zapper v2 balances/tokens.
    Auth: x-api-key header (preferred). Query param works for some plans but often 403s.
    """
    if not ZAPPER_API_KEY:
        return set()
    url = f"{ZAPPER_BASE}/v2/balances/tokens"
    headers = {"x-api-key": ZAPPER_API_KEY}
    params = {"addresses[]": address}
    try:
        r = _get(url, headers=headers, params=params)
        if r.status_code != 200:
            print(f"⚠️ Zapper {address}: {r.status_code} - {r.text[:240]}")
            return set()
        j = r.json()
        toks: Set[str] = set()
        # Zapper returns keyed by lowercase address
        acct = j.get(address.lower()) or {}
        for product in acct.get("products", []):
            for asset in product.get("assets", []):
                sym = (asset.get("symbol") or "").upper().strip()
                bal = float(asset.get("balance") or 0)
                if sym and bal > 0:
                    toks.add(sym)
        return toks
    except Exception as e:
        print(f"❌ Zapper error for {address}: {e}")
        return set()

def fetch_wallet_tokens_covalent(address: str) -> Set[str]:
    """
    Covalent balances_v2 as a fallback.
    """
    if not COVALENT_API_KEY:
        return set()
    url = f"{COVALENT_BASE}/v1/{COVALENT_CHAIN_ID}/address/{address}/balances_v2/"
    try:
        r = _get(url, params={"key": COVALENT_API_KEY, "nft": False})
        if r.status_code != 200:
            print(f"⚠️ Covalent {address}: {r.status_code} - {r.text[:240]}")
            return set()
        j = r.json()
        items = (j.get("data") or {}).get("items") or []
        toks: Set[str] = set()
        for it in items:
            if (it.get("type") or "").lower() != "cryptocurrency":
                continue
            sym = (it.get("contract_ticker_symbol") or "").upper().strip()
            bal_raw = it.get("balance")
            dec = int(it.get("contract_decimals") or 18)
            try:
                bal = float(bal_raw) / (10 ** dec)
            except Exception:
                bal = 0.0
            if sym and bal > 0:
                toks.add(sym)
        return toks
    except Exception as e:
        print(f"❌ Covalent error for {address}: {e}")
        return set()

def fetch_wallet_tokens(address: str):
    tokens = []

    # 1) Try Zapper with header-based key
    if ZAPPER_API_KEY:
        try:
            z_url = f"https://api.zapper.xyz/v2/balances/tokens?addresses[]={address}"
            z_headers = {"x-api-key": ZAPPER_API_KEY, "accept": "application/json"}
            r = requests.get(z_url, headers=z_headers, timeout=20)
            if r.status_code == 200:
                data = r.json() or {}
                acct = data.get(address.lower(), {}) or {}
                for product in acct.get("products", []) or []:
                    for asset in product.get("assets", []) or []:
                        sym = (asset.get("symbol") or "").upper()
                        bal = float(asset.get("balance") or 0)
                        if sym and bal > 0:
                            tokens.append(sym)
                # dedupe and return if we found anything
                if tokens:
                    return sorted(set(tokens))
            else:
                print(f"⚠️ Zapper fetch failed: {r.status_code} - {r.text[:200]}")
        except Exception as e:
            print(f"❌ Zapper error: {e}")

    # 2) Fallback: Covalent (optional)
    COV_KEY = os.getenv("COVALENT_API_KEY")
    if COV_KEY:
        try:
            # eth-mainnet; adjust chain if you want multi-chain later
            c_url = f"https://api.covalenthq.com/v1/eth-mainnet/address/{address}/balances_v2/?key={COV_KEY}"
            r = requests.get(c_url, timeout=20)
            if r.status_code == 200:
                data = r.json() or {}
                items = ((data.get("data") or {}).get("items") or [])
                for it in items:
                    sym = (it.get("contract_ticker_symbol") or "").upper()
                    bal = float(it.get("balance") or 0)
                    # Covalent returns raw integer balance; use formatted if provided
                    display = it.get("pretty_quote") or it.get("quote")
                    # be conservative: require non-zero balance
                    if sym and (bal > 0):
                        tokens.append(sym)
                if tokens:
                    return sorted(set(tokens))
            else:
                print(f"⚠️ Covalent fetch failed: {r.status_code} - {r.text[:200]}")
        except Exception as e:
            print(f"❌ Covalent error: {e}")

    # Nothing worked
    return []
# ========= Sheet logic =========
def run_wallet_monitor():
    print("🔍 Running Wallet Monitor...")
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEET_URL)
        claim_ws = sheet.worksheet("Claim_Tracker")
        decisions_ws = sheet.worksheet("Scout Decisions")

        claim_rows = claim_ws.get_all_records()
        decision_rows = decisions_ws.get_all_records()

        claimed_tokens = {
            (row.get("Token") or "").strip().upper()
            for row in claim_rows
            if (row.get("Claimed?", "") or "").strip().lower() == "claimed"
        }
        pending_claims = {
            (row.get("Token") or "").strip().upper()
            for row in claim_rows
            if (row.get("Claimed?", "") or "").strip().lower() != "claimed"
        }
        approved_tokens = {
            (row.get("Token") or "").strip().upper()
            for row in decision_rows
            if (row.get("Decision") or "").strip().upper() == "YES"
        }

        # Fetch wallet tokens across all configured addresses
        wallet_tokens: Set[str] = set()
        for addr in ADDRESSES:
            if not addr:
                continue
            toks = fetch_wallet_tokens(addr)
            if not toks and ZAPPER_API_KEY:
                # If Zapper responded with 403, nudge in Telegram once
                send_telegram_message(f"⚠️ Wallet fetch 403/empty for {addr[:6]}…{addr[-4:]}. "
                                      "Check ZAPPER_API_KEY or provider allow-list. Falling back where possible.")
            wallet_tokens |= toks

        print(f"🧾 Wallet Tokens: {wallet_tokens}")
        print(f"📋 Pending Claim Tokens: {pending_claims}")

        # Tokens that arrived and are approved but not marked claimed
        unknown_arrivals = sorted([
            t for t in wallet_tokens
            if t in approved_tokens and t not in claimed_tokens
        ])

        if not unknown_arrivals:
            print("✅ Wallet monitor complete (no new arrivals).")
            return

        # Notify + mark “Resolved” in Status (column I) for rows we alert on
        send_telegram_message(
            "🪙 Detected new wallet tokens not marked as claimed:\n"
            + "\n".join(f"• {t}" for t in unknown_arrivals)
        )

        # Batch update to reduce quota: build cell updates for Status col (I)
        # Find row indices once
        token_to_rowidx: Dict[str, int] = {}
        for i, row in enumerate(claim_rows, start=2):  # row 2 = first data row
            tok = (row.get("Token") or "").strip().upper()
            if tok and tok not in token_to_rowidx:
                token_to_rowidx[tok] = i

        updates = []
        for tok in unknown_arrivals:
            ri = token_to_rowidx.get(tok)
            if ri:
                updates.append((ri, "I"))  # Status column

        if updates:
            # gspread batch update A1 notation
            body = {"valueInputOption": "USER_ENTERED", "data": [
                {"range": f"I{ri}:I{ri}", "values": [["Resolved"]]} for (ri, _) in updates
            ]}
            # Raw HTTP via client (gspread batch_update)
            sheet.batch_update(body)
            for (ri, _) in updates:
                print(f"✅ Status for row {ri} set to Resolved")

        print("✅ Wallet monitor complete.")

    if not all_wallet_tokens:
        print("⚠️ Wallet providers returned no tokens; check API keys / allow-list.")
    
    except Exception as e:
        print(f"❌ Error in run_wallet_monitor: {e}")
        try:
            send_telegram_message(f"❌ Wallet monitor error: {e}")
        except Exception:
            pass

if __name__ == "__main__":
    run_wallet_monitor()
