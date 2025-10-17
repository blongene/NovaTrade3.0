# wallet_monitor.py — Zapper GraphQL primary + Covalent/GoldRush fallback + Sheets/Telegram
import os
import time
import requests
import re
from datetime import datetime
from typing import List, Optional, Tuple, Set
from utils import send_telegram_message, get_gspread_client

# ---------- Env ----------
ZAPPER_API_KEY    = os.getenv("ZAPPER_API_KEY")  # header: x-zapper-api-key
COVALENT_API_KEY  = os.getenv("COVALENT_API_KEY")  # optional fallback

METAMASK_ADDRESS  = os.getenv("WALLET_METAMASK", "0x980032AAB743379a99C4Fd18A4538c8A5DCF47d6")
BESTWALLET_ADDRESS= os.getenv("WALLET_BEST", "0x71197A977c905e54b159D8154a69c6948e3Fd880")

SHEET_URL         = os.getenv("SHEET_URL")
CLAIM_TAB         = os.getenv("CLAIM_TAB", "Claim_Tracker")
SCOUT_TAB         = os.getenv("SCOUT_TAB", "Scout Decisions")

WALLET_MONITOR_ENABLED = (os.getenv("WALLET_MONITOR_ENABLED", "1").lower() in {"1","true","yes"})
# Comma list of EVM chain IDs for Zapper GraphQL; if empty -> query all chains
ZAPPER_CHAIN_IDS  = [int(x.strip()) for x in (os.getenv("ZAPPER_CHAIN_IDS","").split(",")) if x.strip().isdigit()]
# Covalent chains for fallback (Covalent expects slugs like 'eth-mainnet', 'base-mainnet')
WALLET_CHAINS     = [c.strip() for c in (os.getenv("WALLET_CHAINS","eth-mainnet").split(",")) if c.strip()]

# Filtering controls
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "0"))  # e.g., 5000000 for $5m floor
REQUIRE_SYMBOL_CLEAN = (os.getenv("REQUIRE_SYMBOL_CLEAN", "1").lower() in {"1","true","yes"})

# Allowed symbol pattern: letters/numbers/_-. up to 10 chars (adjust if needed)
SYM_OK = re.compile(r"^[A-Z0-9._-]{2,10}$")

def is_symbol_clean(sym: str) -> bool:
    s = (sym or "").upper()
    if not s: return False
    if not SYM_OK.match(s): return False
    # Reject obvious phishing markers
    bad_fragments = ("HTTP", "HTTPS", "WWW.", "T.ME", "BIO.LINK", "T.LY", "WR.DO", "CLAIM", "VISIT")
    if any(f in s for f in bad_fragments): return False
    return True
  
# ---------- Zapper GraphQL Primary ----------
_ZAPPER_GQL_URL = "https://public.zapper.xyz/graphql"

_GQL_WITH_CHAINS = """
query TokenBalances($addresses: [Address!]!, $first: Int!, $after: String, $chainIds: [Int!]) {
  portfolioV2(addresses: $addresses, chainIds: $chainIds) {
    tokenBalances {
      byToken(first: $first, after: $after) {
        totalCount
        edges {
          node {
            name
            symbol
            decimals
            balanceRaw
            balanceUSD
            tokenAddress
            onchainMarketData { marketCap }
          }
          cursor
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""

_GQL_ALL_CHAINS = """
query TokenBalances($addresses: [Address!]!, $first: Int!, $after: String) {
  portfolioV2(addresses: $addresses) {
    tokenBalances {
      byToken(first: $first, after: $after) {
        totalCount
        edges {
          node {
            name
            symbol
            decimals
            balanceRaw
            balanceUSD
            tokenAddress
            onchainMarketData { marketCap }
          }
          cursor
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""

def _zapper_graphql_fetch_symbols(address: str):
    if not ZAPPER_API_KEY:
        return set(), "zapper:no-key"

    headers = {
        "x-zapper-api-key": ZAPPER_API_KEY,
        "content-type": "application/json",
        "accept": "application/json",
    }

    query = _GQL_WITH_CHAINS if ZAPPER_CHAIN_IDS else _GQL_ALL_CHAINS
    variables = {
        "addresses": [address],
        "first": 250,
        "after": None,
    }
    if ZAPPER_CHAIN_IDS:
        variables["chainIds"] = ZAPPER_CHAIN_IDS

    out_syms, seen_cursors, loops = set(), set(), 0

    try:
        while True:
            loops += 1
            payload = {"query": query, "variables": variables}
            r = requests.post(_ZAPPER_GQL_URL, headers=headers, json=payload, timeout=30)
            if r.status_code != 200:
                return out_syms, f"zapper:{r.status_code}:{r.text[:200]}"

            j = r.json() or {}
            data = (j.get("data") or {})
            pf = data.get("portfolioV2") or {}
            tb = pf.get("tokenBalances") or {}
            bt = tb.get("byToken") or {}

            edges = bt.get("edges") or []
            for edge in edges:
                node = edge.get("node") or {}
                sym  = (node.get("symbol") or "").upper()
                dec  = node.get("decimals") or 0
                raw  = node.get("balanceRaw")
                mcap = ((node.get("onchainMarketData") or {}).get("marketCap") or 0) or 0.0

                # Apply symbol + market-cap filters
                if REQUIRE_SYMBOL_CLEAN and not is_symbol_clean(sym):
                    continue
                if MIN_MARKET_CAP_USD > 0 and (not isinstance(mcap, (int, float)) or mcap < MIN_MARKET_CAP_USD):
                    continue

                bal = 0.0
                try:
                    bal = float(raw) / (10 ** int(dec)) if raw is not None else 0.0
                except Exception:
                    pass
                if sym and bal > 0:
                    out_syms.add(sym)


            page = bt.get("pageInfo") or {}
            has_next = bool(page.get("hasNextPage"))
            end_cursor = page.get("endCursor")
            if not has_next or not end_cursor or end_cursor in seen_cursors:
                break
            seen_cursors.add(end_cursor)
            variables["after"] = end_cursor
            if loops > 50:  # hard safety
                break

        return out_syms, None

    except Exception as e:
        return out_syms, f"zapper:exc:{e}"
      
# ---------- Covalent/GoldRush Fallback ----------
def _fetch_covalent_tokens(address: str, chains: List[str]) -> Tuple[Set[str], Optional[str]]:
    """
    Try both Covalent legacy (?key=) and GoldRush Bearer header for balances_v2.
    """
    key = COVALENT_API_KEY
    if not key or not chains:
        return set(), "covalent:no-key-or-chains"

    syms: Set[str] = set()
    errs = []

    for chain in chains:
        ok = False
        j = None

        # 1) Legacy query-param style
        try:
            url_qs = f"https://api.covalenthq.com/v1/{chain}/address/{address}/balances_v2/?key={key}"
            r = requests.get(url_qs, timeout=20)
            if r.status_code == 200:
                ok, j = True, (r.json() or {})
            else:
                errs.append(f"{chain}:qs:{r.status_code}")
        except Exception as e:
            errs.append(f"{chain}:qs_exc:{e}")

        # 2) Bearer header style
        if not ok:
            try:
                url_hdr = f"https://api.covalenthq.com/v1/{chain}/address/{address}/balances_v2/"
                r = requests.get(url_hdr, timeout=20, headers={"Authorization": f"Bearer {key}"})
                if r.status_code == 200:
                    ok, j = True, (r.json() or {})
                else:
                    errs.append(f"{chain}:bearer:{r.status_code}")
            except Exception as e:
                errs.append(f"{chain}:bearer_exc:{e}")

        if not ok:
            continue

        items = ((j.get("data") or {}).get("items") or [])
        for it in items:
            sym = (it.get("contract_ticker_symbol") or "").upper()
            raw = it.get("balance")
            dec = it.get("contract_decimals") or 0
            bal = 0.0
            try:
                bal = float(raw) / (10 ** int(dec)) if raw is not None else 0.0
            except Exception:
                pass
            if sym and bal > 0:
                syms.add(sym)

    return syms, (";".join(errs) if errs else None)

# ---------- Unified wallet fetch ----------
def fetch_wallet_tokens(address: str) -> List[str]:
    """
    Primary: Zapper GraphQL (header auth).
    Fallback: Covalent/GoldRush (if key present).
    Returns a sorted, de-duplicated list of token symbols with non-zero balances.
    """
    # 1) Zapper GQL
    z_syms, z_err = _zapper_graphql_fetch_symbols(address)
    if z_syms:
        return sorted(z_syms)

    # 2) Covalent fallback (optional)
    c_syms, c_err = _fetch_covalent_tokens(address, WALLET_CHAINS)
    if c_syms:
        print(f"ℹ️ Zapper failed ({z_err}); used Covalent/GoldRush fallback for {address[:6]}…{address[-4:]}")
        return sorted(c_syms)

    # 3) Nothing worked
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

        # Allowlist from Scout Decisions (Decision=YES)
        allowlist = set(all_approved)
        
        # After gathering all_wallet_tokens, intersect with allowlist if you want only known tokens:
        ONLY_ALLOWLIST = (os.getenv("WALLET_ALLOWLIST_ONLY", "0").lower() in {"1","true","yes"})
        if ONLY_ALLOWLIST:
            before = set(all_wallet_tokens)
            all_wallet_tokens = all_wallet_tokens.intersection(allowlist)
            dropped = before - all_wallet_tokens
            if dropped:
                print(f"ℹ️ Dropped {len(dropped)} non-allowlisted symbols.")

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

            # Auto-mark 'Status' to Resolved if present (Status expected in column I)
            for i, row in enumerate(claim_data, start=2):  # row 2 = first data row
                if (row.get('Token') or '').strip().upper() == token:
                    try:
                        claim_ws.update_acell(f"I{i}", "Resolved")
                        print(f"✅ Status for {token} set to Resolved")
                    except Exception as e:
                        print(f"⚠️ Could not update sheet status for {token}: {e}")

        print("✅ Wallet monitor complete.")

    except Exception as e:
        print(f"❌ Error in run_wallet_monitor: {e}")

# For ad-hoc local test:
if __name__ == "__main__":
    run_wallet_monitor()
