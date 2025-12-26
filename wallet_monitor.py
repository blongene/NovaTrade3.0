# wallet_monitor.py — Bus (on-chain wallet token arrival monitor)
# Primary: Zapper GraphQL; Fallback: Covalent/GoldRush.
# Writes nothing to Wallet_Monitor (that tab is for telemetry_mirror).
# This module monitors *on-chain wallets* for token arrivals vs Claim_Tracker / Scout Decisions.

import os
import re
import time
import requests
from datetime import datetime
from typing import List, Optional, Tuple, Set, Dict

from utils import with_sheet_backoff, get_sheet

# Prefer deduped telegram if available; fallback to send_telegram_message if not
try:
    from utils import telegram_send_deduped as _tg_send  # type: ignore
except Exception:
    try:
        from utils import send_telegram_message as _tg_send  # type: ignore
    except Exception:
        _tg_send = None


# ---------------- Env ----------------
ZAPPER_API_KEY = os.getenv("ZAPPER_API_KEY")  # header: x-zapper-api-key
COVALENT_API_KEY = os.getenv("COVALENT_API_KEY")  # optional fallback

SHEET_URL = os.getenv("SHEET_URL")
CLAIM_TAB = os.getenv("CLAIM_TAB", "Claim_Tracker")
SCOUT_TAB = os.getenv("SCOUT_TAB", "Scout Decisions")

WALLET_MONITOR_ENABLED = os.getenv("WALLET_MONITOR_ENABLED", "1").lower() in {"1", "true", "yes", "on"}

# Prefer new list env, but keep backward compat with existing vars
WALLET_ADDRESSES_ENV = (os.getenv("WALLET_ADDRESSES", "") or "").strip()
METAMASK_ADDRESS = os.getenv("WALLET_METAMASK", "0x980032AAB743379a99C4Fd18A4538c8A5DCF47d6")
BESTWALLET_ADDRESS = os.getenv("WALLET_BEST", "0x71197A977c905e54b159D8154a69c6948e3Fd880")

# Comma list of EVM chain IDs for Zapper GraphQL; if empty -> query all chains
ZAPPER_CHAIN_IDS = [int(x.strip()) for x in (os.getenv("ZAPPER_CHAIN_IDS", "").split(",")) if x.strip().isdigit()]

# Covalent chains for fallback (expects slugs like 'eth-mainnet', 'base-mainnet')
WALLET_CHAINS = [c.strip() for c in (os.getenv("WALLET_CHAINS", "eth-mainnet").split(",")) if c.strip()]

# Filtering controls
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "0"))
REQUIRE_SYMBOL_CLEAN = os.getenv("REQUIRE_SYMBOL_CLEAN", "1").lower() in {"1", "true", "yes", "on"}

# Optional allowlist-only mode (restrict to Scout Decisions YES list)
WALLET_ALLOWLIST_ONLY = os.getenv("WALLET_ALLOWLIST_ONLY", "0").lower() in {"1", "true", "yes", "on"}

# Telegram alert dedupe window (minutes)
WALLET_ALERT_DEDUP_MIN = int(os.getenv("WALLET_ALERT_DEDUP_MIN", "360"))  # 6 hours default

# Optional: auto-mark status/resolved and/or claimed
WALLET_AUTO_RESOLVE_STATUS = os.getenv("WALLET_AUTO_RESOLVE_STATUS", "1").lower() in {"1", "true", "yes", "on"}
# If you truly want auto-claim marking, keep it off by default:
WALLET_AUTO_MARK_CLAIMED = os.getenv("WALLET_AUTO_MARK_CLAIMED", "0").lower() in {"1", "true", "yes", "on"}

# Allowed symbol pattern: letters/numbers/_-. up to 10 chars
SYM_OK = re.compile(r"^[A-Z0-9._-]{2,10}$")


def _addresses() -> List[str]:
    if WALLET_ADDRESSES_ENV:
        out = [a.strip() for a in WALLET_ADDRESSES_ENV.split(",") if a.strip()]
        return out
    # Backward compat
    out = []
    if METAMASK_ADDRESS:
        out.append(METAMASK_ADDRESS)
    if BESTWALLET_ADDRESS:
        out.append(BESTWALLET_ADDRESS)
    return out


def is_symbol_clean(sym: str) -> bool:
    s = (sym or "").upper().strip()
    if not s:
        return False
    if not SYM_OK.match(s):
        return False
    bad_fragments = ("HTTP", "HTTPS", "WWW.", "T.ME", "BIO.LINK", "T.LY", "WR.DO", "CLAIM", "VISIT")
    if any(f in s for f in bad_fragments):
        return False
    return True


# ---------------- Zapper GraphQL ----------------
_ZAPPER_GQL_URL = "https://public.zapper.xyz/graphql"

_GQL_WITH_CHAINS = """
query TokenBalances($addresses: [Address!]!, $first: Int!, $after: String, $chainIds: [Int!]) {
  portfolioV2(addresses: $addresses, chainIds: $chainIds) {
    tokenBalances {
      byToken(first: $first, after: $after) {
        edges {
          node {
            symbol
            decimals
            balanceRaw
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
        edges {
          node {
            symbol
            decimals
            balanceRaw
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


def _zapper_graphql_fetch_symbols(address: str) -> Tuple[Set[str], Optional[str]]:
    if not ZAPPER_API_KEY:
        return set(), "zapper:no-key"

    headers = {
        "x-zapper-api-key": ZAPPER_API_KEY,
        "content-type": "application/json",
        "accept": "application/json",
    }

    query = _GQL_WITH_CHAINS if ZAPPER_CHAIN_IDS else _GQL_ALL_CHAINS
    variables = {"addresses": [address], "first": 250, "after": None}
    if ZAPPER_CHAIN_IDS:
        variables["chainIds"] = ZAPPER_CHAIN_IDS

    out_syms: Set[str] = set()
    seen_cursors: Set[str] = set()
    loops = 0

    try:
        while True:
            loops += 1
            payload = {"query": query, "variables": variables}
            r = requests.post(_ZAPPER_GQL_URL, headers=headers, json=payload, timeout=30)

            if r.status_code != 200:
                return out_syms, f"zapper:{r.status_code}:{(r.text or '')[:200]}"

            j = r.json() or {}
            data = (j.get("data") or {})
            pf = data.get("portfolioV2") or {}
            tb = pf.get("tokenBalances") or {}
            bt = tb.get("byToken") or {}

            edges = bt.get("edges") or []
            for edge in edges:
                node = edge.get("node") or {}
                sym = (node.get("symbol") or "").upper().strip()
                dec = node.get("decimals") or 0
                raw = node.get("balanceRaw")
                mcap = ((node.get("onchainMarketData") or {}).get("marketCap") or 0) or 0.0

                if REQUIRE_SYMBOL_CLEAN and not is_symbol_clean(sym):
                    continue
                if MIN_MARKET_CAP_USD > 0 and (not isinstance(mcap, (int, float)) or float(mcap) < MIN_MARKET_CAP_USD):
                    continue

                try:
                    bal = float(raw) / (10 ** int(dec)) if raw is not None else 0.0
                except Exception:
                    bal = 0.0

                if sym and bal > 0:
                    out_syms.add(sym)

            page = bt.get("pageInfo") or {}
            has_next = bool(page.get("hasNextPage"))
            end_cursor = page.get("endCursor")
            if not has_next or not end_cursor or end_cursor in seen_cursors:
                break

            seen_cursors.add(end_cursor)
            variables["after"] = end_cursor

            if loops > 50:
                return out_syms, "zapper:safety:loops>50"

        return out_syms, None

    except Exception as e:
        return out_syms, f"zapper:exc:{e}"


# ---------------- Covalent/GoldRush fallback ----------------
def _fetch_covalent_tokens(address: str, chains: List[str]) -> Tuple[Set[str], Optional[str]]:
    key = COVALENT_API_KEY
    if not key or not chains:
        return set(), "covalent:no-key-or-chains"

    syms: Set[str] = set()
    errs = []

    for chain in chains:
        ok = False
        j = None

        # (1) Legacy query-param style
        try:
            url_qs = f"https://api.covalenthq.com/v1/{chain}/address/{address}/balances_v2/?key={key}"
            r = requests.get(url_qs, timeout=20)
            if r.status_code == 200:
                ok, j = True, (r.json() or {})
            else:
                errs.append(f"{chain}:qs:{r.status_code}")
        except Exception as e:
            errs.append(f"{chain}:qs_exc:{e}")

        # (2) Bearer header style (GoldRush)
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
            sym = (it.get("contract_ticker_symbol") or "").upper().strip()
            raw = it.get("balance")
            dec = it.get("contract_decimals") or 0
            try:
                bal = float(raw) / (10 ** int(dec)) if raw is not None else 0.0
            except Exception:
                bal = 0.0
            if sym and bal > 0:
                if REQUIRE_SYMBOL_CLEAN and not is_symbol_clean(sym):
                    continue
                syms.add(sym)

    return syms, (";".join(errs) if errs else None)


# ---------------- Unified wallet fetch ----------------
def fetch_wallet_tokens(address: str) -> Tuple[List[str], Dict[str, str]]:
    """
    Returns (symbols, diag) where diag contains provider diagnostics.
    """
    diag: Dict[str, str] = {}

    z_syms, z_err = _zapper_graphql_fetch_symbols(address)
    if z_syms:
        diag["provider"] = "zapper"
        return sorted(z_syms), diag

    diag["zapper_err"] = (z_err or "empty")
    c_syms, c_err = _fetch_covalent_tokens(address, WALLET_CHAINS)
    if c_syms:
        diag["provider"] = "covalent"
        diag["covalent_note"] = f"zapper_failed={diag['zapper_err']}"
        return sorted(c_syms), diag

    diag["covalent_err"] = (c_err or "empty")
    diag["provider"] = "none"
    return [], diag


def _tg(text: str, key: str) -> None:
    if not _tg_send:
        return
    try:
        ttl = WALLET_ALERT_DEDUP_MIN * 60
        # telegram_send_deduped signature: (text, dedup_key, ttl_sec=?)
        try:
            _tg_send(text, key, ttl_sec=ttl)  # type: ignore
        except TypeError:
            # fallback to send_telegram_message(text)
            _tg_send(text)  # type: ignore
    except Exception:
        pass


@with_sheet_backoff
def run_wallet_monitor() -> None:
    """
    On-chain wallet token arrivals monitor.
    Compares wallet tokens vs:
      - Claim_Tracker (Claimed? column)
      - Scout Decisions (Decision=YES allowlist)
    """
    if not WALLET_MONITOR_ENABLED:
        print("🔕 Wallet Monitor disabled (WALLET_MONITOR_ENABLED=0).")
        return
    if not SHEET_URL:
        print("⚠️ Wallet Monitor: SHEET_URL missing; skipping.")
        return

    addrs = _addresses()
    if not addrs:
        print("⚠️ Wallet Monitor: no wallet addresses configured; skipping.")
        return

    print("🔍 Running Wallet Monitor (on-chain)…")

    sheet = get_sheet()
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
        if (row.get("Token") or "").strip()
        and (row.get("Claimed?", "") or "").strip().lower() != "claimed"
    }
    approved_yes = {
        (row.get("Token") or "").strip().upper()
        for row in decision_data
        if (row.get("Decision") or "").strip().upper() == "YES"
        and (row.get("Token") or "").strip()
    }

    all_wallet_tokens: Set[str] = set()
    diags: List[str] = []

    for addr in addrs:
        toks, diag = fetch_wallet_tokens(addr)
        all_wallet_tokens.update(toks)
        if diag.get("provider") != "zapper":
            short = f"{addr[:6]}…{addr[-4:]}"
            diags.append(f"{short}:{diag.get('provider')} zapper={diag.get('zapper_err','')} covalent={diag.get('covalent_err','')}")

    if WALLET_ALLOWLIST_ONLY:
        before = set(all_wallet_tokens)
        all_wallet_tokens = all_wallet_tokens.intersection(approved_yes)
        dropped = before - all_wallet_tokens
        if dropped:
            print(f"ℹ️ Wallet Monitor: dropped {len(dropped)} non-allowlisted symbols (WALLET_ALLOWLIST_ONLY=1).")

    print(f"🧾 Wallet Tokens ({len(all_wallet_tokens)}): {sorted(list(all_wallet_tokens))}")
    if diags:
        print("ℹ️ Provider diagnostics: " + " | ".join(diags)[:400])

    if not all_wallet_tokens:
        print("⚠️ Wallet Monitor: providers returned no tokens. Check ZAPPER_API_KEY/COVALENT_API_KEY and allowlists.")
        return

    # Alert for arrivals: approved & not yet marked as claimed
    arrivals = [t for t in all_wallet_tokens if t in approved_yes and t not in claimed_tokens]

    if not arrivals:
        print("✅ Wallet Monitor: no unclaimed approved arrivals detected.")
        return

    for token in arrivals:
        msg = (
            f"⚠️ *{token}* has arrived in your on-chain wallet,\n"
            f"but is *not marked as claimed* in `{CLAIM_TAB}`.\n\n"
            f"Pending claims count: {len(pending_claims)}\n"
            f"Action: review and mark claimed when appropriate."
        )
        _tg(msg, key=f"wallet_arrival:{token}")

        # Update Status column to Resolved (if configured)
        if WALLET_AUTO_RESOLVE_STATUS:
            # best-effort: find token row and set Status column to Resolved if column exists
            for i, row in enumerate(claim_data, start=2):
                if (row.get("Token") or "").strip().upper() == token:
                    try:
                        # Column I in your original code; keep it but guard
                        claim_ws.update_acell(f"I{i}", "Resolved")
                    except Exception:
                        pass
                    break

        # Optional: auto-mark claimed (OFF by default)
        if WALLET_AUTO_MARK_CLAIMED:
            for i, row in enumerate(claim_data, start=2):
                if (row.get("Token") or "").strip().upper() == token:
                    try:
                        # attempt to set "Claimed?" column if it exists
                        # If the sheet has a header named "Claimed?", updating by cell index is safer
                        # but we keep simple behavior: update column that matches original structure if known.
                        claim_ws.update_acell(f"H{i}", "Claimed")
                    except Exception:
                        pass
                    break

    print(f"✅ Wallet Monitor complete. Alerts sent for: {arrivals}")


if __name__ == "__main__":
    run_wallet_monitor()
