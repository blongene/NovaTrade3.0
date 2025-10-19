import os, json

ROUTER_DEFAULT_VENUE = os.getenv("ROUTER_DEFAULT_VENUE","BINANCEUS")
# New: control fallback behavior explicitly.
#   'default' → fall back to ROUTER_DEFAULT_VENUE if no venue fits
#   'error'   → fail-closed (return None) so upstream policy can block
ROUTER_FALLBACK = os.getenv("ROUTER_FALLBACK","default").lower()

def route_intent(intent:dict, unified_snapshot_rows:list, policy_cfg:dict):
    """
    Returns a patched copy of `intent` choosing best venue/quote given:
      - policy_cfg['venue_order'] (preferred venues, list)
      - policy_cfg['prefer_quotes'][venue] (preferred quote symbol by venue)
      - whether we actually have quote liquidity on that venue (from Unified_Snapshot)
    `unified_snapshot_rows` is get_all_records() from Unified_Snapshot.
    If no venue has the preferred quote and ROUTER_FALLBACK=='error', returns None.
    If ROUTER_FALLBACK=='default', uses ROUTER_DEFAULT_VENUE as last resort.
    """
    patched = dict(intent)
    venue_order = policy_cfg.get("venue_order", [ROUTER_DEFAULT_VENUE])
    prefer = policy_cfg.get("prefer_quotes", {})

    # Build availability map: venue -> set(assets with >0 total)
    have = {}
    for r in unified_snapshot_rows:
        v = str(r.get("Venue","")).upper()
        a = str(r.get("Asset","")).upper()
        tot = float(r.get("Total",0) or 0)
        if tot <= 0: continue
        have.setdefault(v, set()).add(a)

    # Try ordered venues with preferred quote
    for v in venue_order:
        q = prefer.get(v, "USDT")
        assets = have.get(v, set())
        if q in assets:
            patched["venue"] = v
            patched["quote"] = q
            return patched

    # No venue w/ preferred quote found
    if ROUTER_FALLBACK == "error":
        return None

    # Fallback to default venue & its preferred quote
    v0 = venue_order[0] if venue_order else ROUTER_DEFAULT_VENUE
    patched["venue"] = v0
    patched["quote"] = prefer.get(v0, "USDT")
    return patched
