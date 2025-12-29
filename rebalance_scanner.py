# rebalance_scanner.py — Phase-6 Safe
import os, time, random
from utils import (
    get_ws_cached, get_values_cached, ws_batch_update,
    str_or_empty, invalidate_tab, warn,
    get_all_records_cached_dbaware
)

SRC_TAB="Rotation_Stats"
DST_TAB="Rotation_Planner"
TTL_READ_S=int(os.getenv("REBAL_TTL_READ_SEC","300"))
MAX_WRITES=int(os.getenv("REBAL_MAX_WRITES","200"))
JIT_MIN=float(os.getenv("REBAL_JITTER_MIN_S","0.5"))
JIT_MAX=float(os.getenv("REBAL_JITTER_MAX_S","2.0"))

NEEDED_SRC=["Token","Target %","Current %","Suggested %"]
PLAN_HEADERS=["Token","Suggested Action","Reason"]

def _col_letter(n):
    s=""
    while n: n,r=divmod(n-1,26); s=chr(65+r)+s
    return s

def _values_to_records(vals):
    if not vals: return []
    hdr, out = vals[0], []
    for row in vals[1:]:
        out.append({hdr[i] if i<len(hdr) else f"c{i}": row[i] if i<len(row) else "" for i in range(len(hdr))})
    return out

def _to_float(v,default=None):
    try: return float(str(v).replace("%","").replace(",","").strip() or default)
    except Exception: return default

def run_rebalance_scanner():
    try:
        time.sleep(random.uniform(JIT_MIN,JIT_MAX))
        # Phase 22B (DB-aware): pull Rotation_Stats via DB mirror when available.
        # Falls back to Sheets automatically.
        stats = get_all_records_cached_dbaware(
            SRC_TAB,
            ttl_s=TTL_READ_S,
            logical_stream=f"sheet_mirror:{SRC_TAB}",
        )
        planner_vals=get_values_cached(DST_TAB,ttl_s=TTL_READ_S)
        if not stats:
            return
        plan_hdr=planner_vals[0] if planner_vals else []
        changed=False
        for h in PLAN_HEADERS:
            if h not in plan_hdr: plan_hdr.append(h); changed=True
        plan_col={h:i+1 for i,h in enumerate(plan_hdr)}
        plan_idx={}
        tok_col=plan_col.get("Token")
        if tok_col:
            for i,row in enumerate(planner_vals[1:],start=2):
                tok=str_or_empty(row[tok_col-1] if tok_col-1<len(row) else "").upper()
                if tok and tok not in plan_idx: plan_idx[tok]=i
        writes=[]; touched=0; suggestions=[]
        if changed:
            writes.append({"range":f"A1:{_col_letter(len(plan_hdr))}1","values":[plan_hdr]})
        for r in stats:
            token=str_or_empty(r.get("Token")).upper()
            if not token: continue
            tgt=_to_float(r.get("Target %")); cur=_to_float(r.get("Current %"))
            if tgt is None or cur is None: continue
            delta=cur-tgt
            if abs(delta)<1.0: continue
            act="SELL" if delta>0 else "BUY"
            reason=f"Δ={delta:+.2f}pp (cur={cur:.2f}%, tgt={tgt:.2f}%)"
            row=[""]*len(plan_hdr)
            row[plan_col["Token"]-1]=token
            row[plan_col["Suggested Action"]-1]=act
            row[plan_col["Reason"]-1]=reason
            if token in plan_idx:
                ri=plan_idx[token]
                writes.append({"range":f"A{ri}:{_col_letter(len(plan_hdr))}{ri}","values":[row]})
            else:
                suggestions.append(row)
            touched+=1
            if touched>=MAX_WRITES: break
        if suggestions:
            start=(len(planner_vals) if planner_vals else 1)+1
            writes.append({"range":f"A{start}","values":suggestions})
        if writes:
            ws=get_ws_cached(DST_TAB,ttl_s=60)
            ws_batch_update(ws,writes)
            invalidate_tab(DST_TAB)
            print(f"✅ Rebalance scanner wrote {touched} suggestion(s).")
        else:
            print("✅ Rebalance scanner: no changes.")
    except Exception as e:
        warn(f"rebalance_scanner: {e}")
