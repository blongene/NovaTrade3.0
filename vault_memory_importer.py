# vault_memory_importer.py — Phase-6 Safe
import os, random, time
from utils import (
    get_ws_cached, get_records_cached, ws_batch_update,
    str_or_empty, invalidate_tab, warn
)

EVAL_TAB="Vault_Memory_Eval"
TARGET_TAB="Rotation_Stats"
TARGET_COL_NAME="Vault Memory"
JIT_MIN=float(os.getenv("VAULTMEM_JITTER_MIN_S","0.3"))
JIT_MAX=float(os.getenv("VAULTMEM_JITTER_MAX_S","1.2"))

def _col_letter(n):
    s=""
    while n: n,r=divmod(n-1,26); s=chr(65+r)+s
    return s

def run_vault_memory_importer():
    try:
        time.sleep(random.uniform(JIT_MIN,JIT_MAX))
        print("📥 Vault memory importer …")
        eval_rows=get_records_cached(EVAL_TAB,ttl_s=300)
        target_rows=get_records_cached(TARGET_TAB,ttl_s=300)
        if not eval_rows or not target_rows: return
        scores={str_or_empty(r.get("Token")).upper(): str_or_empty(r.get("Score")) for r in eval_rows if r.get("Score")}
        ws=get_ws_cached(TARGET_TAB,ttl_s=60)
        header=ws.row_values(1)
        col_ix=(header.index(TARGET_COL_NAME)+1) if TARGET_COL_NAME in header else len(header)+1
        writes=[]
        if TARGET_COL_NAME not in header:
            writes.append({"range":f"{_col_letter(col_ix)}1","values":[[TARGET_COL_NAME]]})
        for i,r in enumerate(target_rows,start=2):
            token=str_or_empty(r.get("Token")).upper()
            if not token: continue
            new=scores.get(token,""); cur=str_or_empty(r.get(TARGET_COL_NAME))
            if new and new!=cur:
                writes.append({"range":f"{_col_letter(col_ix)}{i}","values":[[new]]})
        if writes:
            ws_batch_update(ws,writes)
            invalidate_tab(TARGET_TAB)
            print(f"✅ Vault memory importer: {len(writes)} cell(s) updated.")
    except Exception as e:
        warn(f"vault_memory_importer: {e}")
