# vault_confidence_score.py — use utils wrappers, keep same scoring policy
import os
from utils import get_all_records_cached, safe_float, str_or_empty

def calculate_confidence(token: str) -> int:
    try:
        rows = get_all_records_cached("Rotation_Stats", ttl_s=300) or []
        t_upper = str(token).strip().upper()

        for row in rows:
            row_token = str_or_empty(row.get("Token")).upper()
            if row_token != t_upper:
                continue

            score = safe_float(row.get("Memory Vault Score"), default=0) or 0
            # Map score to confidence %
            if score >= 5:
                return 90
            elif score >= 3:
                return 70
            elif score >= 1:
                return 50
            else:
                return 20

        return 0  # not found

    except Exception as e:
        print(f"❌ Confidence Score error for {token}: {e}")
        return 0
