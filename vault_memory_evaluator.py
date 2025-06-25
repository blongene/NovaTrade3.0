import gspread
from utils import get_sheet
from datetime import datetime

def evaluate_vault_memory(token):
    sheet = get_sheet()
    roi_ws = sheet.worksheet("Vault_ROI_Tracker")
    feedback_ws = sheet.worksheet(os.getenv("VAULT_REVIEW_SHEET", "ROI_Review_Log"))

    roi_data = roi_ws.get_all_records()
    feedback_data = feedback_ws.get_all_records()

    token_records = [row for row in roi_data if row.get("Token", "").upper() == token.upper()]
    feedback_records = [row for row in feedback_data if row.get("Token", "").upper() == token.upper()]

    if not token_records:
        return {"token": token, "avg_roi": 0, "max_roi": 0, "rebuy_roi": 0, "memory_score": 0}

    roi_values = [float(row.get("Vault ROI", 0)) for row in token_records if row.get("Vault ROI")]
    rebuy_roi = sum([float(row.get("Rebuy ROI", 0)) for row in token_records if row.get("Rebuy ROI")]) / max(len(token_records),1)

    max_roi = max(roi_values)
    avg_roi = sum(roi_values) / len(roi_values)

    feedback_weight = 0
    for row in feedback_records:
        val = row.get("Decision", "").strip().lower()
        if val == "smart":
            feedback_weight += 2
        elif val == "too soon":
            feedback_weight -= 1
        elif val == "should’ve held":
            feedback_weight += 1

    memory_score = (avg_roi + max_roi + rebuy_roi + feedback_weight) / 4
    return {
        "token": token,
        "avg_roi": round(avg_roi, 2),
        "max_roi": round(max_roi, 2),
        "rebuy_roi": round(rebuy_roi, 2),
        "memory_score": round(memory_score, 2)
    }
