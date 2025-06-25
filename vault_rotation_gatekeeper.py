from vault_confidence_score import calculate_confidence
from utils import send_telegram_prompt

def gate_vault_rotation(token):
    confidence = calculate_confidence(token)

    if confidence < 70:
        send_telegram_prompt(
            token,
            message=f"{token} has low rotation confidence ({confidence}%). Re-rotate?",
            buttons=["🔁 Rotate It", "📦 Keep Vaulted", "🔕 Ignore It"],
            prefix="VAULT MEMORY CHECK"
        )
        return False
    else:
        return True
