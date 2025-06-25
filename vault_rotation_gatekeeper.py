# vault_rotation_gatekeeper.py

from vault_confidence_score import calculate_confidence
from rotation_signal_engine import scan_rotation_candidates
from utils import send_telegram_prompt

def gate_vault_rotation(token):
    print(f"🚀 Executing vault gatekeeper for: {token}")
    try:
        confidence = calculate_confidence(token)
        print(f"📊 Confidence score for {token}: {confidence}%")

        if confidence >= 70:
            print(f"✅ Confidence threshold met — rotating {token}.")
            scan_rotation_candidates(token_override=token)
        else:
            print(f"⚠️ Confidence too low for auto-rotation: {confidence}%")
            send_telegram_prompt(
                token,
                f"🔒 {token} has a vault confidence score of {confidence}%. Rotate anyway?",
                buttons=["YES", "NO"],
                prefix="MANUAL ROTATE"
            )
    except Exception as e:
        print(f"❌ Error in gate_vault_rotation for {token}: {e}")
