# rotation_binance_executor.py — LEGACY DISABLED
#
# This module used to talk directly to Binance with API keys and execute
# buys from Rotation_Log. In NovaTrade 3.x, ALL trading must flow through:
#
#   • The Bus (Outbox DB or /api/ops/enqueue)
#   • PolicyEngine + C-Series venue budgets (trade_guard)
#   • Edge Agents behind the command bus
#
# Direct venue API calls from the Bus are no longer allowed. This file is
# intentionally neutered for capital safety.

def run_rotation_binance_executor():
    msg_lines = [
        "",
        "⚠️ rotation_binance_executor is DISABLED in NovaTrade 3.x.",
        "",
        "Direct Binance API trading from the Bus is no longer supported.",
        "All rotation and rebuy execution should be done via the Outbox command bus,",
        "PolicyEngine, and Edge Agents (MEXC/CB/Kraken) using trade_guard.",
        "",
        "If you see this message, something is still calling run_rotation_binance_executor().",
        "Please update that job or script to use the new rotation/command path instead.",
        "",
    ]
    print("\n".join(msg_lines))
