from vault_memory_evaluator import evaluate_vault_memory

def calculate_confidence(token):
    memory = evaluate_vault_memory(token)

    # Scale and weight memory score
    score = memory['memory_score']
    if score > 80:
        return 95
    elif score > 60:
        return 85
    elif score > 40:
        return 70
    elif score > 20:
        return 50
    else:
        return 30
