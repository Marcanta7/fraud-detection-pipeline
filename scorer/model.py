import pickle
import numpy as np
from producer.models import EnrichedTransactionEvent

FEATURES = [
    "amount_vs_avg_ratio",
    "txn_count_1h",
    "distinct_countries_24h",
    "account_age_days",
    "is_cold_start",
    "is_known_mcc",
    "is_high_risk_mcc",
    "is_high_risk_channel",
    "is_3ds",
    "is_virtual_card",
]

# Thresholds — tuned for ops team capacity
BLOCK_THRESHOLD = 0.85   # very high confidence → auto block
REVIEW_THRESHOLD = 0.40  # moderate confidence → human review


class FraudScorer:
    def __init__(self, model_path: str = "scorer/model_artifacts/fraud_model.pkl"):
        with open(model_path, "rb") as f:
            self._model = pickle.load(f)
        print(f"Fraud model loaded from {model_path}")

    def score(self, enriched: EnrichedTransactionEvent) -> dict:
        features = enriched.features
        event = enriched.event

        X = np.array([[
            features.amount_vs_avg_ratio,
            features.txn_count_1h,
            features.distinct_countries_24h,
            event.customer.account_age_days,
            int(features.is_cold_start),
            int(features.is_known_mcc),
            int(features.is_high_risk_mcc),
            int(features.is_high_risk_channel),
            int(event.transaction.is_3ds),
            int(event.card.is_virtual),
        ]])

        fraud_probability = float(self._model.predict_proba(X)[0][1])

        if fraud_probability >= BLOCK_THRESHOLD:
            ml_decision = "BLOCK"
        elif fraud_probability >= REVIEW_THRESHOLD:
            ml_decision = "REVIEW"
        else:
            ml_decision = "ALLOW"

        return {
            "fraud_probability": round(fraud_probability, 4),
            "ml_decision": ml_decision,
            "block_threshold": BLOCK_THRESHOLD,
            "review_threshold": REVIEW_THRESHOLD,
        }