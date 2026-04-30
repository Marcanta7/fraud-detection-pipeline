import json
import logging
import os
import pickle
import threading
import time
from pathlib import Path

import numpy as np
from producer.models import EnrichedTransactionEvent

logger = logging.getLogger(__name__)

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

BLOCK_THRESHOLD = 0.85
REVIEW_THRESHOLD = 0.40
MODEL_DIR = Path("scorer/model_artifacts")
HOT_RELOAD_INTERVAL = 60  # seconds


class FraudScorer:
    def __init__(self, model_path: str = "scorer/model_artifacts/fraud_model.pkl"):
        self._model_path = Path(model_path)
        self._model = self._load_model(self._model_path)
        self._current_version = self._read_version()
        self._lock = threading.RLock()

        # Start hot reload watcher in background
        self._watcher = threading.Thread(
            target=self._watch_for_new_model,
            daemon=True,
        )
        self._watcher.start()
        logger.info(
            f"Fraud model loaded — version={self._current_version}  "
            f"hot_reload_interval={HOT_RELOAD_INTERVAL}s"
        )

    def _load_model(self, path: Path):
        with open(path, "rb") as f:
            return pickle.load(f)

    def _read_version(self) -> str:
        meta_path = MODEL_DIR / "model_metadata.json"
        if meta_path.exists():
            with open(meta_path) as f:
                return json.load(f).get("version", "unknown")
        return "initial"

    def _watch_for_new_model(self):
        """Background thread — checks for new model version every 60s."""
        while True:
            time.sleep(HOT_RELOAD_INTERVAL)
            try:
                latest_version = self._read_version()
                if latest_version != self._current_version:
                    logger.info(
                        f"New model detected — "
                        f"{self._current_version} → {latest_version}  "
                        f"reloading..."
                    )
                    new_model = self._load_model(self._model_path)
                    with self._lock:
                        self._model = new_model
                        self._current_version = latest_version
                    logger.info(f"Model hot-reloaded — version={latest_version}")
            except Exception as e:
                logger.warning(f"Hot reload check failed: {e}")

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

        with self._lock:
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
            "model_version": self._current_version,
            "block_threshold": BLOCK_THRESHOLD,
            "review_threshold": REVIEW_THRESHOLD,
        }