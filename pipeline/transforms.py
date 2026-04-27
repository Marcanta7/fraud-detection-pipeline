import json
import logging
from datetime import datetime, timezone

from feature_store.redis_client import get_redis, get_customer_features
from producer.models import (
    TransactionAuthorisationEvent,
    FraudFeatures,
    EnrichedTransactionEvent,
)

HIGH_RISK_MCCS = {"7995", "4829", "6011"}
HIGH_RISK_CHANNELS = {"ecommerce", "atm"}

logger = logging.getLogger(__name__)


class ParseEvent:
    def process(self, element):
        try:
            raw = json.loads(element.decode("utf-8") if isinstance(element, bytes) else element)
            event = TransactionAuthorisationEvent(**raw)
            return event, False  # (event, is_dead_letter)
        except Exception as e:
            logger.warning(f"Failed to parse event: {e}")
            return element, True


class EnrichWithFeatures:
    def setup(self):
        self._redis = get_redis()

    def teardown(self):
        self._redis.close()

    def process(self, event: TransactionAuthorisationEvent):
        customer_features = get_customer_features(
            self._redis, event.customer.customer_id
        )

        avg = customer_features["avg_spend_minor"]
        amount = event.transaction.amount_minor
        ratio = round(amount / avg, 3) if avg > 0 else 99.0

        features = FraudFeatures(
            avg_spend_minor=avg,
            txn_count_1h=customer_features["txn_count_1h"],
            distinct_countries_24h=customer_features["distinct_countries_24h"],
            known_mccs=customer_features["known_mccs"],
            is_cold_start=customer_features.get("is_cold_start", False),
            amount_vs_avg_ratio=ratio,
            is_known_mcc=event.transaction.mcc in customer_features["known_mccs"],
            is_high_risk_mcc=event.transaction.mcc in HIGH_RISK_MCCS,
            is_high_risk_channel=event.transaction.channel in HIGH_RISK_CHANNELS,
        )

        enriched = EnrichedTransactionEvent(
            event=event,
            features=features,
            enriched_at=datetime.now(timezone.utc).isoformat(),
        )

        return enriched


class SerialiseToJson:
    def process(self, enriched: EnrichedTransactionEvent):
        return json.dumps(enriched.model_dump()).encode("utf-8")