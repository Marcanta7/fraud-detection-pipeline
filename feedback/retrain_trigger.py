import json
import logging
import os
import subprocess
from datetime import datetime, timezone

from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

RETRAIN_THRESHOLD = int(os.getenv("RETRAIN_THRESHOLD", "50"))  # low for dev
FRAUD_LABEL_MIN = int(os.getenv("FRAUD_LABEL_MIN", "10"))      # min fraud labels


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

    consumer = KafkaConsumer(
        "confirmed-fraud",
        bootstrap_servers=bootstrap,
        group_id="retrain-trigger",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
    )

    label_counts = {"fraud": 0, "legit": 0, "total": 0}
    last_retrain_at = None

    logger.info(
        f"Retraining trigger running — "
        f"threshold={RETRAIN_THRESHOLD} labels  "
        f"min_fraud={FRAUD_LABEL_MIN}"
    )

    for message in consumer:
        label_event = message.value
        label = label_event.get("label", -1)

        if label == 1:
            label_counts["fraud"] += 1
        elif label == 0:
            label_counts["legit"] += 1
        label_counts["total"] += 1

        # Check if we should retrain
        should_retrain = (
            label_counts["total"] >= RETRAIN_THRESHOLD
            and label_counts["fraud"] >= FRAUD_LABEL_MIN
        )

        if should_retrain:
            logger.info(
                f"Retraining threshold reached — "
                f"total={label_counts['total']}  "
                f"fraud={label_counts['fraud']}  "
                f"legit={label_counts['legit']}"
            )

            # Fire retraining job
            logger.info("Firing retraining job...")
            result = subprocess.run(
                ["uv", "run", "python", "-m", "feedback.retrain_job"],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                logger.info("Retraining job completed successfully")
                logger.info(result.stdout)
                last_retrain_at = datetime.now(timezone.utc)
            else:
                logger.error(f"Retraining job failed: {result.stderr}")

            # Reset counters after retraining
            label_counts = {"fraud": 0, "legit": 0, "total": 0}
            logger.info(
                f"Label counters reset — "
                f"last retrain: {last_retrain_at}"
            )

        elif label_counts["total"] % 10 == 0:
            logger.info(
                f"Labels accumulated — "
                f"total={label_counts['total']}/{RETRAIN_THRESHOLD}  "
                f"fraud={label_counts['fraud']}  "
                f"legit={label_counts['legit']}"
            )


if __name__ == "__main__":
    run()