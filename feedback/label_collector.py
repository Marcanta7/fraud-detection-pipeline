import json
import logging
import os
import random
from datetime import datetime, timezone

from google.cloud import bigquery
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("GCP_DATASET_ID", "fraud_pipeline")

# Simulation parameters — tweak these to generate more labels faster
CHARGEBACK_RATE = 0.30       # 30% of BLOCK decisions get a chargeback
OPS_CONFIRM_RATE = 0.25      # 25% of REVIEW decisions confirmed as fraud by ops
AUTO_LABEL_THRESHOLD = 0.90  # BLOCK + prob > 0.90 → auto-label as fraud
LEGIT_LABEL_RATE = 0.05      # 5% of ALLOW decisions labelled as legit (for balance)


def make_label_event(scored: dict, label: int, source: str) -> dict:
    return {
        "event_id":         scored["event_id"],
        "transaction_id":   scored["transaction_id"],
        "customer_id":      scored["customer_id"],
        "label":            label,
        "label_source":     source,
        "original_decision": scored["decision"],
        "fraud_probability": scored["fraud_probability"],
        "rule_name":        scored.get("rule_name"),
        "amount_minor":     scored["amount_minor"],
        "currency":         scored["currency"],
        "confirmed_at":     datetime.now(timezone.utc).isoformat(),
        "scored_at":        scored["scored_at"],
    }


def write_label_to_bigquery(bq: bigquery.Client, label_event: dict):
    table_id = f"{PROJECT}.{DATASET}.fraud_labels"
    errors = bq.insert_rows_json(table_id, [label_event])
    if errors:
        logger.error(f"BigQuery label insert error: {errors}")


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

    bq = bigquery.Client(project=PROJECT)

    consumer = KafkaConsumer(
        "transactions-scored",
        bootstrap_servers=bootstrap,
        group_id="label-collector",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=5,
    )

    stats = {
        "processed": 0,
        "chargebacks": 0,
        "ops_confirmed": 0,
        "auto_labelled": 0,
        "legit_labelled": 0,
    }

    logger.info("Label collector running — watching transactions-scored")

    for message in consumer:
        scored = message.value
        decision = scored.get("decision", "")
        prob = scored.get("fraud_probability", 0.0)
        label_event = None

        # Source 1 — chargebacks (delayed fraud confirmation on BLOCKs)
        if decision == "BLOCK" and random.random() < CHARGEBACK_RATE:
            label_event = make_label_event(scored, label=1, source="chargeback")
            stats["chargebacks"] += 1

        # Source 2 — ops team confirms REVIEW decisions
        elif decision == "REVIEW" and random.random() < OPS_CONFIRM_RATE:
            label_event = make_label_event(scored, label=1, source="ops_review")
            stats["ops_confirmed"] += 1

        # Source 3 — auto-label very high confidence BLOCKs
        elif decision == "BLOCK" and prob >= AUTO_LABEL_THRESHOLD:
            label_event = make_label_event(scored, label=1, source="auto_label")
            stats["auto_labelled"] += 1

        # Source 4 — label some ALLOWs as legit (gives model negative examples)
        elif decision == "ALLOW" and random.random() < LEGIT_LABEL_RATE:
            label_event = make_label_event(scored, label=0, source="ops_review")
            stats["legit_labelled"] += 1

        if label_event:
            # Publish to Kafka
            producer.send(
                "confirmed-fraud",
                key=scored["customer_id"],
                value=label_event,
            )
            # Write to BigQuery
            write_label_to_bigquery(bq, label_event)

        stats["processed"] += 1

        if stats["processed"] % 100 == 0:
            total_labels = (
                stats["chargebacks"] +
                stats["ops_confirmed"] +
                stats["auto_labelled"] +
                stats["legit_labelled"]
            )
            logger.info(
                f"processed={stats['processed']}  "
                f"labels={total_labels}  "
                f"chargebacks={stats['chargebacks']}  "
                f"ops={stats['ops_confirmed']}  "
                f"auto={stats['auto_labelled']}  "
                f"legit={stats['legit_labelled']}"
            )


if __name__ == "__main__":
    run()