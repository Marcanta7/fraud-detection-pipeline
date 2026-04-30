import json
import logging
import os
from datetime import datetime, timezone

from google.cloud import bigquery
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("GCP_DATASET_ID", "fraud_pipeline")
BATCH_SIZE = 100
BATCH_TIMEOUT_SEC = 5


def parse_row(data: dict) -> dict:
    return {
        "event_id":          data["event_id"],
        "transaction_id":    data["transaction_id"],
        "customer_id":       data["customer_id"],
        "amount_minor":      int(data["amount_minor"]),
        "currency":          data["currency"],
        "decision":          data["decision"],
        "fraud_probability": float(data["fraud_probability"]),
        "ml_decision":       data["ml_decision"],
        "rule_triggered":    bool(data["rule_triggered"]),
        "rule_name":         data.get("rule_name"),
        "rule_reason":       data.get("rule_reason"),
        "decision_source":   data["decision_source"],
        "scored_at":         data["scored_at"],
    }


def flush(client: bigquery.Client, table_id: str, rows: list) -> int:
    if not rows:
        return 0
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
        return 0
    return len(rows)


def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    scored_topic = "transactions-scored"
    alerts_topic = os.getenv("KAFKA_TOPIC_ALERTS", "fraud-alerts")

    bq = bigquery.Client(project=PROJECT)
    scored_table = f"{PROJECT}.{DATASET}.transactions_scored"
    alerts_table = f"{PROJECT}.{DATASET}.fraud_alerts"

    consumer = KafkaConsumer(
        scored_topic,
        alerts_topic,
        bootstrap_servers=bootstrap,
        group_id="bigquery-sink",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
    )

    scored_batch = []
    alerts_batch = []
    total_inserted = 0
    last_flush = datetime.now(timezone.utc).timestamp()

    logger.info(f"BigQuery sink running — project={PROJECT} dataset={DATASET}")

    for message in consumer:
        try:
            row = parse_row(message.value)

            if message.topic == scored_topic:
                scored_batch.append(row)
            elif message.topic == alerts_topic:
                alerts_batch.append(row)

            now = datetime.now(timezone.utc).timestamp()
            should_flush = (
                len(scored_batch) >= BATCH_SIZE
                or len(alerts_batch) >= BATCH_SIZE
                or (now - last_flush) >= BATCH_TIMEOUT_SEC
            )

            if should_flush:
                n_scored = flush(bq, scored_table, scored_batch)
                n_alerts = flush(bq, alerts_table, alerts_batch)
                total_inserted += n_scored + n_alerts

                logger.info(
                    f"Flushed — scored: {n_scored}  "
                    f"alerts: {n_alerts}  "
                    f"total: {total_inserted}"
                )

                scored_batch.clear()
                alerts_batch.clear()
                last_flush = now

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            continue


if __name__ == "__main__":
    run()