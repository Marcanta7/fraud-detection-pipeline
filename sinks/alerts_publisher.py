import json
import logging
import os

from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def format_alert(data: dict) -> str:
    emoji = "BLOCK" if data["decision"] == "BLOCK" else "REVIEW"
    amount = data["amount_minor"] / 100
    currency = data["currency"]
    prob = data["fraud_probability"] * 100

    lines = [
        f"[{emoji}] {'='*50}",
        f"  Transaction : {data['transaction_id']}",
        f"  Customer    : {data['customer_id']}",
        f"  Amount      : {currency} {amount:.2f}",
        f"  Fraud prob  : {prob:.1f}%",
        f"  Source      : {data['decision_source']}",
    ]

    if data.get("rule_name"):
        lines.append(f"  Rule        : {data['rule_name']}")
        lines.append(f"  Reason      : {data['rule_reason']}")

    return "\n".join(lines)


def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    alerts_topic = os.getenv("KAFKA_TOPIC_ALERTS", "fraud-alerts")

    consumer = KafkaConsumer(
        alerts_topic,
        bootstrap_servers=bootstrap,
        group_id="alerts-publisher",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
    )

    stats = {"block": 0, "review": 0}
    logger.info(f"Alerts publisher running — consuming {alerts_topic}")

    for message in consumer:
        data = message.value
        decision = data.get("decision", "")

        if decision == "BLOCK":
            stats["block"] += 1
            print(format_alert(data))
            print()
        elif decision == "REVIEW":
            stats["review"] += 1
            if stats["review"] % 20 == 0:
                logger.info(
                    f"Review queue — "
                    f"reviews: {stats['review']}  "
                    f"blocks: {stats['block']}"
                )


if __name__ == "__main__":
    run()