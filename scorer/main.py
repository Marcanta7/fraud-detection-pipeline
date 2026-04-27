import json
import logging
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.live import Live

from producer.models import EnrichedTransactionEvent
from scorer.model import FraudScorer
from scorer.rules import run_rules
from scorer.router import route

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)
console = Console()


def build_table(stats: dict) -> Table:
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("metric", style="dim", width=28)
    table.add_column("value", justify="right")
    table.add_row("scored", str(stats["total"]))
    table.add_row("ALLOW",  f"[green]{stats['allow']}[/green]")
    table.add_row("REVIEW", f"[yellow]{stats['review']}[/yellow]")
    table.add_row("BLOCK",  f"[red]{stats['block']}[/red]")
    table.add_row(
        "block rate",
        f"{stats['block'] / max(stats['total'], 1) * 100:.2f}%"
    )
    table.add_row(
        "review rate",
        f"{stats['review'] / max(stats['total'], 1) * 100:.2f}%"
    )
    table.add_row("rule-only decisions", str(stats["rule_only"]))
    table.add_row("ml-only decisions",   str(stats["ml_only"]))
    table.add_row("ml+rule decisions",   str(stats["ml_rule"]))
    return table


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    input_topic = "transactions-enriched"
    alerts_topic = os.getenv("KAFKA_TOPIC_ALERTS", "fraud-alerts")
    scored_topic = "transactions-scored"

    scorer = FraudScorer()

    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=bootstrap,
        group_id="fraud-scorer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,
        consumer_timeout_ms=-1,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=5,
        compression_type="gzip",
    )

    stats = {
        "total": 0, "allow": 0, "review": 0, "block": 0,
        "rule_only": 0, "ml_only": 0, "ml_rule": 0,
    }

    console.print("\n[bold]Fraud scorer[/bold] — reading from "
                  f"[cyan]{input_topic}[/cyan]\n")

    with Live(build_table(stats), refresh_per_second=4, console=console) as live:
        for message in consumer:
            try:
                data = json.loads(message.value)
                enriched = EnrichedTransactionEvent(**data)
            except Exception as e:
                logger.warning(f"Failed to parse enriched event: {e}")
                continue

            # Score
            ml_result = scorer.score(enriched)
            rule_result = run_rules(enriched)
            decision = route(ml_result, rule_result)

            # Build output event
            scored_event = {
                "event_id": enriched.event.event_id,
                "transaction_id": enriched.event.transaction.transaction_id,
                "customer_id": enriched.event.customer.customer_id,
                "amount_minor": enriched.event.transaction.amount_minor,
                "currency": enriched.event.transaction.currency,
                "decision": decision.decision,
                "fraud_probability": decision.fraud_probability,
                "ml_decision": decision.ml_decision,
                "rule_triggered": decision.rule_triggered,
                "rule_name": decision.rule_name,
                "rule_reason": decision.rule_reason,
                "decision_source": decision.decision_source,
                "scored_at": datetime.now(timezone.utc).isoformat(),
            }

            # Always write to scored topic
            producer.send(
                scored_topic,
                key=enriched.event.customer.customer_id,
                value=scored_event,
            )

            # Write alerts for non-ALLOW decisions
            if decision.decision != "ALLOW":
                producer.send(
                    alerts_topic,
                    key=enriched.event.customer.customer_id,
                    value=scored_event,
                )

            # Update stats
            stats["total"] += 1
            stats[decision.decision.lower()] += 1
            if decision.decision_source == "RULE":
                stats["rule_only"] += 1
            elif decision.decision_source == "ML":
                stats["ml_only"] += 1
            elif decision.decision_source == "ML+RULE":
                stats["ml_rule"] += 1

            live.update(build_table(stats))

    consumer.close()
    producer.flush()
    producer.close()


if __name__ == "__main__":
    run()