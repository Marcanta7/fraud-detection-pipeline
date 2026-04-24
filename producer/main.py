import json
import os
import time
import random
from kafka import KafkaProducer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.live import Live

from producer.generator import generate_event

load_dotenv()
console = Console()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        # Tuned for low-latency local dev
        linger_ms=5,
        batch_size=16384,
        compression_type="gzip",
    )


def build_table(stats: dict) -> Table:
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("metric", style="dim", width=28)
    table.add_column("value", justify="right")
    table.add_row("events sent", str(stats["total"]))
    table.add_row("fraud events", f"[red]{stats['fraud']}[/red]")
    table.add_row("legit events", f"[green]{stats['legit']}[/green]")
    table.add_row(
        "fraud rate",
        f"{stats['fraud'] / max(stats['total'], 1) * 100:.1f}%"
    )
    table.add_row("events/sec", f"{stats['eps']:.1f}")
    return table


def run() -> None:
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-raw")
    eps = float(os.getenv("PRODUCER_EVENTS_PER_SECOND", "10"))
    fraud_rate = float(os.getenv("PRODUCER_FRAUD_RATE", "0.02"))
    sleep = 1.0 / eps

    producer = make_producer()
    stats = {"total": 0, "fraud": 0, "legit": 0, "eps": eps}

    console.print(
        f"\n[bold]Fraud pipeline producer[/bold] — "
        f"topic=[cyan]{topic}[/cyan]  "
        f"rate=[cyan]{eps}/s[/cyan]  "
        f"fraud=[cyan]{fraud_rate*100:.0f}%[/cyan]\n"
    )

    with Live(build_table(stats), refresh_per_second=4, console=console) as live:
        while True:
            is_fraud = random.random() < fraud_rate
            event = generate_event(is_fraud=is_fraud)

            producer.send(
                topic,
                key=event.customer.customer_id,   # partition by customer
                value=event.model_dump(),
            )

            stats["total"] += 1
            if is_fraud:
                stats["fraud"] += 1
            else:
                stats["legit"] += 1

            live.update(build_table(stats))
            time.sleep(sleep)


if __name__ == "__main__":
    run()