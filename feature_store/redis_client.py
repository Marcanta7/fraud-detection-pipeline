import json
import os
import random
from datetime import datetime, timezone
from redis import Redis
from dotenv import load_dotenv

load_dotenv()


def get_redis() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )


def get_customer_features(r: Redis, customer_id: str) -> dict:
    """
    Look up behavioural features for a customer.
    Returns defaults for unknown customers (cold start).
    """
    raw = r.get(f"features:{customer_id}")
    if raw:
        return json.loads(raw)

    # Cold start — unknown customer, treat as higher risk
    return {
        "avg_spend_minor": 0,
        "txn_count_1h": 0,
        "distinct_countries_24h": 0,
        "known_mccs": [],
        "is_cold_start": True,
    }


def set_customer_features(r: Redis, customer_id: str, features: dict) -> None:
    r.set(f"features:{customer_id}", json.dumps(features), ex=86400)


def seed_features(n_customers: int = 500) -> None:
    """Seed Redis with realistic baseline features for fake customers."""
    r = get_redis()
    mccs = ["5411", "5812", "5541", "4111", "5912", "5311", "7011"]

    for i in range(n_customers):
        customer_id = f"cust_{i:06d}"
        features = {
            "avg_spend_minor": random.randint(500, 15000),
            "txn_count_1h": random.randint(0, 3),
            "distinct_countries_24h": random.randint(1, 2),
            "known_mccs": random.sample(mccs, k=random.randint(2, 5)),
            "is_cold_start": False,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        set_customer_features(r, customer_id, features)

    print(f"Seeded features for {n_customers} customers")


if __name__ == "__main__":
    seed_features()