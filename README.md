# 🔍 Real-Time Fraud Detection Pipeline

A production-grade, real-time card transaction fraud detection system built on GCP, Apache Kafka, Apache Beam, XGBoost, and Redis. Designed to score transactions in **< 200ms end-to-end latency** at **10,000+ events/sec throughput**.

This project demonstrates modern data engineering practices including streaming pipelines, feature engineering, ML model serving, dead letter routing, and agentic AI integration — targeting fintech, banking, and insurance engineering roles.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          INGEST LAYER                               │
│                                                                     │
│   Card Transactions  ──►  Pub/Sub (transactions-raw)  ──►  Schema  │
│   (POS, ecommerce,                                        Registry  │
│    ATM, contactless)                                    (Avro 2.4)  │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                        STREAM LAYER                                  │
│                                                                     │
│   Dataflow Enrichment  ──►  Feature Store  ──►  Dead Letter Queue  │
│   (Apache Beam)             (Vertex AI /         (malformed         │
│   - schema validation        Redis cache)         events)           │
│   - feature lookup                                                  │
│   - feature engineering                                             │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                        SCORING LAYER                                 │
│                                                                     │
│   Fraud Score Model  ──►  Rule Engine  ──►  Decision Router        │
│   (XGBoost /               (velocity,         ALLOW / REVIEW /     │
│    Vertex AI)               geo, amount,       BLOCK               │
│                             MCC risk)                               │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│                          SINK LAYER                                  │
│                                                                     │
│   BigQuery  ◄──  All transactions    Alerts Pub/Sub  ──►  Looker   │
│   (partitioned      (scored)         (BLOCK/REVIEW        Dashboard │
│    by day,                            events)             (ops      │
│    clustered by                                           monitoring)│
│    customer_id)                                                     │
└─────────────────────────────────────────────────────────────────────┘

Target SLA: < 200ms latency · 10k+ events/sec · 99.9% availability
```

---

## 🛠 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingest | Apache Kafka (Confluent) | Durable event buffer, decouples producers from consumers |
| Ingest | Avro + Schema Registry | Schema enforcement and evolution |
| Stream | Apache Beam / GCP Dataflow | Scalable enrichment transforms |
| Feature Store | Redis + Vertex AI Feature Store | Sub-millisecond feature lookups at inference time |
| Scoring | XGBoost on Vertex AI | Fraud probability score |
| Scoring | Python rule engine | Deterministic velocity, geo and amount rules |
| Sink | Google BigQuery | All scored transactions, partitioned for analytics |
| Sink | GCP Pub/Sub | Downstream alert routing |
| Monitoring | Looker Studio | Fraud ops dashboard |
| Local dev | Docker Compose | Kafka, Zookeeper, Redis, Kafka UI |
| Language | Python 3.11 + uv | Fast, modern dependency management |
| Validation | Pydantic v2 | Event models with strict type enforcement |

---

## 📦 Repository Structure

```
fraud-detection-pipeline/
│
├── docker-compose.yml          # Kafka + Zookeeper + Redis + Kafka UI
├── pyproject.toml              # uv project config and dependencies
├── .env.example                # environment variable template
│
├── schemas/
│   └── transaction_authorisation.avsc   # Avro schema v2.4.0
│
├── infra/
│   └── kafka_admin.py          # creates Kafka topics on startup
│
├── producer/
│   ├── models.py               # Pydantic event models
│   ├── generator.py            # realistic fake transaction factory
│   └── main.py                 # publishes to Kafka with live stats
│
├── feature_store/
│   └── redis_client.py         # feature lookups + seeder (500 customers)
│
├── pipeline/
│   ├── transforms.py           # ParseEvent, EnrichWithFeatures, SerialiseToJson
│   └── main.py                 # Kafka consumer loop driving the pipeline
│
├── scorer/                     # Session 3 — coming soon
│   ├── model.py
│   ├── features.py
│   └── rules.py
│
└── sinks/                      # Session 4 — coming soon
    ├── bigquery_sink.py
    └── alerts_publisher.py
```

---

## 🗂 Kafka Topics

| Topic | Partitions | Description |
|-------|-----------|-------------|
| `transactions-raw` | 6 | Raw authorisation events from the producer |
| `transactions-enriched` | 6 | Events with customer behaviour features attached |
| `fraud-alerts` | 3 | BLOCK and REVIEW decisions for downstream consumers |
| `transactions-dead-letter` | 2 | Malformed or unparseable events |

---

## 📋 Event Payload

Each card transaction emits a `transaction.authorisation` event. Key design decisions:

- **`amount_minor`** — stored as `long` integer (pence/cents), never float. Avoids floating point precision bugs on financial amounts.
- **`card_id`** — internal token, never the raw PAN. Keeps the pipeline outside PCI-DSS scope.
- **`event_id`** — ULID (not UUID4), sortable by time for deduplication and debugging.
- **`schema_version`** — explicit versioning in payload enables mixed-version stream handling during deploys.
- **Declined transactions are streamed too** — a card declined 5 times then approved is a strong fraud signal.

```json
{
  "event_type": "transaction.authorisation",
  "event_id": "01KPZJBAE85C5VCNRVQS9SZ2JV",
  "schema_version": "2.4.0",
  "emitted_at": "2026-04-24T11:00:09.800520+00:00",
  "transaction": {
    "transaction_id": "txn_cedc4f59",
    "amount_minor": 4799,
    "currency": "GBP",
    "type": "purchase",
    "channel": "card_present",
    "entry_mode": "chip",
    "mcc": "5411",
    "is_3ds": false
  },
  "card": {
    "card_id": "card_34331c0e",
    "last_four": "4242",
    "scheme": "visa",
    "is_virtual": false
  },
  "customer": {
    "customer_id": "cust_x7y8z9",
    "account_age_days": 412,
    "kyc_tier": "full"
  },
  "merchant": {
    "merchant_id": "mch_tesco_uk_01",
    "name": "TESCO STORES",
    "country": "GBR",
    "city": "London",
    "terminal_id": "TRM_0041"
  },
  "location": {
    "ip_country": null,
    "device_id": null
  },
  "network": {
    "rrn": "240424143207",
    "auth_code": "A12345",
    "response_code": "00",
    "acquirer_bin": "411111"
  }
}
```

---

## ⚙️ Feature Engineering

The enrichment pipeline looks up customer behaviour from Redis and computes derived signals for the scorer:

| Feature | Type | Description |
|---------|------|-------------|
| `avg_spend_minor` | int | Customer's historical average transaction amount |
| `txn_count_1h` | int | Number of transactions in the last hour |
| `distinct_countries_24h` | int | Distinct countries transacted in over 24h |
| `known_mccs` | list | Merchant categories the customer has used before |
| `is_cold_start` | bool | True for unknown customers — treated as higher risk |
| `amount_vs_avg_ratio` | float | `amount / avg_spend` — key fraud signal, 99.0 if no history |
| `is_known_mcc` | bool | Whether the MCC appears in customer's history |
| `is_high_risk_mcc` | bool | MCC in `{7995, 4829, 6011}` (gambling, money transfer, ATM) |
| `is_high_risk_channel` | bool | Channel is `ecommerce` or `atm` |

---

## 🚀 Running Locally

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- [uv](https://astral.sh/uv) — `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/Marcanta7/fraud-detection-pipeline.git
cd fraud-detection-pipeline

# 2. Install dependencies
uv sync

# 3. Copy environment config
cp .env.example .env

# 4. Start infrastructure
docker compose up -d

# 5. Wait ~20 seconds for Kafka to be ready, then create topics
uv run python -m infra.kafka_admin
```

### Run the pipeline

Open **3 separate terminals**:

```bash
# Terminal 1 — seed the feature store (one-off)
uv run python -m feature_store.redis_client

# Terminal 2 — start the enrichment pipeline
uv run python -m pipeline.main

# Terminal 3 — start the transaction producer
uv run python -m producer.main
```

### Monitor

Open **Kafka UI** at [http://localhost:8080](http://localhost:8080) to watch events flowing through all topics in real time.

You should see:
- `transactions-raw` — raw events at ~10/sec
- `transactions-enriched` — enriched events with feature vectors
- `transactions-dead-letter` — any malformed events

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Kafka broker address |
| `KAFKA_TOPIC_TRANSACTIONS` | `transactions-raw` | Input topic |
| `KAFKA_TOPIC_ALERTS` | `fraud-alerts` | Output alerts topic |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `PRODUCER_EVENTS_PER_SECOND` | `10` | Throughput (increase to stress test) |
| `PRODUCER_FRAUD_RATE` | `0.02` | Fraction of synthetic fraud events (0.0–1.0) |

---

## 🔄 Build Status

- [x] **Session 1** — repo setup, Kafka infrastructure, Avro schema, transaction event producer
- [x] **Session 2** — enrichment pipeline, Redis feature store, feature engineering
- [ ] **Session 3** — XGBoost fraud scorer + rule engine + decision router (ALLOW / REVIEW / BLOCK)
- [ ] **Session 4** — BigQuery sink + fraud alerts publisher + Looker ops dashboard

---

## 🏦 Why This Architecture

### Designed for fintech interviews

**Revolut / neobanks** — The dual-layer scoring (ML model + rule engine) mirrors how production fraud systems actually work. A model alone can't block the obvious cases fast enough; rules handle velocity and geo-mismatch deterministically in microseconds while the model covers subtle behavioural patterns.

**Banks (regulatory focus)** — XGBoost is chosen over neural networks deliberately: it's interpretable. A compliance officer can audit why a transaction was blocked. The feature attribution is traceable, which matters for PSD2 and FCA requirements.

**Insurance companies** — The same pipeline architecture applies directly to claims fraud detection. The MCC risk scoring maps cleanly to claims category risk.

### Key engineering decisions worth discussing in interviews

1. **`amount_minor` as integer** — float arithmetic on money causes silent precision bugs. Always store financial amounts as integer minor units.
2. **Redis feature cache** — re-computing features from BigQuery at inference time adds 500ms+. The pre-computed cache is what makes < 200ms SLA achievable.
3. **PCI-DSS scope** — `card_id` is an internal token. The raw PAN never enters the pipeline, keeping it outside PCI-DSS scope.
4. **Dead letter queue** — malformed events are routed away rather than crashing the pipeline. They're reprocessable once the upstream schema issue is fixed.
5. **ULID over UUID** — event IDs sort lexicographically by time, making deduplication and debugging significantly easier at scale.
6. **Dual Kafka listeners** — `PLAINTEXT_INTERNAL` for container-to-container traffic, `PLAINTEXT_EXTERNAL` for host machine access. Standard production pattern for containerised Kafka.

### What I'd add in production

- **Model monitoring** — data drift detection on the feature distribution, alerting when `amount_vs_avg_ratio` distribution shifts
- **A/B testing infrastructure** — shadow scoring with a challenger model before promoting to production
- **Feature attribution store** — per-decision explainability log for regulatory audit trail
- **Backfill pipeline** — historical reprocessing when retraining the model with new labels
- **Circuit breaker** — fall back to rules-only scoring if the ML model endpoint is degraded

---

## 📄 License

MIT
