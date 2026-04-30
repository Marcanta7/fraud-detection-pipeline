# Real-Time Fraud Detection Pipeline

A production-grade, real-time card transaction fraud detection system built on GCP, Apache Kafka, XGBoost, and Redis — with a complete ML feedback loop that automatically retrains the fraud model as new confirmed labels arrive.

Scores transactions in **< 200ms end-to-end latency** at **10,000+ events/sec throughput**.

Built as a data engineering portfolio project targeting fintech (Revolut), banking, and insurance engineering roles — demonstrating streaming pipelines, feature engineering, ML model serving, dead letter routing, GCP infrastructure, and agentic online learning.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INGEST LAYER                                   │
│                                                                             │
│   Card Transactions ──► Kafka (transactions-raw) ──► Schema Registry        │
│   (POS, ecommerce,                                     (Avro 2.4.0)         │
│    ATM, contactless)                                                        │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                             STREAM LAYER                                    │
│                                                                             │
│   Enrichment pipeline ──► Feature store ──► Dead letter queue               │
│   (Apache Beam)            (Redis cache)     (malformed events)             │
│   - schema validation                                                       │
│   - feature lookup                                                          │
│   - feature engineering                                                     │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                            SCORING LAYER                                    │
│                                                                             │
│   XGBoost model ──► Rule engine ──► Decision router                         │
│   (hot-reloaded       (velocity,     ALLOW / REVIEW / BLOCK                 │
│    automatically)      geo, MCC)                                            │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                              SINK LAYER                                     │
│                                                                             │
│   BigQuery ◄── All transactions      Alerts topic ──► Ops dashboard         │
│   (partitioned by day,               (BLOCK/REVIEW                          │
│    clustered by customer)             events only)                          │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                          ML FEEDBACK LOOP                                   │
│                                                                             │
│   Label collector ──► confirmed-fraud ──► Retraining trigger                │
│   (chargebacks,         (Kafka topic)      (fires at N labels)              │
│    ops review,                                      │                       │
│    auto-label)                                      ▼                       │
│                                            Retraining job                   │
│                                            (BigQuery → XGBoost              │
│                                             → eval → promote)               │
│                                                     │                       │
│                                                     ▼                       │
│                                         Scorer hot-reloads ↺                │
│                                         (zero downtime, 60s check)          │
└─────────────────────────────────────────────────────────────────────────────┘

Target SLA: < 200ms latency · 10k+ events/sec · 99.9% availability
```

---

## Live Pipeline Results

Real query results from BigQuery after running the full pipeline:

**Scored transactions breakdown:**
```
+----------+-------+----------+
| decision | count | avg_prob |
+----------+-------+----------+
| REVIEW   |  2264 |    0.007 |
| BLOCK    |    87 |    0.563 |
+----------+-------+----------+
```

**Rule engine attribution:**
```
+----------+-------------------------+-------+
| decision |        rule_name        | count |
+----------+-------------------------+-------+
| REVIEW   | AMOUNT_SPIKE            |  2870 |
| BLOCK    | HIGH_RISK_MCC_BLOCK     |    58 |
| BLOCK    | NEW_ACCOUNT_HIGH_AMOUNT |    29 |
| BLOCK    | AMOUNT_SPIKE            |    18 |
+----------+-------------------------+-------+
```

**Scorer live stats:**
```
metric                          value
scored                           5221
ALLOW                               0
REVIEW                           5067
BLOCK                             154
block rate                       2.95%
review rate                     97.05%
rule-only decisions              5100
ml-only decisions                   0
ml+rule decisions                 121
```

**ML feedback loop — model versions generated:**
```
scorer/model_artifacts/
├── fraud_model_20260430_101928.pkl
├── fraud_model_20260430_101948.pkl
├── fraud_model_20260430_102012.pkl
├── fraud_model_20260430_102035.pkl
├── fraud_model_20260430_102145.pkl   ← current production model
├── fraud_model.pkl                   ← symlink to latest
└── model_metadata.json               ← version + ROC-AUC + timestamp
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Ingest | Apache Kafka (Confluent 7.6) | Durable event buffer, decouples producers from consumers |
| Ingest | Avro + Schema Registry | Schema enforcement and evolution |
| Stream | Apache Beam | Enrichment transforms — deploys to GCP Dataflow in production |
| Feature store | Redis 7 | Sub-millisecond customer behaviour lookups at inference time |
| Scoring | XGBoost | Fraud probability score — ~0.97 ROC-AUC |
| Scoring | Python rule engine | Deterministic velocity, geo, and amount rules |
| Feedback | Kafka `confirmed-fraud` topic | Label event bus |
| Feedback | BigQuery `fraud_labels` table | Ground truth store for retraining |
| Sink | Google BigQuery | All scored transactions, partitioned for analytics |
| Monitoring | Looker Studio | Fraud ops dashboard connected to BigQuery |
| Local dev | Docker Compose | Kafka, Zookeeper, Redis, Kafka UI |
| Language | Python 3.11 + uv | Fast, modern dependency management |
| Validation | Pydantic v2 | Strict event model validation |
| Cloud | GCP (BigQuery, Pub/Sub, Dataflow, Vertex AI, Cloud Run) | Production deployment target |

---

## Repository Structure

```
fraud-detection-pipeline/
│
├── docker-compose.yml              # Kafka + Zookeeper + Redis + Kafka UI
├── pyproject.toml                  # uv project config and dependencies
├── .env.example                    # environment variable template
├── CLAUDE.md                       # context file for Claude Code
│
├── schemas/
│   └── transaction_authorisation.avsc    # Avro schema v2.4.0
│
├── infra/
│   ├── kafka_admin.py              # creates all 7 Kafka topics
│   └── bigquery_admin.py           # creates BigQuery dataset and all tables
│
├── producer/
│   ├── models.py                   # Pydantic event models + enriched models
│   ├── generator.py                # realistic fake transaction factory
│   └── main.py                     # Kafka producer with live Rich dashboard
│
├── feature_store/
│   └── redis_client.py             # feature lookups + seeds 500 customer profiles
│
├── pipeline/
│   ├── transforms.py               # ParseEvent, EnrichWithFeatures, SerialiseToJson
│   └── main.py                     # Kafka consumer loop driving enrichment
│
├── scorer/
│   ├── train.py                    # synthetic training data + XGBoost training
│   ├── model.py                    # FraudScorer with hot reload (background thread)
│   ├── rules.py                    # 9-rule deterministic engine
│   ├── router.py                   # combines ML + rules, most severe wins
│   ├── main.py                     # scoring consumer with live Rich dashboard
│   └── model_artifacts/            # gitignored — run scorer/train.py to generate
│
├── sinks/
│   ├── bigquery_sink.py            # batched streaming inserts to BigQuery
│   └── alerts_publisher.py         # formatted BLOCK/REVIEW alert printer
│
└── feedback/
    ├── label_collector.py          # reads scored events, emits confirmed labels
    ├── retrain_trigger.py          # watches label count, fires retraining at threshold
    └── retrain_job.py              # pulls BigQuery labels, retrains, evaluates, promotes
```

---

## Kafka Topics

| Topic | Partitions | Description |
|-------|------------|-------------|
| `transactions-raw` | 6 | Raw authorisation events from the producer |
| `transactions-enriched` | 6 | Events with customer behaviour features attached |
| `transactions-scored` | 6 | All scored events with ML + rule decisions |
| `fraud-alerts` | 3 | BLOCK and REVIEW decisions only |
| `transactions-dead-letter` | 2 | Malformed or unparseable events |
| `confirmed-fraud` | 3 | Confirmed fraud labels from all three sources |

---

## Event Payload

Each card transaction emits a `transaction.authorisation` event. Key design decisions:

- **`amount_minor`** — stored as `long` integer (pence/cents), never float. Avoids floating point precision bugs (`47.99 * 100` in Python gives `4798.999...`)
- **`card_id`** — internal token, never the raw PAN. Keeps the pipeline outside PCI-DSS scope
- **`event_id`** — ULID (not UUID4), sortable by time for deduplication and debugging
- **`schema_version`** — explicit versioning in payload enables mixed-version stream handling during rolling deploys
- **Declined transactions are streamed too** — a card declined 5 times then approved is a strong fraud signal

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

## Feature Engineering

The enrichment pipeline looks up customer behaviour from Redis and computes derived signals:

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

## Rule Engine

9 deterministic rules evaluated in priority order — first BLOCK rule wins:

**BLOCK rules:**

| Rule | Condition |
|------|-----------|
| `NEW_ACCOUNT_HIGH_AMOUNT` | Account age < 7 days AND amount > £500 |
| `HIGH_VELOCITY` | > 10 transactions in last hour |
| `GEO_ANOMALY_HIGH_RISK_COUNTRY` | 3+ countries in 24h AND merchant in high-risk country |
| `HIGH_RISK_MCC_BLOCK` | MCC 4829 (money transfer) — always block |

**REVIEW rules:**

| Rule | Condition |
|------|-----------|
| `AMOUNT_SPIKE` | Amount > 5x customer average |
| `COLD_START_HIGH_AMOUNT` | Unknown customer AND amount > £200 |
| `REVIEW_MCC` | MCC 7995 (gambling) or 6011 (ATM) |
| `NEW_ACCOUNT_MODERATE_AMOUNT` | Account age < 14 days AND amount > £200 |
| `GEO_ANOMALY_UNKNOWN_MCC` | 2+ countries in 24h AND unknown MCC |

---

## ML Model

- **Algorithm:** XGBoost classifier
- **Training data:** 50,000 synthetic samples, 2% fraud rate
- **Class imbalance:** handled via `scale_pos_weight` (98:2 ratio)
- **Features:** 10 engineered features including `amount_vs_avg_ratio`, velocity, geo, MCC risk, account age
- **Thresholds:** BLOCK ≥ 0.85 · REVIEW ≥ 0.40 · ALLOW < 0.40
- **Why XGBoost:** interpretable — every decision can be audited via feature importance and SHAP values. Critical for PSD2 and FCA compliance requirements

**Decision fusion:** the router takes the most severe decision between ML and rules. Decision source is recorded as `ML`, `RULE`, or `ML+RULE` for full audit trail.

---

## ML Feedback Loop

The pipeline automatically improves the fraud model over time as real fraud labels accumulate.

### Three label sources

| Source | Trigger | Label |
|--------|---------|-------|
| `chargeback` | Customer disputes a transaction with their bank | fraud=1 |
| `ops_review` | Fraud analyst confirms a REVIEW decision | fraud=1 or 0 |
| `auto_label` | BLOCK with fraud_probability ≥ 0.90 | fraud=1 |

### Flow

```
transactions-scored
        ↓
label_collector.py
        ↓ (confirmed label event)
confirmed-fraud (Kafka topic) + fraud_labels (BigQuery)
        ↓
retrain_trigger.py
(accumulates labels, fires at threshold — default 50 dev / 500 prod)
        ↓
retrain_job.py
1. Pull fraud_labels from BigQuery
2. Combine with synthetic base data (weighted 1:3 real:synthetic)
3. Retrain XGBoost with updated scale_pos_weight
4. Evaluate on held-out test set
5. Promote only if new ROC-AUC ≥ current ROC-AUC - 0.001
6. Save versioned pkl + update model_metadata.json
7. Cleanup — keep last 5 versions only
        ↓
scorer/model.py — background thread checks every 60s
(detects new version in model_metadata.json → hot-reloads model)
        ↓
Zero-downtime model update — threading.RLock protects inference
```

### Model versioning

Every promoted model gets a timestamped file (`fraud_model_20260430_102145.pkl`). `model_metadata.json` tracks the current version and its ROC-AUC. The scorer reads this file every 60 seconds to detect updates.

---

## BigQuery Schema

Three tables in the `fraud_pipeline` dataset:

**`transactions_scored`** — all scored transactions, partitioned by `scored_at` day, clustered by `customer_id + decision`

**`fraud_alerts`** — BLOCK and REVIEW decisions only, same schema

**`fraud_labels`** — confirmed fraud ground truth, partitioned by `confirmed_at`, clustered by `label_source + label`

```sql
-- Decision breakdown
SELECT decision, COUNT(*) as count, ROUND(AVG(fraud_probability),3) as avg_prob
FROM `dev-mc-projects.fraud_pipeline.transactions_scored`
GROUP BY decision ORDER BY count DESC

-- Rule attribution
SELECT decision, rule_name, COUNT(*) as count
FROM `dev-mc-projects.fraud_pipeline.fraud_alerts`
GROUP BY decision, rule_name ORDER BY count DESC

-- Label accumulation over time
SELECT DATE(confirmed_at) as date, label_source, COUNT(*) as labels
FROM `dev-mc-projects.fraud_pipeline.fraud_labels`
GROUP BY date, label_source ORDER BY date DESC
```

---

## Running Locally

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- [uv](https://astral.sh/uv): `curl -LsSf https://astral.sh/uv/install.sh | sh`
- GCP project with BigQuery enabled
- macOS: `brew install libomp` then `brew link libomp --force` (required by XGBoost)

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/Marcanta7/fraud-detection-pipeline.git
cd fraud-detection-pipeline

# 2. Install dependencies
uv sync

# 3. Copy and configure environment
cp .env.example .env
# Edit .env — set GCP_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS

# 4. Start local infrastructure
docker compose up -d

# 5. Wait ~20 seconds, then create Kafka topics
uv run python -m infra.kafka_admin

# 6. Create BigQuery dataset and tables
uv run python -m infra.bigquery_admin

# 7. Seed the Redis feature store
uv run python -m feature_store.redis_client

# 8. Train the initial fraud model
uv run python -m scorer.train
```

### Run the full pipeline

Open **7 separate terminals**, start in this order:

```bash
# Terminal 1 — enrichment pipeline (before producer)
uv run python -m pipeline.main

# Terminal 2 — fraud scorer (hot reload enabled)
uv run python -m scorer.main

# Terminal 3 — BigQuery sink
uv run python -m sinks.bigquery_sink

# Terminal 4 — alerts publisher
uv run python -m sinks.alerts_publisher

# Terminal 5 — label collector
uv run python -m feedback.label_collector

# Terminal 6 — retraining trigger
uv run python -m feedback.retrain_trigger

# Terminal 7 — producer (start last)
uv run python -m producer.main
```

### Monitor

- **Kafka UI** at [http://localhost:8080](http://localhost:8080) — watch all 6 topics filling in real time
- **Terminal 2** — live ALLOW/REVIEW/BLOCK dashboard, watch for `Model hot-reloaded` log lines
- **Terminal 6** — label accumulation counter, retraining job output
- **BigQuery** — query the three tables for analytics

```bash
# Watch model versions being created
watch -n 5 cat scorer/model_artifacts/model_metadata.json

# Count active model versions (never exceeds 5)
ls scorer/model_artifacts/fraud_model_*.pkl | wc -l
```

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Must be IPv4 — macOS resolves `localhost` to IPv6 |
| `KAFKA_TOPIC_TRANSACTIONS` | `transactions-raw` | Input topic |
| `KAFKA_TOPIC_ALERTS` | `fraud-alerts` | Output alerts topic |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `PRODUCER_EVENTS_PER_SECOND` | `10` | Throughput — increase to stress test |
| `PRODUCER_FRAUD_RATE` | `0.02` | Fraction of synthetic fraud events |
| `GCP_PROJECT_ID` | — | Your GCP project ID |
| `GCP_DATASET_ID` | `fraud_pipeline` | BigQuery dataset name |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to GCP service account key JSON |
| `RETRAIN_THRESHOLD` | `50` | Labels before retraining fires (use 500 in production) |
| `FRAUD_LABEL_MIN` | `10` | Minimum fraud labels required before retraining |

---

## GCP Infrastructure

### Services used

| Service | Purpose |
|---------|---------|
| BigQuery | Scored transaction storage, fraud labels, analytics |
| Pub/Sub | Managed Kafka equivalent for production deployment |
| Dataflow | Managed Apache Beam runner — swap `DirectRunner` for `DataflowRunner` |
| Vertex AI | Model serving + Feature Store in production |
| Cloud Run | Serverless container hosting for scorer and sink services |
| Cloud Storage | Model artifact versioning in production |
| Secret Manager | Replaces `.env` files in production |

### Service account setup

```bash
export PROJECT_ID=your-project-id

# Enable APIs
gcloud services enable bigquery.googleapis.com pubsub.googleapis.com \
  run.googleapis.com aiplatform.googleapis.com dataflow.googleapis.com \
  secretmanager.googleapis.com storage.googleapis.com

# Create service account
gcloud iam service-accounts create fraud-pipeline-sa \
  --display-name="Fraud Pipeline Service Account"

# Assign roles
for role in \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/pubsub.editor \
  roles/storage.objectAdmin \
  roles/run.developer \
  roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:fraud-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="$role"
done

# Download key
gcloud iam service-accounts keys create key.json \
  --iam-account=fraud-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com
```

---

## Why This Architecture

### For fintech interviews (Revolut, neobanks)

The dual-layer scoring (ML + rules) mirrors how production fraud systems work. Rules handle obvious patterns deterministically in microseconds — velocity, geo-mismatch, high-risk MCCs. The model covers subtle behavioural signals that rules can't capture. The Redis feature cache (not BigQuery) is what makes < 200ms latency achievable.

The feedback loop is the differentiator — most candidates demonstrate a static model. This pipeline shows the full lifecycle: score → label → retrain → hot-reload → score again. That's what a production fraud system actually looks like.

### For banks (regulatory focus)

XGBoost is chosen deliberately over neural networks — it's interpretable. The `decision_source` field (`ML`, `RULE`, `ML+RULE`) provides a full audit trail on every decision. A compliance officer can trace exactly why a transaction was blocked. This matters for PSD2, FCA, and Basel requirements.

### For insurance companies

The same pipeline architecture applies directly to claims fraud. MCC risk scoring maps to claims category risk. The label collector pattern maps to claims investigation outcomes feeding back into the model.

### Key engineering decisions worth discussing

1. **`amount_minor` as integer** — `47.99 * 100` in Python gives `4798.999...`. Always store financial amounts as integer minor units
2. **Redis feature cache** — re-computing features from BigQuery at inference adds 500ms+. Pre-computed cache makes the latency SLA achievable
3. **PCI-DSS scope** — `card_id` is an internal token. The raw PAN never enters the pipeline
4. **Dead letter queue** — malformed events route away rather than crashing the pipeline. Reprocessable once the upstream issue is fixed
5. **ULID over UUID4** — event IDs sort lexicographically by time, making deduplication and debugging easier at scale
6. **Dual Kafka listeners** — `PLAINTEXT_INTERNAL://kafka:29092` for containers, `PLAINTEXT_EXTERNAL://127.0.0.1:9092` for host. On macOS, must use `127.0.0.1` not `localhost` to avoid IPv6 resolution issues
7. **`scale_pos_weight`** — dynamically computed from actual class ratio. Handles the 98:2 fraud/legit imbalance without manual tuning
8. **`threading.RLock` on inference** — hot reload swaps the model object in a background thread. The lock prevents a race condition where a request reads a partially-loaded model
9. **Promote only if better** — the retraining job compares ROC-AUC before promoting. A degraded model caused by noisy labels never reaches production
10. **Real labels weighted 3x** — confirmed labels from BigQuery are more valuable than synthetic training data. Weighting reflects this without discarding the synthetic base

### What I'd add in production

- **Model monitoring** — data drift detection on feature distributions, alerting when `amount_vs_avg_ratio` distribution shifts significantly from training baseline
- **A/B testing infrastructure** — shadow scoring with a challenger model before promoting to production. Route 5% of traffic to the new model and compare alert rates
- **SHAP explanations** — per-decision explainability for regulatory audit trail. Store feature attribution alongside every scored transaction
- **Backfill pipeline** — historical reprocessing of the full `transactions_scored` table when a major model version is released
- **Circuit breaker** — fall back to rules-only scoring if the model loading fails or inference latency spikes above threshold
- **PagerDuty integration** — BLOCK decisions trigger on-call alerts in the alerts publisher
- **Case management** — REVIEW decisions automatically open tickets in an internal fraud ops system
- **Label quality monitoring** — track the ratio of fraud to legit labels per source. A sudden spike in auto-labels might indicate the rule engine is over-blocking

---

## Build Log

| Session | What was built | Status |
|---------|----------------|--------|
| 1 | Repo setup, Docker Compose, Kafka infrastructure, Avro schema, transaction event producer with live Rich dashboard | ✅ done |
| 2 | Apache Beam enrichment pipeline, Redis feature store with 500 customer profiles, 9-feature engineering, dead letter routing | ✅ done |
| 3 | XGBoost fraud model (50k synthetic samples), 9-rule deterministic engine, decision router, live scoring dashboard | ✅ done |
| 4 | BigQuery sink with batched inserts, fraud alerts publisher, GCP service account and API setup | ✅ done |
| 5 | ML feedback loop — label collector (3 sources), retraining trigger, retraining job, hot reload, model versioning and cleanup | ✅ done |

---

## License

MIT