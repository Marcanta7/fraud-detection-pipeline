from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("GCP_DATASET_ID", "fraud_pipeline")


def create_dataset_and_tables():
    client = bigquery.Client(project=PROJECT)

    dataset_id = f"{PROJECT}.{DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} ready")

    schema = [
        bigquery.SchemaField("event_id",          "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("transaction_id",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("customer_id",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("amount_minor",       "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("currency",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("decision",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("fraud_probability",  "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("ml_decision",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("rule_triggered",     "BOOLEAN",   mode="REQUIRED"),
        bigquery.SchemaField("rule_name",          "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("rule_reason",        "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("decision_source",    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("scored_at",          "TIMESTAMP", mode="REQUIRED"),
    ]

    # Fraud labels table — ground truth for retraining
    labels_table_id = f"{dataset_id}.fraud_labels"
    labels_schema = [
        bigquery.SchemaField("event_id",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("transaction_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("customer_id",         "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("label",               "INTEGER",   mode="REQUIRED"),  # 1=fraud, 0=legit
        bigquery.SchemaField("label_source",        "STRING",    mode="REQUIRED"),  # chargeback|ops_review|auto_label
        bigquery.SchemaField("original_decision",   "STRING",    mode="REQUIRED"),  # what model said
        bigquery.SchemaField("fraud_probability",   "FLOAT",     mode="REQUIRED"),  # model confidence at time
        bigquery.SchemaField("rule_name",           "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("amount_minor",        "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("currency",            "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("confirmed_at",        "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("scored_at",           "TIMESTAMP", mode="REQUIRED"),
    ]
    labels_table = bigquery.Table(labels_table_id, labels_schema)
    labels_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="confirmed_at",
    )
    labels_table.clustering_fields = ["label_source", "label"]
    client.create_table(labels_table, exists_ok=True)
    print(f"Table {labels_table_id} ready — ground truth labels for retraining")

    # Scored transactions table
    table_id = f"{dataset_id}.transactions_scored"
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="scored_at",
    )
    table.clustering_fields = ["customer_id", "decision"]
    client.create_table(table, exists_ok=True)
    print(f"Table {table_id} ready — partitioned by day, clustered by customer_id + decision")

    # Fraud alerts table
    alerts_table_id = f"{dataset_id}.fraud_alerts"
    alerts_table = bigquery.Table(alerts_table_id, schema=schema)
    alerts_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="scored_at",
    )
    client.create_table(alerts_table, exists_ok=True)
    print(f"Table {alerts_table_id} ready")


if __name__ == "__main__":
    create_dataset_and_tables()