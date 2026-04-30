import logging
import os
import pickle
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import xgboost as xgb
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("GCP_DATASET_ID", "fraud_pipeline")
MODEL_DIR = Path("scorer/model_artifacts")
SEED = 42

FEATURES = [
    "amount_vs_avg_ratio",
    "txn_count_1h",
    "distinct_countries_24h",
    "account_age_days",
    "is_cold_start",
    "is_known_mcc",
    "is_high_risk_mcc",
    "is_high_risk_channel",
    "is_3ds",
    "is_virtual_card",
]


def load_labels_from_bigquery(bq: bigquery.Client) -> pd.DataFrame:
    """Pull confirmed labels joined with scored transaction features."""
    query = f"""
        SELECT
            l.label,
            l.label_source,
            l.fraud_probability,
            l.amount_minor,
            s.amount_vs_avg_ratio,
            s.txn_count_1h,
            s.distinct_countries_24h,
            s.account_age_days,
            s.is_cold_start,
            s.is_known_mcc,
            s.is_high_risk_mcc,
            s.is_high_risk_channel,
            s.is_3ds,
            s.is_virtual_card
        FROM `{PROJECT}.{DATASET}.fraud_labels` l
        LEFT JOIN `{PROJECT}.{DATASET}.transactions_scored_features` s
            USING (transaction_id)
        WHERE l.label IS NOT NULL
    """
    try:
        df = bq.query(query).to_dataframe()
        logger.info(f"Loaded {len(df)} labels from BigQuery")
        return df
    except Exception as e:
        logger.warning(f"Could not join with features table: {e}")
        logger.info("Falling back to label metadata only")
        return pd.DataFrame()

def cleanup_old_models(keep: int = 5):
    """Keep only the N most recent versioned model files."""
    versioned = sorted(MODEL_DIR.glob("fraud_model_*.pkl"))
    to_delete = versioned[:-keep]
    for path in to_delete:
        path.unlink()
        logger.info(f"Deleted old model: {path.name}")

def load_synthetic_base() -> pd.DataFrame:
    """Load the original synthetic training data as base."""
    from scorer.train import generate_training_data, FEATURES as TRAIN_FEATURES
    df = generate_training_data(n_samples=50000)
    df["amount_vs_avg_ratio"] = (
        df["amount_minor"] / df["avg_spend_minor"].clip(lower=1)
    ).round(3)
    return df


def build_training_data(bq: bigquery.Client) -> tuple[pd.DataFrame, pd.Series]:
    """Combine synthetic base data with real labels from BigQuery."""
    logger.info("Loading synthetic base training data...")
    df_base = load_synthetic_base()

    logger.info("Loading confirmed labels from BigQuery...")
    df_labels = load_labels_from_bigquery(bq)

    if not df_labels.empty and all(f in df_labels.columns for f in FEATURES):
        df_labels = df_labels.dropna(subset=FEATURES)
        logger.info(
            f"Real labels — total: {len(df_labels)}  "
            f"fraud: {df_labels['label'].sum()}  "
            f"legit: {(df_labels['label']==0).sum()}"
        )
        # Weight real labels 3x — they're more valuable than synthetic
        df_labels_weighted = pd.concat([df_labels] * 3, ignore_index=True)
        df_combined = pd.concat([
            df_base[FEATURES + ["label"]],
            df_labels_weighted[FEATURES + ["label"]],
        ], ignore_index=True).sample(frac=1, random_state=SEED)
    else:
        logger.info("No joinable real labels — training on synthetic data + label metadata")
        df_combined = df_base[FEATURES + ["label"]]

    X = df_combined[FEATURES]
    y = df_combined["label"]
    logger.info(
        f"Final dataset — total: {len(df_combined)}  "
        f"fraud: {int(y.sum())}  "
        f"fraud rate: {y.mean()*100:.1f}%"
    )
    return X, y


def load_current_model_auc() -> float:
    """Load the AUC of the current production model from metadata."""
    meta_path = MODEL_DIR / "model_metadata.json"
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
        return meta.get("roc_auc", 0.0)
    return 0.0


def save_model(model, auc: float, version: str):
    """Save model + metadata. Scorer picks up new version via hot reload."""
    MODEL_DIR.mkdir(exist_ok=True)

    # Save model
    model_path = MODEL_DIR / f"fraud_model_{version}.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Update latest symlink
    latest_path = MODEL_DIR / "fraud_model.pkl"
    with open(latest_path, "wb") as f:
        pickle.dump(model, f)

    # Save metadata
    meta = {
        "version": version,
        "roc_auc": auc,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "model_path": str(model_path),
    }
    with open(MODEL_DIR / "model_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"Model saved — version={version}  path={model_path}")


def run():
    version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Retraining job started — version={version}")

    bq = bigquery.Client(project=PROJECT)

    # Build training data
    X, y = build_training_data(bq)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=SEED, stratify=y
    )

    # Train new model
    logger.info("Training new XGBoost model...")
    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=int((y == 0).sum() / max((y == 1).sum(), 1)),
        eval_metric="auc",
        random_state=SEED,
        verbosity=0,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # Evaluate
    y_prob = model.predict_proba(X_test)[:, 1]
    new_auc = roc_auc_score(y_test, y_prob)
    current_auc = load_current_model_auc()

    logger.info(f"New model ROC-AUC:     {new_auc:.4f}")
    logger.info(f"Current model ROC-AUC: {current_auc:.4f}")
    logger.info("\n" + classification_report(
        y_test,
        (y_prob >= 0.5).astype(int),
        target_names=["legit", "fraud"],
    ))

    # Only promote if better (or no current model)
    if new_auc >= current_auc - 0.001:
        logger.info(f"Promoting new model — AUC {current_auc:.4f} → {new_auc:.4f}")
        save_model(model, new_auc, version)
        cleanup_old_models(keep=5)
        logger.info("Model promoted — scorer will hot-reload within 60 seconds")
    else:
        logger.warning(
            f"New model underperforms ({new_auc:.4f} < {current_auc:.4f}) — "
            f"keeping current model"
        )


if __name__ == "__main__":
    run()