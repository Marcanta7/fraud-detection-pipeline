import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
import xgboost as xgb
import pickle
import os

SEED = 42
np.random.seed(SEED)


def generate_training_data(n_samples: int = 50000) -> pd.DataFrame:
    """
    Generate synthetic labelled fraud dataset matching our feature schema.
    Fraud rate ~2% matching production PRODUCER_FRAUD_RATE.
    """
    n_fraud = int(n_samples * 0.02)
    n_legit = n_samples - n_fraud

    def legit():
        return {
            "amount_minor": np.random.randint(300, 15000, n_legit),
            "avg_spend_minor": np.random.randint(500, 12000, n_legit),
            "txn_count_1h": np.random.randint(0, 4, n_legit),
            "distinct_countries_24h": np.random.randint(1, 3, n_legit),
            "account_age_days": np.random.randint(30, 2000, n_legit),
            "is_cold_start": np.zeros(n_legit),
            "is_known_mcc": np.random.choice([0, 1], n_legit, p=[0.1, 0.9]),
            "is_high_risk_mcc": np.random.choice([0, 1], n_legit, p=[0.85, 0.15]),
            "is_high_risk_channel": np.random.choice([0, 1], n_legit, p=[0.6, 0.4]),
            "is_3ds": np.random.choice([0, 1], n_legit, p=[0.7, 0.3]),
            "is_virtual_card": np.zeros(n_legit),
            "label": np.zeros(n_legit),
        }

    def fraud():
        avg = np.random.randint(500, 8000, n_fraud)
        return {
            # Fraud: high amounts relative to average
            "amount_minor": avg * np.random.uniform(2.0, 8.0, n_fraud),
            "avg_spend_minor": avg,
            "txn_count_1h": np.random.randint(3, 15, n_fraud),
            "distinct_countries_24h": np.random.randint(2, 6, n_fraud),
            "account_age_days": np.random.randint(0, 20, n_fraud),
            "is_cold_start": np.random.choice([0, 1], n_fraud, p=[0.3, 0.7]),
            "is_known_mcc": np.random.choice([0, 1], n_fraud, p=[0.8, 0.2]),
            "is_high_risk_mcc": np.random.choice([0, 1], n_fraud, p=[0.2, 0.8]),
            "is_high_risk_channel": np.random.choice([0, 1], n_fraud, p=[0.1, 0.9]),
            "is_3ds": np.random.choice([0, 1], n_fraud, p=[0.9, 0.1]),
            "is_virtual_card": np.random.choice([0, 1], n_fraud, p=[0.4, 0.6]),
            "label": np.ones(n_fraud),
        }

    df = pd.concat([
        pd.DataFrame(legit()),
        pd.DataFrame(fraud()),
    ]).sample(frac=1, random_state=SEED).reset_index(drop=True)

    # Derived features
    df["amount_vs_avg_ratio"] = (
        df["amount_minor"] / df["avg_spend_minor"].clip(lower=1)
    ).round(3)

    return df


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


def train():
    print("Generating training data...")
    df = generate_training_data(50000)

    X = df[FEATURES]
    y = df["label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=SEED, stratify=y
    )

    print(f"Training set: {len(X_train)} samples | "
          f"fraud: {int(y_train.sum())} ({y_train.mean()*100:.1f}%)")

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=49,   # handles class imbalance (98:2 ratio)
        eval_metric="auc",
        random_state=SEED,
        verbosity=0,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # Evaluation
    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= 0.5).astype(int)
    auc = roc_auc_score(y_test, y_prob)

    print(f"\nModel performance:")
    print(f"  ROC-AUC: {auc:.4f}")
    print(classification_report(y_test, y_pred, target_names=["legit", "fraud"]))

    # Save model
    os.makedirs("scorer/model_artifacts", exist_ok=True)
    with open("scorer/model_artifacts/fraud_model.pkl", "wb") as f:
        pickle.dump(model, f)

    print("Model saved to scorer/model_artifacts/fraud_model.pkl")
    return model


if __name__ == "__main__":
    train()