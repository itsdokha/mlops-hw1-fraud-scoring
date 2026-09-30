"""One-time offline training on the competition's train.csv.

Run locally with: .venv/bin/python scripts/train_model.py
The deployed scorer never runs this script.
"""

import csv
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score

from fraud_service.preprocessing import CATEGORICAL_FEATURES, FEATURE_NAMES, preprocess


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "train.csv"
MODEL_PATH = ROOT / "model" / "fraud_catboost.cbm"
METADATA_PATH = ROOT / "model" / "metrics.json"


def load_data():
    features, labels, timestamps = [], [], []
    with DATA_PATH.open(encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            features.append(preprocess(row))
            labels.append(int(row["target"]))
            timestamps.append(row["transaction_time"])
    return pd.DataFrame.from_records(features, columns=FEATURE_NAMES), np.asarray(labels), pd.to_datetime(timestamps)


def main():
    print("Reading and preprocessing", DATA_PATH, flush=True)
    features, labels, timestamps = load_data()
    order = np.argsort(timestamps)
    split = int(len(order) * 0.8)
    train_index, valid_index = order[:split], order[split:]
    print("Rows:", len(order), "train frauds:", labels[train_index].sum(), "validation frauds:", labels[valid_index].sum(), flush=True)
    model = CatBoostClassifier(
        iterations=500, depth=6, learning_rate=0.08, l2_leaf_reg=5,
        loss_function="Logloss", eval_metric="AUC", task_type="CPU",
        thread_count=4, random_seed=42, allow_writing_files=False,
    )
    train_pool = Pool(features.iloc[train_index], label=labels[train_index], cat_features=list(CATEGORICAL_FEATURES))
    valid_pool = Pool(features.iloc[valid_index], label=labels[valid_index], cat_features=list(CATEGORICAL_FEATURES))
    model.fit(train_pool, eval_set=valid_pool, early_stopping_rounds=50, verbose=50)
    probabilities = model.predict_proba(valid_pool)[:, 1]
    precision, recall, thresholds = precision_recall_curve(labels[valid_index], probabilities)
    f1 = 2 * precision[:-1] * recall[:-1] / np.maximum(precision[:-1] + recall[:-1], 1e-12)
    best = int(np.argmax(f1))
    metrics = {
        "train_rows": int(len(train_index)),
        "validation_rows": int(len(valid_index)),
        "train_frauds": int(labels[train_index].sum()),
        "validation_frauds": int(labels[valid_index].sum()),
        "validation_roc_auc": float(roc_auc_score(labels[valid_index], probabilities)),
        "validation_average_precision": float(average_precision_score(labels[valid_index], probabilities)),
        "validation_f1": float(f1[best]),
        "threshold": float(thresholds[best]),
        "best_iteration": int(model.best_iteration_),
        "validation_method": "last 20% by transaction_time",
    }
    MODEL_PATH.parent.mkdir(exist_ok=True)
    model.save_model(str(MODEL_PATH))
    METADATA_PATH.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metrics, indent=2), flush=True)


if __name__ == "__main__":
    main()
