"""Consume transactions and publish exactly three scoring fields."""

import json
import logging
import os
import time

from kafka import KafkaConsumer, KafkaProducer

from .model import FraudModel
from .preprocessing import preprocess


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)


def score_message(message, model, threshold):
    if not isinstance(message, dict):
        raise ValueError("Kafka message must be a JSON object")
    transaction_id = message.get("transaction_id")
    row = message.get("data", message)
    if not transaction_id and isinstance(row, dict):
        transaction_id = row.get("transaction_id") or row.get("trans_num") or row.get("id")
    if transaction_id is None or str(transaction_id).strip() == "":
        raise ValueError("transaction_id is required")
    score = model.predict_proba(preprocess(row))
    return {"transaction_id": str(transaction_id), "score": score, "fraud_flag": int(score >= threshold)}


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    model = FraudModel(os.getenv("MODEL_PATH", "model/fraud_catboost.cbm"))
    threshold_override = os.getenv("FRAUD_THRESHOLD")
    metadata_path = os.getenv("MODEL_METADATA_PATH", "model/metrics.json")
    threshold = float(threshold_override) if threshold_override else float(json.loads(open(metadata_path, encoding="utf-8").read())["threshold"])
    if not 0 <= threshold <= 1:
        raise ValueError("FRAUD_THRESHOLD must be between 0 and 1")
    while True:
        try:
            consumer = KafkaConsumer(
                "transactions", bootstrap_servers=bootstrap, group_id="fraud-scorer-v1",
                auto_offset_reset="earliest", enable_auto_commit=False,
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            producer = KafkaProducer(
                bootstrap_servers=bootstrap, acks="all", retries=5,
                value_serializer=lambda value: json.dumps(value, allow_nan=False).encode("utf-8"),
            )
            for record in consumer:
                try:
                    result = score_message(record.value, model, threshold)
                except (ValueError, TypeError, KeyError) as exc:
                    LOG.error("Skipping invalid transaction at offset %s: %s", record.offset, exc)
                    consumer.commit()
                    continue
                producer.send("scores", key=result["transaction_id"].encode(), value=result).get(timeout=30)
                consumer.commit()
                LOG.info("Scored transaction %s", result["transaction_id"])
        except Exception:
            LOG.exception("Scorer disconnected; retrying")
            time.sleep(5)


if __name__ == "__main__":
    run()
