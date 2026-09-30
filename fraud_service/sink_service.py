"""Persist Kafka scores to PostgreSQL, with idempotent replay handling."""

import json
import logging
import os
import time

import psycopg
from kafka import KafkaConsumer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

UPSERT = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO UPDATE SET
    score = EXCLUDED.score,
    fraud_flag = EXCLUDED.fraud_flag,
    scored_at = now()
"""


def validate_score(payload):
    transaction_id = str(payload["transaction_id"])
    score = float(payload["score"])
    fraud_flag = int(payload["fraud_flag"])
    if not transaction_id or not 0 <= score <= 1 or fraud_flag not in (0, 1):
        raise ValueError("invalid score message")
    return transaction_id, score, fraud_flag


def run():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    database_url = os.environ["DATABASE_URL"]
    while True:
        try:
            with psycopg.connect(database_url) as connection:
                consumer = KafkaConsumer(
                    "scores", bootstrap_servers=bootstrap, group_id="scores-postgres-v1",
                    auto_offset_reset="earliest", enable_auto_commit=False,
                    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
                )
                for record in consumer:
                    try:
                        values = validate_score(record.value)
                    except (ValueError, TypeError, KeyError) as exc:
                        LOG.error("Skipping invalid score at offset %s: %s", record.offset, exc)
                        consumer.commit()
                        continue
                    with connection.transaction():
                        connection.execute(UPSERT, values)
                    consumer.commit()
                    LOG.info("Saved score for %s", values[0])
        except Exception:
            LOG.exception("Scores sink disconnected; retrying")
            time.sleep(5)


if __name__ == "__main__":
    run()
