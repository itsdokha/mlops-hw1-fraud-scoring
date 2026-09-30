"""Upload transaction CSV rows and inspect persisted scoring results."""

import csv
import io
import json
import os
from uuid import uuid4

import psycopg
import pandas as pd
import streamlit as st
from kafka import KafkaProducer
from fraud_service.preprocessing import validate_columns


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL = os.environ["DATABASE_URL"]

st.set_page_config(page_title="Fraud scoring", layout="wide")
st.title("Скоринг транзакций")
upload_tab, results_tab = st.tabs(["Загрузить транзакции", "Результаты"])

with upload_tab:
    uploaded = st.file_uploader("Выберите CSV в формате test.csv", type="csv")
    st.caption("Для быстрой проверки выберите sample_transactions.csv из папки проекта. sample_submition.csv — шаблон ответа Kaggle, он не содержит транзакций.")
    if uploaded is not None and st.button("Отправить в Kafka", type="primary"):
        try:
            decoded = io.TextIOWrapper(uploaded, encoding="utf-8-sig")
            reader = csv.DictReader(decoded)
            if not reader.fieldnames:
                raise ValueError("CSV должен содержать заголовок")
            if set(reader.fieldnames) == {"index", "prediction"}:
                raise ValueError("это шаблон ответа Kaggle. Выберите sample_transactions.csv или data/test.csv")
            validate_columns(reader.fieldnames)
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                acks="all",
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            )
            count = 0
            for row in reader:
                transaction_id = row.get("transaction_id") or row.get("trans_num") or row.get("id") or str(uuid4())
                producer.send("transactions", key=str(transaction_id).encode(), value={"transaction_id": str(transaction_id), "data": row})
                count += 1
            producer.flush(timeout=60)
            producer.close()
            st.success(f"Отправлено транзакций: {count}. Результаты появятся после обработки.")
        except Exception as exc:
            st.error(f"Не удалось отправить {uploaded.name}: {exc}")

with results_tab:
    if st.button("Посмотреть результаты", type="primary"):
        try:
            with psycopg.connect(DATABASE_URL) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT transaction_id, score, scored_at
                        FROM transaction_scores WHERE fraud_flag = 1
                        ORDER BY scored_at DESC, transaction_id DESC LIMIT 10
                    """)
                    fraud_rows = cursor.fetchall()
                    cursor.execute("""
                        SELECT score FROM transaction_scores
                        ORDER BY scored_at DESC, transaction_id DESC LIMIT 100
                    """)
                    scores = [row[0] for row in cursor.fetchall()]
            st.subheader("Последние 10 транзакций с флагом фрода")
            if fraud_rows:
                st.dataframe(
                    [{"transaction_id": row[0], "score": row[1], "scored_at": row[2]} for row in fraud_rows],
                    use_container_width=True,
                )
            else:
                st.info("Транзакций с fraud_flag = 1 пока нет")
            st.subheader("Распределение скоров последних 100 транзакций")
            if scores:
                import numpy as np

                counts, edges = np.histogram(scores, bins=np.linspace(0, 1, 11))
                histogram = pd.DataFrame({
                    "Интервал": [f"{edges[i]:.1f}–{edges[i + 1]:.1f}" for i in range(len(counts))],
                    "Количество": counts,
                }).set_index("Интервал")
                st.bar_chart(histogram)
            else:
                st.info("Результатов пока нет")
        except Exception as exc:
            st.error(f"Не удалось получить результаты: {exc}")
