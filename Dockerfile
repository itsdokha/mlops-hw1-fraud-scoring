FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY requirements-model.txt .
COPY vendor/ /tmp/vendor/
RUN if ls /tmp/vendor/catboost-*.whl >/dev/null 2>&1; then pip install --no-deps /tmp/vendor/catboost-*.whl; fi \
    && pip install --no-cache-dir --default-timeout=120 --retries=10 -r requirements-model.txt
COPY fraud_service ./fraud_service
COPY model ./model
COPY ui ./ui
ENV PYTHONPATH=/app
