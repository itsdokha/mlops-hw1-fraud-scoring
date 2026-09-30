"""Features shared by offline training and streaming CPU inference."""

from datetime import datetime
from math import asin, cos, log1p, radians, sin, sqrt


NUMERIC_FEATURES = (
    "log_amount", "hour", "weekday", "month", "night", "distance_km",
    "log_population", "lat", "lon", "merchant_lat", "merchant_lon",
)
CATEGORICAL_FEATURES = ("cat_id", "gender", "us_state", "merch", "jobs")
FEATURE_NAMES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
REQUIRED_COLUMNS = ("transaction_time", "amount", "lat", "lon", "merchant_lat", "merchant_lon", "cat_id", "population_city")


def validate_columns(columns):
    """Reject unrelated CSV files before they reach the model."""
    missing = [name for name in REQUIRED_COLUMNS if name not in columns]
    if missing:
        raise ValueError("Не хватает колонок транзакции: " + ", ".join(missing))


def _first(row, *names):
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _number(value, default=0.0):
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else default
    except (ValueError, TypeError):
        return default


def _datetime(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _distance_km(lat1, lon1, lat2, lon2):
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return 0.0
    a1, o1, a2, o2 = map(lambda x: radians(_number(x)), (lat1, lon1, lat2, lon2))
    h = sin((a2 - a1) / 2) ** 2 + cos(a1) * cos(a2) * sin((o2 - o1) / 2) ** 2
    return 6371.0 * 2 * asin(min(1.0, sqrt(h)))


def preprocess(row):
    """Return the model's ordered feature mapping for one CSV/JSON transaction."""
    if not isinstance(row, dict):
        raise ValueError("transaction data must be a JSON object")
    validate_columns(row)
    timestamp = _datetime(_first(row, "transaction_time", "trans_date_trans_time", "timestamp"))
    lat = _first(row, "lat", "customer_lat")
    lon = _first(row, "lon", "long", "customer_lon")
    merchant_lat = _first(row, "merchant_lat", "merch_lat")
    merchant_lon = _first(row, "merchant_lon", "merch_long")
    amount = max(0.0, _number(_first(row, "amount", "amt", "transaction_amount")))
    population = max(0.0, _number(_first(row, "population_city", "city_pop", "city_population")))
    return {
        "log_amount": log1p(amount),
        "hour": float(timestamp.hour if timestamp else 0),
        "weekday": float(timestamp.weekday() if timestamp else 0),
        "month": float(timestamp.month if timestamp else 0),
        "night": float(timestamp is not None and (timestamp.hour < 6 or timestamp.hour >= 23)),
        "distance_km": min(_distance_km(lat, lon, merchant_lat, merchant_lon), 20000.0),
        "log_population": log1p(population),
        "lat": _number(lat),
        "lon": _number(lon),
        "merchant_lat": _number(merchant_lat),
        "merchant_lon": _number(merchant_lon),
        "cat_id": str(_first(row, "cat_id", "category") or "<missing>"),
        "gender": str(_first(row, "gender") or "<missing>"),
        "us_state": str(_first(row, "us_state", "state") or "<missing>"),
        "merch": str(_first(row, "merch", "merchant") or "<missing>"),
        "jobs": str(_first(row, "jobs", "job") or "<missing>"),
    }
