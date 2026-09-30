"""Load the trained CatBoost model for CPU-only inference."""

from catboost import CatBoostClassifier

from .preprocessing import FEATURE_NAMES


class FraudModel:
    def __init__(self, path):
        self.model = CatBoostClassifier()
        self.model.load_model(path)
        if tuple(self.model.feature_names_) != FEATURE_NAMES:
            raise ValueError("model features do not match preprocessing")

    def predict_proba(self, features):
        values = [features[name] for name in FEATURE_NAMES]
        return float(self.model.predict_proba([values], thread_count=1)[0][1])
