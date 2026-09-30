import unittest

from fraud_service.preprocessing import FEATURE_NAMES, preprocess, validate_columns


class PreprocessingTests(unittest.TestCase):
    def test_competition_columns(self):
        features = preprocess({
            "transaction_time": "2019-12-27 02:21", "amount": "100",
            "lat": "55.75", "lon": "37.62", "merchant_lat": "40.71",
            "merchant_lon": "-74", "population_city": "100000",
            "cat_id": "7", "gender": "F", "us_state": "NY",
        })
        self.assertEqual(tuple(features), FEATURE_NAMES)
        self.assertEqual(features["night"], 1)
        self.assertGreater(features["distance_km"], 1000)
        self.assertEqual(features["cat_id"], "7")
        self.assertGreater(features["log_population"], 0)

    def test_missing_optional_fields(self):
        row = {name: "" for name in ("transaction_time", "amount", "lat", "lon", "merchant_lat", "merchant_lon", "cat_id", "population_city")}
        features = preprocess(row)
        self.assertEqual(features["log_amount"], 0)
        self.assertEqual(features["cat_id"], "<missing>")

    def test_submission_template_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Не хватает колонок"):
            validate_columns(["index", "prediction"])


if __name__ == "__main__":
    unittest.main()
