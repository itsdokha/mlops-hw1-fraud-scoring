CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    scored_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS transaction_scores_recent_idx
    ON transaction_scores (scored_at DESC);
