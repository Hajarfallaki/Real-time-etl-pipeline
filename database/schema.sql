CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(10),
    amount NUMERIC(10, 2),
    type VARCHAR(20),
    status VARCHAR(20),
    event_timestamp TIMESTAMPTZ,
    amount_category VARCHAR(10),
    processed_at TIMESTAMPTZ
);
