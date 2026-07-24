CREATE TABLE IF NOT EXISTS messages
(
    id          UUID        PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    result      VARCHAR(50) NOT NULL
);
