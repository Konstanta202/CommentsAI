CREATE TABLE IF NOT EXISTS default.messages (
    id String,
    source String,
    text String,
    region String,
    sentiment String,
    sentiment_score Float32,
    topics Array(String),
    hashtags Array(String),
    author String,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY created_at;