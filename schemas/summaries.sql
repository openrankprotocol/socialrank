CREATE TABLE socialrank.channel_summaries (
    id BIGSERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL UNIQUE,
    summary JSONB NOT NULL,
    topic TEXT,
    few_words TEXT,
    one_sentence TEXT,
    error TEXT,
    model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_channel_summaries_created_at
    ON socialrank.channel_summaries (created_at DESC);