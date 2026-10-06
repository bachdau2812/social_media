-- Apply before enabling post.popularity.ingestion-enabled.
-- post_interaction broker retention must be configured to seven days by operators.
-- Receipts remain eight days; retries beyond this horizon are outside the retry contract.
CREATE TABLE IF NOT EXISTS post_interaction_receipts (
    actor_id VARCHAR(64) NOT NULL,
    event_id CHAR(36) NOT NULL,
    post_id VARCHAR(64) NOT NULL,
    impression_id CHAR(36) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    computed_score INT NOT NULL,
    accepted_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (actor_id, event_id),
    INDEX idx_post_interaction_receipt_expiry (accepted_at)
);
