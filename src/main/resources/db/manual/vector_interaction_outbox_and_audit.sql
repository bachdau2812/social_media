-- Manual MySQL migration. Review/apply before deploying writers; dispatcher stays disabled by default.
-- No backfill or historical event replay. Keep these tables/cursors beyond short-term vector TTL.
CREATE TABLE IF NOT EXISTS vector_outbox_sequences (
    aggregate_id VARCHAR(255) NOT NULL PRIMARY KEY,
    next_sequence BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS vector_interaction_outbox (
    event_id VARCHAR(512) NOT NULL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    record_key VARCHAR(255) NOT NULL,
    payload LONGTEXT NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    sequence BIGINT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    sent_at TIMESTAMP(6) NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    lease_token VARCHAR(36) NULL,
    lease_until TIMESTAMP(6) NULL,
    UNIQUE KEY uq_vector_outbox_user_sequence (aggregate_id, sequence),
    INDEX idx_vector_outbox_dispatch (status, lease_until, created_at)
) ENGINE=InnoDB;

-- Nullable for historical logs. Uniqueness applies only to new canonical source identities.
SET @sql = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE()
    AND table_name = 'audit_logs' AND column_name = 'source_event_id') = 0,
    'ALTER TABLE audit_logs ADD COLUMN source_event_id VARCHAR(512) NULL', 'SELECT 1');
PREPARE vector_stmt FROM @sql; EXECUTE vector_stmt; DEALLOCATE PREPARE vector_stmt;
SET @sql = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE()
    AND table_name = 'audit_logs' AND index_name = 'uq_audit_source_event_id') = 0,
    'CREATE UNIQUE INDEX uq_audit_source_event_id ON audit_logs (source_event_id)', 'SELECT 1');
PREPARE vector_stmt FROM @sql; EXECUTE vector_stmt; DEALLOCATE PREPARE vector_stmt;

SET @sql = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE()
    AND table_name = 'comments' AND column_name = 'moderation_status') = 0,
    'ALTER TABLE comments ADD COLUMN moderation_status VARCHAR(20) NULL', 'SELECT 1');
PREPARE vector_stmt FROM @sql; EXECUTE vector_stmt; DEALLOCATE PREPARE vector_stmt;

-- Existing frontend_support_schema.sql already defines this constraint. If upgrading another schema,
-- inspect/resolve duplicate actor_id/post_id pairs before creating it; do not silently remove history.
SELECT actor_id, post_id, COUNT(*) AS duplicate_count FROM post_reposts
    GROUP BY actor_id, post_id HAVING COUNT(*) > 1;
SET @sql = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE()
    AND table_name = 'post_reposts' AND index_name = 'uq_post_reposts_actor_post') = 0,
    'CREATE UNIQUE INDEX uq_post_reposts_actor_post ON post_reposts (actor_id, post_id)', 'SELECT 1');
PREPARE vector_stmt FROM @sql; EXECUTE vector_stmt; DEALLOCATE PREPARE vector_stmt;

-- Task 6 durable recovery ledger. No TTL/automatic deletion: COMPLETED and SKIPPED retain dedup.
-- Canonical generation identifies a fixed topic partition topology, never silently repartition/replay.
CREATE TABLE IF NOT EXISTS feed_interaction_processing (
    source_event_id VARCHAR(512) NOT NULL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    action VARCHAR(20) NOT NULL,
    source_id VARCHAR(512) NOT NULL,
    occurred_at TIMESTAMP(6) NOT NULL,
    canonical_topic VARCHAR(255) NOT NULL,
    canonical_generation VARCHAR(100) NOT NULL,
    canonical_partition INT NOT NULL,
    canonical_offset BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    operation_id VARCHAR(36) NULL,
    desired_short_vector LONGTEXT NULL,
    vector_expires_at TIMESTAMP(6) NULL,
    reason VARCHAR(255) NULL,
    row_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_feed_processing_user_status (user_id, status),
    INDEX idx_feed_processing_cursor (canonical_generation, canonical_topic, canonical_partition, canonical_offset)
) ENGINE=InnoDB;
