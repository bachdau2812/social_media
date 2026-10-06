-- Manual migration. Do not apply automatically. Operation keys are durable idempotency keys.
CREATE TABLE IF NOT EXISTS user_vector_update_operations (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    row_version BIGINT NOT NULL DEFAULT 0,
    operation_key VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    operation_kind VARCHAR(32) NOT NULL,
    baseline_vector LONGTEXT NOT NULL,
    desired_vector LONGTEXT NOT NULL,
    baseline_seq_no BIGINT NULL,
    baseline_primary_term BIGINT NULL,
    operation_id VARCHAR(36) NOT NULL,
    formula_version VARCHAR(64) NOT NULL,
    range_from TIMESTAMP(6) NULL,
    range_to TIMESTAMP(6) NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL,
    UNIQUE KEY uk_user_vector_operation_key (operation_key),
    KEY ix_user_vector_pending (user_id, status, id)
);
-- PREPARED -> ES_APPLIED -> COMPLETED; CONFLICTED permits the same logical key with a fresh
-- baseline/desired vector and a new attempt operation_id after reread/recompute under the lease.
-- DELETED is terminal when authoritative SQL user no longer exists; never recreate its ES document.
