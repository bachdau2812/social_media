CREATE TABLE IF NOT EXISTS audit_logs (
    id VARCHAR(36) PRIMARY KEY,
    actor_id VARCHAR(255) NOT NULL,
    actor_type VARCHAR(50) NOT NULL DEFAULT 'USER',
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100),
    resource_id VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'SUCCESS',
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_audit_logs_actor_created_at (actor_id, created_at),
    INDEX idx_audit_logs_action_created_at (action, created_at),
    INDEX idx_audit_logs_resource (resource_type, resource_id)
);
