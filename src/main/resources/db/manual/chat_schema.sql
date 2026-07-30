CREATE TABLE IF NOT EXISTS conversations (
    id VARCHAR(36) NOT NULL,
    conversation_type VARCHAR(16) NOT NULL,
    title VARCHAR(255) NULL,
    direct_key CHAR(64) NULL,
    last_message_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_message_id VARCHAR(36) NULL,
    last_message_at DATETIME(3) NULL,
    is_dissolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_by VARCHAR(64) NOT NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_conversations_direct_key (direct_key),
    CONSTRAINT chk_conversation_type
        CHECK (conversation_type IN ('DIRECT', 'GROUP'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS conversation_members (
    id VARCHAR(36) NOT NULL,
    conversation_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    nickname VARCHAR(100) NULL,
    member_role VARCHAR(16) NOT NULL,
    member_status VARCHAR(16) NOT NULL,
    joined_seq BIGINT UNSIGNED NOT NULL,
    last_delivered_seq BIGINT UNSIGNED NOT NULL,
    last_read_seq BIGINT UNSIGNED NOT NULL,
    last_deleted_message_seq BIGINT UNSIGNED NULL DEFAULT NULL,
    muted_until DATETIME(3) NULL,
    joined_at DATETIME(3) NOT NULL,
    left_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_conversation_member (conversation_id, user_id),
    KEY idx_member_user_status (user_id, member_status, conversation_id),
    CONSTRAINT fk_member_conversation FOREIGN KEY (conversation_id)
        REFERENCES conversations(id),
    CONSTRAINT chk_member_role CHECK (member_role IN ('USER', 'ADMIN')),
    CONSTRAINT chk_member_status
        CHECK (member_status IN ('ACTIVE', 'LEFT', 'REMOVED', 'BANNED'))
) ENGINE=InnoDB;

ALTER TABLE conversation_members
    ADD COLUMN IF NOT EXISTS nickname VARCHAR(100) NULL AFTER user_id;

ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS is_dissolved BOOLEAN NOT NULL DEFAULT FALSE AFTER last_message_at;

ALTER TABLE conversation_members
    ADD COLUMN IF NOT EXISTS last_deleted_message_seq BIGINT UNSIGNED NULL DEFAULT NULL AFTER last_read_seq;
ALTER TABLE conversation_members
    MODIFY COLUMN last_deleted_message_seq BIGINT UNSIGNED NULL DEFAULT NULL;

CREATE TABLE IF NOT EXISTS conversation_member_requests (
    id VARCHAR(36) NOT NULL,
    conversation_id VARCHAR(36) NOT NULL,
    target_user_id VARCHAR(64) NOT NULL,
    requested_by VARCHAR(64) NOT NULL,
    request_status VARCHAR(16) NOT NULL,
    resolved_by VARCHAR(64) NULL,
    resolved_at DATETIME(3) NULL,
    pending_key CHAR(64) NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_member_request_pending (pending_key),
    KEY idx_member_request_conversation
        (conversation_id, request_status, created_at),
    CONSTRAINT fk_request_conversation FOREIGN KEY (conversation_id)
        REFERENCES conversations(id),
    CONSTRAINT chk_request_status
        CHECK (request_status IN ('PENDING', 'APPROVED', 'REJECTED'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS messages (
    id VARCHAR(36) NOT NULL,
    conversation_id VARCHAR(36) NOT NULL,
    message_seq BIGINT UNSIGNED NOT NULL,
    client_message_id VARCHAR(36) NOT NULL,
    sender_id VARCHAR(64) NOT NULL,
    message_type VARCHAR(16) NOT NULL,
    content TEXT NULL,
    metadata JSON NULL,
    reply_to_seq BIGINT UNSIGNED NULL,
    created_at DATETIME(3) NOT NULL,
    edited_at DATETIME(3) NULL,
    deleted_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_message_sequence (conversation_id, message_seq),
    UNIQUE KEY uk_message_client_retry (sender_id, client_message_id),
    KEY idx_message_history (conversation_id, message_seq),
    CONSTRAINT fk_message_conversation FOREIGN KEY (conversation_id)
        REFERENCES conversations(id),
    CONSTRAINT chk_message_type
        CHECK (message_type IN ('TEXT', 'IMAGE', 'VIDEO', 'FILE', 'AUDIO', 'SYSTEM')),
    CONSTRAINT chk_message_sequence CHECK (message_seq > 0),
    CONSTRAINT chk_reply_sequence
        CHECK (reply_to_seq IS NULL OR reply_to_seq < message_seq)
) ENGINE=InnoDB;

-- Chat attachments reuse the shared media table. VARCHAR keeps existing owner values
-- and allows the CHAT_MESSAGE owner type without introducing a chat-only media table.
ALTER TABLE media
    MODIFY COLUMN owner_type VARCHAR(32) NOT NULL;
-- Apply to existing installations after adding SYSTEM timeline entries.
ALTER TABLE messages DROP CHECK chk_message_type;
ALTER TABLE messages
    ADD CONSTRAINT chk_message_type
        CHECK (message_type IN ('TEXT', 'IMAGE', 'VIDEO', 'FILE', 'AUDIO', 'SYSTEM'));

INSERT INTO notification_template (id, action_type, template)
SELECT next_template.next_id,
       'CHAT_MEMBER_REQUEST',
       '{ACTOR} đề xuất thêm {TARGET} vào nhóm'
FROM (
    SELECT COALESCE(MAX(id), 0) + 1 AS next_id
    FROM notification_template
) next_template
WHERE NOT EXISTS (
    SELECT 1 FROM notification_template current_template
    WHERE current_template.action_type = 'CHAT_MEMBER_REQUEST'
);