-- Apply once to an existing chat schema before deploying the upgraded backend.
ALTER TABLE messages ADD COLUMN reaction_version BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS message_reactions (
    message_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    reaction VARCHAR(16) NOT NULL,
    reacted_at DATETIME(3) NOT NULL,
    PRIMARY KEY (message_id, user_id),
    KEY idx_message_reaction_list (message_id, reaction, user_id),
    CONSTRAINT fk_reaction_message FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    CONSTRAINT chk_chat_reaction CHECK (reaction IN ('HEART', 'LIKE', 'HAHA', 'WOW', 'SAD', 'ANGRY'))
) ENGINE=InnoDB;

-- Chat-owned at-least-once event queue. Unrelated vector flags do not control this publisher.
CREATE TABLE IF NOT EXISTS chat_reaction_outbox (
    id VARCHAR(36) NOT NULL,
    conversation_id VARCHAR(36) NOT NULL,
    payload LONGTEXT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    available_at DATETIME(3) NOT NULL,
    lease_token VARCHAR(36) NULL,
    leased_until DATETIME(3) NULL,
    attempts INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    KEY idx_chat_reaction_outbox_ready (available_at, leased_until, created_at)
) ENGINE=InnoDB;
