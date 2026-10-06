-- Apply once AFTER chat_reactions_schema.sql, before starting this backend. No live migration is performed.
-- Preflight on the actual schema before applying this file:
-- SELECT COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME
-- FROM information_schema.COLUMNS
-- WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'media' AND COLUMN_NAME = 'asset_id';
-- Match chat_message_media_refs.asset_id to that exact type/charset/collation.
-- VARCHAR(255) below follows existing chat SQL test fixtures; the repository does not contain base media DDL.
-- ALTER statements are intentionally apply-once; record the migration and do not rerun it after a partial apply.
ALTER TABLE messages ADD COLUMN forwarded BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE conversations ADD COLUMN pin_version BIGINT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS chat_message_pins (
    conversation_id VARCHAR(36) NOT NULL,
    message_id VARCHAR(36) NOT NULL,
    pinned_by VARCHAR(64) NOT NULL,
    pinned_at DATETIME(3) NOT NULL,
    PRIMARY KEY (conversation_id, message_id),
    CONSTRAINT fk_chat_pin_conversation FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
    CONSTRAINT fk_chat_pin_message FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS chat_message_forwards (
    message_id VARCHAR(36) NOT NULL PRIMARY KEY,
    source_conversation_id VARCHAR(36) NOT NULL,
    source_message_id VARCHAR(36) NOT NULL,
    CONSTRAINT fk_chat_forward_message FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
) ENGINE=InnoDB;
-- Source IDs are server-only idempotency provenance, never included in REST or realtime responses.
CREATE TABLE IF NOT EXISTS chat_message_media_refs (
    message_id VARCHAR(36) NOT NULL,
    asset_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (message_id, asset_id),
    CONSTRAINT fk_chat_media_ref_message FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    CONSTRAINT fk_chat_media_ref_asset FOREIGN KEY (asset_id) REFERENCES media(asset_id)
) ENGINE=InnoDB;
-- Reuse chat_reaction_outbox unchanged as the single chat durable queue.
