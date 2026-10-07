-- Apply once to existing installations before deploying clientMessageId payload checks.
ALTER TABLE messages
    ADD COLUMN client_payload_hash CHAR(64) NULL AFTER client_message_id;
