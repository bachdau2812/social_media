-- Read-only rollout check. Select the application's database first.
-- An empty result means all listed chat schema requirements are present.
-- This checks column presence, not types, constraints, indexes or permissions.
SELECT required.table_name, required.column_name AS missing_column
FROM (
    SELECT 'messages' AS table_name, 'client_payload_hash' AS column_name
    UNION ALL SELECT 'messages', 'message_seq'
    UNION ALL SELECT 'messages', 'client_message_id'
    UNION ALL SELECT 'messages', 'metadata'
    UNION ALL SELECT 'messages', 'reply_to_seq'
    UNION ALL SELECT 'messages', 'reaction_version'
    UNION ALL SELECT 'messages', 'forwarded'
    UNION ALL SELECT 'conversations', 'last_message_seq'
    UNION ALL SELECT 'conversations', 'is_dissolved'
    UNION ALL SELECT 'conversations', 'pin_version'
    UNION ALL SELECT 'conversation_members', 'joined_seq'
    UNION ALL SELECT 'conversation_members', 'last_deleted_message_seq'
    UNION ALL SELECT 'conversation_members', 'last_delivered_seq'
    UNION ALL SELECT 'conversation_members', 'last_read_seq'
    UNION ALL SELECT 'conversation_members', 'nickname'
    UNION ALL SELECT 'chat_reaction_outbox', 'payload'
    UNION ALL SELECT 'chat_reaction_outbox', 'available_at'
    UNION ALL SELECT 'chat_reaction_outbox', 'lease_token'
    UNION ALL SELECT 'chat_reaction_outbox', 'leased_until'
    UNION ALL SELECT 'chat_reaction_outbox', 'attempts'
    UNION ALL SELECT 'message_reactions', 'reaction'
    UNION ALL SELECT 'chat_message_pins', 'message_id'
    UNION ALL SELECT 'chat_message_forwards', 'source_message_id'
    UNION ALL SELECT 'chat_message_media_refs', 'asset_id'
) required
LEFT JOIN information_schema.COLUMNS actual
  ON actual.TABLE_SCHEMA = DATABASE()
 AND actual.TABLE_NAME = required.table_name
 AND actual.COLUMN_NAME = required.column_name
WHERE actual.COLUMN_NAME IS NULL
ORDER BY required.table_name, required.column_name;
