-- Additive Story Like notification deduplication and Story Reply message type.
-- Safe to run more than once on MySQL 8+.
SET @schema_name = DATABASE();

SET @table_name = 'story_views';
SET @column_name = 'reaction_interaction_id';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE story_views ADD COLUMN reaction_interaction_id VARCHAR(36) NULL AFTER reaction',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @table_name = 'notification_events';
SET @column_name = 'story_like_dedup_key';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN story_like_dedup_key VARCHAR(255) GENERATED ALWAYS AS (CASE WHEN dedup_key LIKE ''LIKE_STORY:%'' THEN dedup_key ELSE NULL END) STORED',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @index_name = 'uk_notification_events_story_like_dedup';
SET @index_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND index_name = @index_name
);
SET @sql = IF(
    @index_exists = 0,
    'CREATE UNIQUE INDEX uk_notification_events_story_like_dedup ON notification_events (story_like_dedup_key)',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

INSERT INTO notification_template (id, action_type, template)
SELECT next_template.next_id,
       'LIKE_STORY',
       '{{USERNAME}} đã thích tin của bạn'
FROM (
    SELECT COALESCE(MAX(id), 0) + 1 AS next_id
    FROM notification_template
) next_template
WHERE NOT EXISTS (
    SELECT 1
    FROM notification_template
    WHERE action_type = 'LIKE_STORY'
);

SET @table_name = 'messages';
SET @constraint_name = 'chk_message_type';
SET @constraint_exists = (
    SELECT COUNT(*)
    FROM information_schema.table_constraints
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND constraint_name = @constraint_name
      AND constraint_type = 'CHECK'
);
SET @sql = IF(
    @constraint_exists > 0,
    'ALTER TABLE messages DROP CHECK chk_message_type',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE messages
    ADD CONSTRAINT chk_message_type
        CHECK (message_type IN (
            'TEXT',
            'IMAGE',
            'VIDEO',
            'FILE',
            'AUDIO',
            'SYSTEM',
            'STORY_REPLY'
        ));
