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

SET @table_name = 'story_like_outbox';
SET @table_exists = (
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = @schema_name
      AND table_name = @table_name
);
SET @sql = IF(
    @table_exists = 0,
    'CREATE TABLE story_like_outbox (
        interaction_id VARCHAR(36) NOT NULL,
        story_id VARCHAR(36) NOT NULL,
        actor_id VARCHAR(36) NOT NULL,
        owner_id VARCHAR(36) NOT NULL,
        created_at TIMESTAMP(6) NOT NULL,
        attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMP(6) NOT NULL,
        lease_token VARCHAR(36) NULL,
        lease_until TIMESTAMP(6) NULL,
        PRIMARY KEY (interaction_id),
        KEY idx_story_like_outbox_due (next_attempt_at, lease_until)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

INSERT INTO story_like_outbox (
    interaction_id, story_id, actor_id, owner_id,
    created_at, attempt_count, next_attempt_at
)
SELECT sv.reaction_interaction_id,
       sv.story_id,
       sv.viewer_id,
       us.user_id,
       COALESCE(sv.viewed_at, CURRENT_TIMESTAMP(6)),
       0,
       CURRENT_TIMESTAMP(6)
FROM story_views sv
JOIN user_stories us ON us.id = sv.story_id
WHERE sv.reaction = 'LIKE'
  AND sv.reaction_interaction_id IS NOT NULL
  AND sv.reaction_interaction_id <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM story_like_outbox outbox
      WHERE outbox.interaction_id = sv.reaction_interaction_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM notification_events ne
      WHERE ne.dedup_key = CONCAT('LIKE_STORY:', sv.reaction_interaction_id, ':', us.user_id)
  );
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
