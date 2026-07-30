-- Additive Story publication metadata and durable notification deduplication.
-- Safe to run more than once on MySQL 8+.
SET @schema_name = DATABASE();

SET @table_name = 'user_stories';
SET @column_name = 'publication_id';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE user_stories ADD COLUMN publication_id VARCHAR(64) NULL AFTER music_end', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @column_name = 'publication_order';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE user_stories ADD COLUMN publication_order INT NULL AFTER publication_id', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @column_name = 'publication_item_count';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE user_stories ADD COLUMN publication_item_count INT NULL AFTER publication_order', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

UPDATE user_stories
SET publication_id = id,
    publication_order = 1,
    publication_item_count = 1
WHERE publication_id IS NULL;

SET @index_name = 'idx_user_stories_publication_order';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_user_stories_publication_order ON user_stories (publication_id, publication_order)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'uk_user_stories_publication_item';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE UNIQUE INDEX uk_user_stories_publication_item ON user_stories (user_id, publication_id, publication_order)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @table_name = 'notification_events';
-- Only Story publication keys are unique. Existing notification categories retain
-- their original repeat behavior, while concurrent Story item events race safely.
SET @column_name = 'story_publication_dedup_key';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE notification_events ADD COLUMN story_publication_dedup_key VARCHAR(255) GENERATED ALWAYS AS (CASE WHEN dedup_key LIKE ''UP_STORY:%'' THEN dedup_key ELSE NULL END) STORED', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Remove the broad unique index from an earlier draft of this migration if present.
SET @index_name = 'uk_notification_events_dedup_key';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists > 0, 'DROP INDEX uk_notification_events_dedup_key ON notification_events', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'uk_notification_events_story_publication_dedup';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE UNIQUE INDEX uk_notification_events_story_publication_dedup ON notification_events (story_publication_dedup_key)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
