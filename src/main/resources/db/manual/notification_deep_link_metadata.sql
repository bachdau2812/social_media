-- Additive notification payload fields used by in-app notifications and FCM deep links.
-- Safe to run more than once on MySQL 8+.
SET @schema_name = DATABASE();
SET @table_name = 'notification_events';

SET @column_name = 'content';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN content TEXT NULL AFTER entity_type',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @column_name = 'metadata';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN metadata LONGTEXT NULL AFTER content',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @column_name = 'deep_link';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN deep_link VARCHAR(1000) NULL AFTER metadata',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @column_name = 'dedup_key';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN dedup_key VARCHAR(255) NULL AFTER deep_link',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_notification_events_dedup_key';
SET @index_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND index_name = @index_name
);
SET @sql = IF(
    @index_exists = 0,
    'CREATE INDEX idx_notification_events_dedup_key ON notification_events (dedup_key)',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
