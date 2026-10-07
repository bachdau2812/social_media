-- Add durable idempotency for notification consumers that carry EVENT_ID.
-- Existing dedup_key values are retained so pre-migration events can still be
-- recognized when the same source event is replayed.
-- Safe to run repeatedly on MySQL 8+.
SET @schema_name = DATABASE();

SET @table_name = 'notification_events';
SET @column_name = 'source_event_key';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN source_event_key VARCHAR(592) NULL AFTER dedup_key',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @index_name = 'uk_notification_events_source_event_key';
SET @index_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND index_name = @index_name
);
SET @sql = IF(
    @index_exists = 0,
    'CREATE UNIQUE INDEX uk_notification_events_source_event_key ON notification_events (source_event_key)',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
