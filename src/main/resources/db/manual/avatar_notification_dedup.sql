-- Run before deploying avatar notification retries (MySQL 8).
-- This prefix is distinct from historical AVATAR_UPDATE:<recipient>:<actor>
-- keys, so repeated older avatar notifications are not made unique.
SET @avatar_schema = DATABASE();
SET @avatar_column_exists = (SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = @avatar_schema AND table_name = 'notification_events'
      AND column_name = 'avatar_upload_dedup_key');
SET @avatar_sql = IF(@avatar_column_exists = 0,
    'ALTER TABLE notification_events ADD COLUMN avatar_upload_dedup_key VARCHAR(255) GENERATED ALWAYS AS (CASE WHEN LEFT(dedup_key, 21) = ''AVATAR_UPDATE_UPLOAD:'' THEN dedup_key ELSE NULL END) STORED',
    'SELECT 1');
PREPARE avatar_stmt FROM @avatar_sql;
EXECUTE avatar_stmt;
DEALLOCATE PREPARE avatar_stmt;

SET @avatar_index_exists = (SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = @avatar_schema AND table_name = 'notification_events'
      AND index_name = 'uk_notification_events_avatar_upload_dedup');
SET @avatar_sql = IF(@avatar_index_exists = 0,
    'CREATE UNIQUE INDEX uk_notification_events_avatar_upload_dedup ON notification_events (avatar_upload_dedup_key)',
    'SELECT 1');
PREPARE avatar_stmt FROM @avatar_sql;
EXECUTE avatar_stmt;
DEALLOCATE PREPARE avatar_stmt;
