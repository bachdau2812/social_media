SET @schema_name = DATABASE();
SET @table_name = 'user_stories';

SET @column_name = 'music_id';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE user_stories ADD COLUMN music_id VARCHAR(255) NULL AFTER media_type',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE user_stories
    MODIFY COLUMN music_id VARCHAR(255) NULL;

SET @column_name = 'music_start';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE user_stories ADD COLUMN music_start BIGINT NULL AFTER music_id',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @column_name = 'music_end';
SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = @column_name
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE user_stories ADD COLUMN music_end BIGINT NULL AFTER music_start',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE user_stories
    MODIFY COLUMN music_start BIGINT NULL,
    MODIFY COLUMN music_end BIGINT NULL;
