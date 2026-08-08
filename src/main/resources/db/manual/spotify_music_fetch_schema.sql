SET @schema_name = DATABASE();
SET @table_name = 'musics';

SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = 'release_year'
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE musics ADD COLUMN release_year SMALLINT DEFAULT NULL AFTER category',
    'ALTER TABLE musics MODIFY COLUMN release_year SMALLINT DEFAULT NULL'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = 'album_name'
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE musics ADD COLUMN album_name VARCHAR(1000) DEFAULT NULL AFTER release_year',
    'ALTER TABLE musics MODIFY COLUMN album_name VARCHAR(1000) DEFAULT NULL'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = 'fetched'
);
SET @sql = IF(
    @column_exists = 0,
    'ALTER TABLE musics ADD COLUMN fetched TINYINT DEFAULT NULL AFTER album_name',
    'ALTER TABLE musics MODIFY COLUMN fetched TINYINT DEFAULT NULL'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
UPDATE musics
SET fetched = CASE
    WHEN song_url IS NULL OR TRIM(song_url) = '' THEN 0
    ELSE 1
END
WHERE fetched IS NULL;

SET @column_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = @table_name
      AND column_name = 'release_date'
);
SET @sql = IF(
    @column_exists = 1,
    'ALTER TABLE musics DROP COLUMN release_date',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
