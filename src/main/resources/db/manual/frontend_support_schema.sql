SET @schema_name = DATABASE();


CREATE TABLE IF NOT EXISTS user_social_media (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    link TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS user_job (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),
    position VARCHAR(255),
    from_date DATE,
    to_date DATE,
    is_public BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS user_university (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    school_name VARCHAR(255),
    major VARCHAR(255),
    from_date DATE,
    to_date DATE,
    is_graduate BOOLEAN NOT NULL DEFAULT FALSE,
    is_public BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS user_high_school (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    school_name VARCHAR(255),
    from_date DATE,
    to_date DATE,
    is_graduate BOOLEAN NOT NULL DEFAULT FALSE,
    is_public BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS user_settings (
    user_id VARCHAR(255) PRIMARY KEY,
    account_visibility VARCHAR(32) NOT NULL DEFAULT 'PUBLIC',
    story_visibility VARCHAR(32) NOT NULL DEFAULT 'FOLLOWERS',
    comment_permission VARCHAR(32) NOT NULL DEFAULT 'EVERYONE',
    mention_permission VARCHAR(32) NOT NULL DEFAULT 'EVERYONE',
    tag_approval_required BOOLEAN NOT NULL DEFAULT FALSE,
    activity_status_visible BOOLEAN NOT NULL DEFAULT TRUE,
    read_receipts_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    push_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    email_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    likes_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    comments_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    follows_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mentions_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    stories_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    messages_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    security_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sensitive_content_level VARCHAR(32) NOT NULL DEFAULT 'STANDARD',
    autoplay_video VARCHAR(32) NOT NULL DEFAULT 'WIFI_ONLY',
    theme VARCHAR(32) NOT NULL DEFAULT 'SYSTEM',
    reduced_motion BOOLEAN NOT NULL DEFAULT FALSE,
    text_scale DOUBLE NOT NULL DEFAULT 1.0,
    high_contrast BOOLEAN NOT NULL DEFAULT FALSE,
    always_show_captions BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS saved_collections (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    name VARCHAR(120) NOT NULL,
    cover_thumbnail_urls TEXT,
    item_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS saved_items (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    collection_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_saved_items_user_post_collection UNIQUE (user_id, post_id, collection_id)
);

CREATE TABLE IF NOT EXISTS user_drafts (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    draft_type VARCHAR(32) NOT NULL DEFAULT 'POST',
    thumbnail_url TEXT,
    media_count INT NOT NULL DEFAULT 0,
    caption_preview TEXT,
    payload TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_archive_items (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    content_id VARCHAR(255) NOT NULL,
    content_type VARCHAR(32) NOT NULL,
    thumbnail_url TEXT,
    caption_preview TEXT,
    archived_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_user_archive_content UNIQUE (user_id, content_id)
);

CREATE TABLE IF NOT EXISTS notification_events (
    id VARCHAR(255) PRIMARY KEY,
    actor_id VARCHAR(255),
    action_type VARCHAR(80) NOT NULL,
    entity_id VARCHAR(255),
    entity_type VARCHAR(80),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

SET @column_name = 'action_type';
SET @table_name = 'notification_events';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE notification_events ADD COLUMN action_type VARCHAR(80) NOT NULL DEFAULT ''SYSTEM'' AFTER actor_id', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS user_notifications (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    event_id VARCHAR(255),
    notification_status VARCHAR(32) NOT NULL DEFAULT 'UNREAD',
    read_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

SET @index_name = 'idx_saved_collections_user_id';
SET @table_name = 'saved_collections';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_saved_collections_user_id ON saved_collections (user_id, updated_at DESC)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_saved_items_user_id';
SET @table_name = 'saved_items';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_saved_items_user_id ON saved_items (user_id, created_at DESC)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_user_drafts_user_id';
SET @table_name = 'user_drafts';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_user_drafts_user_id ON user_drafts (user_id, updated_at DESC)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_user_archive_items_user_id';
SET @table_name = 'user_archive_items';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_user_archive_items_user_id ON user_archive_items (user_id, archived_at DESC)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_user_notifications_user_status';
SET @table_name = 'user_notifications';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_user_notifications_user_status ON user_notifications (user_id, notification_status, created_at DESC)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;




SET @table_name = 'post_details';
SET @column_name = 'music_id';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE post_details ADD COLUMN music_id VARCHAR(255) NULL AFTER validate_status', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @column_name = 'music_start';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE post_details ADD COLUMN music_start BIGINT NULL AFTER music_id', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
ALTER TABLE post_details MODIFY COLUMN music_start BIGINT NULL;

SET @column_name = 'music_end';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE post_details ADD COLUMN music_end BIGINT NULL AFTER music_start', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
ALTER TABLE post_details MODIFY COLUMN music_end BIGINT NULL;

SET @column_name = 'media_ratio';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE post_details ADD COLUMN media_ratio VARCHAR(5) NOT NULL DEFAULT ''4:5'' AFTER music_end', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
UPDATE post_details SET media_ratio = '4:5' WHERE media_ratio IS NULL OR TRIM(media_ratio) = '';
ALTER TABLE post_details MODIFY COLUMN media_ratio VARCHAR(5) NOT NULL DEFAULT '4:5';

SET @column_name = 'music_display_name';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists > 0, 'ALTER TABLE post_details DROP COLUMN music_display_name', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @column_name = 'music_url';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists > 0, 'ALTER TABLE post_details DROP COLUMN music_url', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS post_items (
    id VARCHAR(255) PRIMARY KEY,
    post_id VARCHAR(255) NOT NULL,
    order_number INT NOT NULL,
    media_id VARCHAR(255),
    caption TEXT,
    music_id VARCHAR(255),
    music_start BIGINT,
    music_end BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_post_items_post_order UNIQUE (post_id, order_number)
);
CREATE TABLE IF NOT EXISTS post_reposts (
    id VARCHAR(255) PRIMARY KEY,
    actor_id VARCHAR(255) NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    post_owner_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_post_reposts_actor_post UNIQUE (actor_id, post_id)
);
SET @index_name = 'idx_post_reposts_actor_id';
SET @table_name = 'post_reposts';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_post_reposts_actor_id ON post_reposts (actor_id, created_at DESC)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_post_reposts_post_id';
SET @table_name = 'post_reposts';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_post_reposts_post_id ON post_reposts (post_id)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @table_name = 'user_details';
SET @column_name = 'full_name';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE user_details ADD COLUMN full_name VARCHAR(255) NULL AFTER username', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
UPDATE user_details SET full_name = username WHERE full_name IS NULL OR TRIM(full_name) = '';

SET @column_name = 'school';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists > 0, 'ALTER TABLE user_details DROP COLUMN school', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @column_name = 'job';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists > 0, 'ALTER TABLE user_details DROP COLUMN job', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @table_name = 'user_stories';
SET @column_name = 'music_id';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE user_stories ADD COLUMN music_id VARCHAR(255) NULL AFTER media_type', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @column_name = 'music_url';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = @column_name);
SET @sql = IF(@column_exists = 0, 'ALTER TABLE user_stories ADD COLUMN music_url TEXT NULL AFTER music_id', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @index_name = 'idx_post_items_post_id';
SET @table_name = 'post_items';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_post_items_post_id ON post_items (post_id, order_number)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @index_name = 'idx_user_social_media_user_id';
SET @table_name = 'user_social_media';
SET @index_exists = (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = @table_name AND index_name = @index_name);
SET @sql = IF(@index_exists = 0, 'CREATE INDEX idx_user_social_media_user_id ON user_social_media (user_id)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
-- Keep the university date columns away from SQL reserved words used by R2DBC inserts.
SET @table_name = 'user_university';
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = 'from');
SET @new_column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = 'from_date');
SET @sql = IF(@column_exists = 1 AND @new_column_exists = 0, 'ALTER TABLE user_university CHANGE COLUMN `from` from_date DATE NULL', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = 'to');
SET @new_column_exists = (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = @table_name AND column_name = 'to_date');
SET @sql = IF(@column_exists = 1 AND @new_column_exists = 0, 'ALTER TABLE user_university CHANGE COLUMN `to` to_date DATE NULL', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;