ALTER TABLE post_details
    ADD COLUMN media_ratio VARCHAR(5) NULL DEFAULT '4:5';

UPDATE post_details
SET media_ratio = '4:5'
WHERE media_ratio IS NULL OR TRIM(media_ratio) = '';

ALTER TABLE post_details
    MODIFY COLUMN media_ratio VARCHAR(5) NOT NULL DEFAULT '4:5';