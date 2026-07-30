-- Collections were removed from Library. Run once after deploying code that no longer reads collection_id.
ALTER TABLE saved_items DROP COLUMN IF EXISTS collection_id;
DROP TABLE IF EXISTS saved_collections;
