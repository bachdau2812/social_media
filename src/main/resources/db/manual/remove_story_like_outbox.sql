-- Remove the obsolete Story Like notification outbox after deploying direct Kafka publishing.
DROP TABLE IF EXISTS story_like_outbox;
