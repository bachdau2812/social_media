# Direct Story Like Notification Design

## Goal

Replace the Story-like transactional outbox with the same request-chain Kafka publishing pattern already used by post and comment likes. Newly created or retried Story likes must publish `like_event` directly and proceed through the existing notification persistence and Firebase delivery flow.

## Scope

- Keep `PUT /profile-media/stories/{storyId}/like` and its response unchanged.
- Keep Story reaction state in `story_views`.
- Keep `story_views.reaction_interaction_id` as the stable notification identity.
- Keep the existing `like_event` topic and Story event payload fields.
- Keep notification deduplication by interaction and recipient.
- Do not replay historical missing notifications.
- Remove the Story-like outbox code and database table.

## Architecture

`StoryReactionService` will persist the Story LIKE state, resolve the persisted `reaction_interaction_id`, build the Story `like_event`, and publish it through `KafkaSender<String, String>` in one subscribed Reactor chain. The controller returns only after the Kafka send publisher completes.

This matches the working `LikeService` pattern used by post and comment likes. Story state remains separate because Story viewers and current frontend state depend on `story_views` rather than the generic `likes` table.

The existing notification Kafka listener continues routing `targetType=STORY` to the Story notification path, which persists `notification_events` and `user_notifications` before attempting Firebase delivery.

## Retry and Failure Behavior

- A first LIKE creates a stable `reaction_interaction_id` and publishes it.
- Repeating PUT for an already-liked Story reuses the persisted interaction ID and publishes again.
- Notification persistence deduplicates retries with `LIKE_STORY:<interactionId>:<ownerId>`.
- Kafka publishing errors propagate to the HTTP request and are logged.
- As with the current post/comment implementation, a Kafka error does not reverse the already persisted LIKE. A repeated PUT repairs delivery using the same interaction ID.
- Firebase delivery failure does not remove the in-app notification already persisted by the consumer.

## Files and Schema

Modify:

- `StoryReactionService.java`: replace outbox enqueue with direct Kafka publishing.
- `StoryReactionServiceTest.java`: verify event payload, completion ordering, retry identity, and publisher failure propagation.
- `story_reply_schema.sql`: remove outbox creation and seed sections while retaining interaction identity and notification dedup schema.

Delete:

- `StoryLikeOutboxPublisher.java`
- `StoryLikeOutboxRepository.java`
- `StoryLikeOutboxEntry.java`
- Tests and source contracts dedicated only to Story-like outbox behavior.

Create:

- A manual migration that executes only `DROP TABLE IF EXISTS story_like_outbox;` with documentation explaining that the table is obsolete after direct publishing is deployed.

## Observability

Keep the existing controller, reaction, Kafka consumer, notification persistence, and Firebase delivery logs. Replace `outboxVerified` and outbox publisher logs with direct publisher lifecycle logs containing `storyId`, `actorId`, `ownerId`, and `interactionId`:

- `publishLikeEvent|sending`
- `publishLikeEvent|brokerAcknowledged`
- `publishLikeEvent|failed`

Do not log device tokens or sensitive notification payloads.

## Verification

- RED/GREEN service tests for direct Kafka send on first and repeated PUT.
- Verify the emitted JSON contains the required Story fields and stable interaction ID.
- Verify Kafka failure propagates and does not return a successful API response.
- Verify outbox-only classes are absent and no production reference remains.
- Verify migration retains reaction/dedup schema and drops only `story_like_outbox`.
- Run scoped Story notification tests and a backend package build.

