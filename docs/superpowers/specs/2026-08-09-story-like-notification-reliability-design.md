# Story Like Notification Reliability Design

## Goal

Guarantee that every newly accepted story-like request creates a durable notification intent. A request must not return success if its corresponding `story_like_outbox` entry cannot be verified inside the same database transaction.

Existing story likes that are missing notifications will not be replayed.

## Scope

- Keep the existing `PUT /profile-media/stories/{storyId}/like` contract.
- Keep the existing `like_event` Kafka payload and notification/Firebase contracts.
- Preserve the transactional story-like and outbox architecture.
- Add focused lifecycle logging from the HTTP request through outbox persistence, Kafka acknowledgement, notification persistence, and push delivery.
- Do not modify frontend behavior or recover historical missing notifications.

## Design

`StoryReactionService` continues to persist the LIKE state and outbox intent inside one `TransactionalOperator`. The outbox repository will expose a required-enqueue operation that:

1. Executes the idempotent outbox insert.
2. Reads the row back by `interaction_id` within the active transaction.
3. Validates that `story_id`, `actor_id`, and `owner_id` match the intended event.
4. Emits an error when the row is absent or inconsistent.

Because verification remains inside the same transaction, any missing or invalid outbox intent rolls back the story-like mutation. The controller only returns a successful response after transaction commit.

The publisher and notification consumer retain their current retry and durable-dedup behavior. No historical reconciliation job will be introduced.

## Observability

Logs will use stable fields (`storyId`, `actorId`, `ownerId`, `interactionId`) and cover:

- controller request and completion/failure;
- persisted LIKE state;
- verified outbox intent;
- transaction commit/rollback;
- outbox lease, Kafka send and broker acknowledgement;
- Kafka consumer receipt;
- `notification_events` and `user_notifications` persistence;
- Firebase sent, missing-token, or delivery-failed result.

No device token or sensitive payload will be logged.

## Error Handling

- Missing/inconsistent outbox intent is treated as `LIKE_CREATE_FAILED` and rolls back the transaction.
- Kafka failures remain in the outbox retry path.
- Notification persistence failures continue to fail Kafka processing so the consumer can retry.
- Firebase delivery failure does not remove the already persisted in-app notification; it is logged distinctly.

## Verification

- Repository/service test: successful enqueue is read back and accepted.
- Service test: absent or inconsistent outbox verification fails and prevents successful completion.
- Existing outbox publisher and notification handler tests remain green.
- Backend package/build succeeds.
- No migration or historical notification replay is performed.

