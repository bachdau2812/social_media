# Direct Story Like Notification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish new Story-like events directly to Kafka in the request chain, matching post/comment likes, and remove the obsolete Story-like outbox code and table.

**Architecture:** `StoryReactionService` keeps `story_views` as the state source and reuses the persisted `reaction_interaction_id`. After persistence it sends the existing Story payload directly through `KafkaSender`; retries republish the same identity and notification persistence deduplicates it. All outbox classes, tests, schema creation, and the database table are removed.

**Tech Stack:** Java 21, Spring Boot WebFlux, Spring Data R2DBC, Reactor Kafka, MySQL 8, JUnit 5, Mockito, Reactor Test.

## Global Constraints

- Keep `PUT /profile-media/stories/{storyId}/like` unchanged.
- Keep Story reaction state in `story_views`.
- Keep `reaction_interaction_id` and `LIKE_STORY:<interactionId>:<ownerId>` deduplication.
- Keep topic `like_event` and the current Story payload field names.
- Do not modify frontend code or replay historical notifications.
- Drop only `story_like_outbox`; do not remove Story reaction or notification dedup columns.
- Preserve unrelated dirty music, search, and Redis SSE changes.

---

### Task 1: Publish Story likes directly through Kafka

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryReactionService.java`
- Modify: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryReactionServiceTest.java`

**Interfaces:**
- Consumes: `KafkaSender<String, String>.send(Publisher<SenderRecord<String, String, String>>)`.
- Produces: unchanged `Mono<Boolean> like(String storyId, String actorId)` and a `like_event` JSON payload with `actorId`, `targetId`, `targetType`, `targetOwnerId`, `interactionId`, and `timestamp`.

- [ ] **Step 1: Write failing direct-publisher tests**

Replace outbox mocks with a mocked `KafkaSender`. Capture `SenderRecord` arguments and verify a first LIKE emits:

```json
{
  "actorId": "actor-1",
  "targetId": "story-1",
  "targetType": "STORY",
  "targetOwnerId": "owner-1",
  "interactionId": "interaction-1"
}
```

Add tests proving repeated PUT publishes the same persisted interaction ID and a failed Kafka publisher propagates the original error without returning a Boolean.

- [ ] **Step 2: Run test and verify RED**

```powershell
mvn -Dtest=StoryReactionServiceTest test
```

Expected: compilation or assertion failure because `StoryReactionService` still requires outbox dependencies and never calls `KafkaSender`.

- [ ] **Step 3: Implement the direct publisher**

Inject `KafkaSender<String, String>` and remove `StoryLikeOutboxRepository` plus `TransactionalOperator`. After `persistLike`, call:

```java
publishLikeEvent(new StoryLikeEventPayload(
        actorId,
        story.getId(),
        "STORY",
        story.getUserId(),
        result.view().getReactionInteractionId(),
        Instant.now()))
    .thenReturn(result);
```

Implement `publishLikeEvent` with one `ProducerRecord` and return the sender publisher chain without calling `subscribe()`. Inspect `SenderResult.exception()` and require non-null broker metadata before completion. Log `sending`, `brokerAcknowledged`, and `failed` with stable IDs.

- [ ] **Step 4: Run test and verify GREEN**

```powershell
mvn -Dtest=StoryReactionServiceTest test
```

Expected: all Story reaction tests pass.

- [ ] **Step 5: Commit Task 1**

```powershell
git add -- src/main/java/com/dauducbach/clone/modules/post/service/story/StoryReactionService.java src/test/java/com/dauducbach/clone/modules/post/service/story/StoryReactionServiceTest.java
git commit -m "fix: publish story likes directly"
```

### Task 2: Remove the obsolete outbox implementation and schema creation

**Files:**
- Delete: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryLikeOutboxPublisher.java`
- Delete: `src/main/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepository.java`
- Delete: `src/main/java/com/dauducbach/clone/modules/post/entity/story/StoryLikeOutboxEntry.java`
- Delete: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryLikeOutboxPublisherTest.java`
- Delete: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryReactionOutboxContractTest.java`
- Delete: `src/test/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepositoryContractTest.java`
- Modify: `src/main/resources/db/manual/story_reply_schema.sql`
- Create: `src/main/resources/db/manual/remove_story_like_outbox.sql`
- Create: `src/test/java/com/dauducbach/clone/modules/post/service/story/DirectStoryLikeContractTest.java`

**Interfaces:**
- Consumes: Task 1 direct `KafkaSender` implementation.
- Produces: a codebase with no Story outbox reference and an idempotent database cleanup migration.

- [ ] **Step 1: Write a failing source/schema contract test**

The test must assert that `StoryReactionService.java` contains `KafkaSender`, `publishLikeEvent`, and `LIKE_EVENT_TOPIC`, while production source contains no `StoryLikeOutbox` reference. It must assert `story_reply_schema.sql` retains `reaction_interaction_id` and `uk_notification_events_story_like_dedup` but no longer contains `CREATE TABLE story_like_outbox` or outbox seed SQL. It must assert the cleanup migration is exactly an explanatory comment plus:

```sql
DROP TABLE IF EXISTS story_like_outbox;
```

- [ ] **Step 2: Run contract test and verify RED**

```powershell
mvn -Dtest=DirectStoryLikeContractTest test
```

Expected: failure because outbox classes/schema still exist and cleanup migration does not.

- [ ] **Step 3: Delete outbox code and update migrations**

Delete the six outbox-only source/test files. Remove only the `story_like_outbox` create/seed section from `story_reply_schema.sql`. Create `remove_story_like_outbox.sql` with the approved idempotent DROP statement. Do not alter `reaction_interaction_id`, notification template, generated dedup column/index, or Story reply message constraint.

- [ ] **Step 4: Run contract and scoped tests**

```powershell
mvn "-Dtest=DirectStoryLikeContractTest,StoryReactionServiceTest,StoryLibraryControllerTest,PushModuleNotificationHandlerTest" test
```

Expected: all selected tests pass and no deleted class is referenced.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- src/main/java/com/dauducbach/clone/modules/post/service/story/StoryLikeOutboxPublisher.java src/main/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepository.java src/main/java/com/dauducbach/clone/modules/post/entity/story/StoryLikeOutboxEntry.java src/test/java/com/dauducbach/clone/modules/post/service/story/StoryLikeOutboxPublisherTest.java src/test/java/com/dauducbach/clone/modules/post/service/story/StoryReactionOutboxContractTest.java src/test/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepositoryContractTest.java src/main/resources/db/manual/story_reply_schema.sql src/main/resources/db/manual/remove_story_like_outbox.sql src/test/java/com/dauducbach/clone/modules/post/service/story/DirectStoryLikeContractTest.java
git commit -m "refactor: remove story like outbox"
```

### Task 3: Verify build and remove the database table safely

**Files:**
- No additional production files expected.

**Interfaces:**
- Consumes: `remove_story_like_outbox.sql` from Task 2.
- Produces: verified artifact and a database without `story_like_outbox` once no old application process depends on it.

- [ ] **Step 1: Run final scoped tests**

```powershell
mvn "-Dtest=DirectStoryLikeContractTest,StoryReactionServiceTest,StoryLibraryControllerTest,PushModuleNotificationHandlerTest" test
```

Expected: all selected tests pass.

- [ ] **Step 2: Build backend**

```powershell
mvn -DskipTests package
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Check whether an old backend process is still running**

```powershell
Get-NetTCPConnection -LocalPort 8888 -State Listen -ErrorAction SilentlyContinue
```

If an old process is listening, do not drop the table while its outbox scheduler is active. Report that the new artifact must be restarted first. If no old process is listening, apply `remove_story_like_outbox.sql` to `ins_clone`.

- [ ] **Step 4: Verify table removal when migration is applied**

```sql
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = 'story_like_outbox';
```

Expected: `0`.

- [ ] **Step 5: Verify commit scope and whitespace**

```powershell
git diff --check HEAD~2..HEAD
git status --short
```

Expected: direct Story-like commits are clean; unrelated pre-existing files remain unstaged.

