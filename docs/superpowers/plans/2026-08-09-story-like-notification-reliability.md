# Story Like Notification Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure every newly successful story-like transaction contains a verified durable outbox intent and can be traced through notification persistence and push delivery.

**Architecture:** Keep the existing transactional outbox and Kafka flow. Strengthen the repository boundary so enqueue returns a verified `StoryLikeOutboxEntry`; the service treats absence or field mismatch as `LIKE_CREATE_FAILED`, causing the enclosing reactive transaction to roll back. Existing historical likes are not replayed.

**Tech Stack:** Java 21, Spring Boot WebFlux, Spring Data R2DBC, Reactor, Reactor Kafka, JUnit 5, Mockito, Reactor Test.

## Global Constraints

- Keep `PUT /profile-media/stories/{storyId}/like` unchanged.
- Keep the `like_event` Kafka payload unchanged.
- Do not modify frontend code.
- Do not replay historical missing story-like notifications.
- Do not stage or overwrite unrelated dirty music, search, or Redis SSE files.

---

### Task 1: Require a verified outbox intent in the like transaction

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepository.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryReactionService.java`
- Test: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryReactionServiceTest.java`

**Interfaces:**
- Produces: `Mono<StoryLikeOutboxEntry> enqueueRequired(String interactionId, String storyId, String actorId, String ownerId, Instant createdAt)`.
- Consumes: the existing `StoryLikeOutboxEntry` fields and `TransactionalOperator` boundary.

- [ ] **Step 1: Write the failing service tests**

Replace enqueue stubs with `enqueueRequired` and add a test that returns `Mono.error(new AppException(ErrorCode.LIKE_CREATE_FAILED, ...))`; assert `StepVerifier` receives `LIKE_CREATE_FAILED` and never emits success. The success test must return an entry whose four identity fields match the request and assert the repository is invoked once per request.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
mvn -Dtest=StoryReactionServiceTest test
```

Expected: compilation/test failure because `enqueueRequired` does not exist.

- [ ] **Step 3: Implement required enqueue and verification**

Add a private `findByInteractionId` query selecting all outbox columns. Implement `enqueueRequired` by executing the existing idempotent insert, reading the row back in the same subscription/transaction, validating `interactionId`, `storyId`, `actorId`, and `ownerId`, and returning `AppException(ErrorCode.LIKE_CREATE_FAILED, "Story Like notification intent could not be persisted")` for absence or mismatch.

Log only stable IDs after verification:

```java
log.info("|StoryLikeOutboxRepository|enqueue|verified|storyId={}|actorId={}|ownerId={}|interactionId={}",
        storyId, actorId, ownerId, interactionId);
```

Update `StoryReactionService.like` to call `enqueueRequired(...)`, log `likePersisted` before enqueue and `outboxVerified` after it, and retain the existing transaction-commit log.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```powershell
mvn -Dtest=StoryReactionServiceTest,StoryReactionOutboxContractTest,StoryLikeOutboxRepositoryContractTest test
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit only Task 1 files**

```powershell
git add -- src/main/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepository.java src/main/java/com/dauducbach/clone/modules/post/service/story/StoryReactionService.java src/test/java/com/dauducbach/clone/modules/post/service/story/StoryReactionServiceTest.java src/test/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepositoryContractTest.java
git commit -m "fix: require story like notification intent"
```

### Task 2: Complete request-to-push observability

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/controller/story/StoryLibraryController.java`
- Modify only if an identified lifecycle edge lacks a log: `src/main/java/com/dauducbach/clone/modules/notification/service/PushModuleNotificationHandler.java`
- Modify only if an identified lifecycle edge lacks a log: `src/main/java/com/dauducbach/clone/modules/notification/service/NotificationPersistenceFlow.java`
- Test: `src/test/java/com/dauducbach/clone/modules/post/controller/story/StoryLibraryControllerTest.java`

**Interfaces:**
- Consumes: `StoryReactionService.like(String storyId, String actorId)`.
- Produces: unchanged `Mono<ApiResponse<Boolean>>` response with lifecycle logs.

- [ ] **Step 1: Write a failing controller logging contract test**

Use an in-memory Logback appender or the project’s existing log-capture pattern. Invoke `likeStory`, subscribe with `StepVerifier`, and assert request and completion logs include `storyId` and authenticated `actorId`. Add a failure case asserting an error log is produced while the original exception remains propagated.

- [ ] **Step 2: Run the controller test and verify RED**

Run:

```powershell
mvn -Dtest=StoryLibraryControllerTest test
```

Expected: failure because the controller currently has no like lifecycle logs.

- [ ] **Step 3: Add minimal controller lifecycle logs**

Add `@Slf4j` and log request, completion with `changed`, and failure with `errorType`. Do not log authentication credentials, device tokens, or notification bodies. Preserve the returned response exactly.

Audit existing publisher/consumer/persistence logs. Retain them when they already cover Kafka send/ack, consumer receipt, event save, recipient link, and Firebase sent/no-token/failed; do not duplicate those lines.

- [ ] **Step 4: Run Story notification tests and verify GREEN**

Run:

```powershell
mvn -Dtest=StoryLibraryControllerTest,StoryReactionServiceTest,StoryLikeOutboxPublisherTest,PushModuleNotificationHandlerTest test
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit only Task 2 files**

```powershell
git add -- src/main/java/com/dauducbach/clone/modules/post/controller/story/StoryLibraryController.java src/test/java/com/dauducbach/clone/modules/post/controller/story/StoryLibraryControllerTest.java
git commit -m "chore: trace story like requests"
```

### Task 3: Final verification

**Files:**
- No production changes expected.

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: verified backend build and scoped diff evidence.

- [ ] **Step 1: Run the scoped test suite**

```powershell
mvn -Dtest=StoryLibraryControllerTest,StoryReactionServiceTest,StoryReactionOutboxContractTest,StoryLikeOutboxRepositoryContractTest,StoryLikeOutboxPublisherTest,PushModuleNotificationHandlerTest test
```

Expected: all selected tests pass.

- [ ] **Step 2: Build backend without mutating unrelated sources**

```powershell
mvn -DskipTests package
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Verify scope and whitespace**

```powershell
git diff --check HEAD~2..HEAD
git status --short
```

Expected: scoped commits are whitespace-clean; unrelated pre-existing dirty files remain unstaged.

