# Chat Audio Nullable Dimensions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow chat audio messages to be created when Cloudinary and the request do not provide positive width or height values.

**Architecture:** Keep the nullable dimension contract in `MediaMetadataRequest`. Exercise the full `SendMessageService.sendMessage` path with a fetched audio `Media`, then replace the primitive/boxed ternary expressions in metadata normalization with explicit nullable-safe assignments.

**Tech Stack:** Java 24, Spring WebFlux, Reactor, JUnit 5, Mockito, AssertJ.

## Global Constraints

- Do not change the request contract or database schema.
- Prefer positive Cloudinary dimensions; otherwise preserve nullable request dimensions.
- Keep URL, byte-size, MIME type, filename, duration, persistence, and event behavior unchanged.
- Commit only backend files in the `social_media` repository on `main`.

---

### Task 1: Nullable-safe chat audio media normalization

**Files:**
- Modify: `src/test/java/com/dauducbach/clone/modules/chat/service/SendMessageServiceTest.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/service/SendMessageService.java:119-133`

**Interfaces:**
- Consumes: `SendMessageService.sendMessage(String actorId, String conversationId, SendMessageRequest request)` and `MediaMetadataRequest` nullable `Integer width/height`.
- Produces: a completed `ChatMessageResponse` whose AUDIO metadata has null width/height when no positive dimensions exist.

- [ ] **Step 1: Add the failing service regression test**

Create an AUDIO request with valid URL, public ID, MIME type, size, filename and duration, but null width/height. Mock `MediaCompatibilityFacade.fetchMediaByPublicId` to return a `Media` with width/height zero, configure the existing conversation/message persistence collaborators, and assert the resulting response is AUDIO with null metadata width/height.

```java
MediaMetadataRequest metadata = new MediaMetadataRequest(
        "https://cdn.test/voice.webm", "voice-1", "audio/webm", 5L,
        "voice.webm", null, null, 1200L);
Media fetched = Media.builder()
        .assetId("asset-1")
        .publicId("voice-1")
        .resourceType("video")
        .bytes(5)
        .secureUrl("https://cdn.test/voice.webm")
        .width(0)
        .height(0)
        .build();
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```powershell
mvn "-Dtest=SendMessageServiceTest" test
```

Expected: the new case fails with `CHAT_MESSAGE_CREATE_FAILED`, caused by null `MediaMetadataRequest.width()` being unboxed via `Integer.intValue()`.

- [ ] **Step 3: Implement nullable-safe normalization**

Replace the mixed primitive/boxed conditional expressions with explicit assignments:

```java
Integer width = requested.width();
if (media.getWidth() > 0) {
    width = media.getWidth();
}
Integer height = requested.height();
if (media.getHeight() > 0) {
    height = media.getHeight();
}
```

Keep all other metadata fields unchanged.

- [ ] **Step 4: Run focused and regression tests**

Run:

```powershell
mvn "-Dtest=SendMessageServiceTest" test
mvn "-Dtest=ChatMessageValidatorTest,ChatResponseMapperTest,SendMessageServiceTest" test
```

Expected: focused service tests pass; chat regression suite reports 29 tests, 0 failures, 0 errors.

- [ ] **Step 5: Inspect and commit only the fix**

```powershell
git diff --check
git commit --only src/main/java/com/dauducbach/clone/modules/chat/service/SendMessageService.java src/test/java/com/dauducbach/clone/modules/chat/service/SendMessageServiceTest.java -m "fix: allow nullable chat audio dimensions"
```
