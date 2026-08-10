# Central Media Policy and Image-Only Scan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Centralize media upload limits in backend configuration, expose them to the frontend, set image/video limits to 100 MB application-wide, and bypass binary/external moderation scans for videos.

**Architecture:** A validated Spring ConfigurationProperties bean is the authoritative policy. Media moderation resolves media kind before scanning; images use MediaScanUtils while videos return an approved-without-scan decision and continue existing Cloudinary metadata/persistence flows. The frontend consumes a cached read-only policy endpoint with safe defaults and all upload surfaces use one shared validator.

**Tech Stack:** Java 21, Spring Boot WebFlux, Reactor, Kafka, Cloudinary, JUnit 5, Mockito, TypeScript, React, Vitest.

## Global Constraints

- Image maximum size is 100 MB.
- Video maximum size is 100 MB.
- Chat audio remains 50 MB.
- Videos are not downloaded through MediaScanUtils and are not sent to the external scan API.
- Existing Cloudinary metadata, persistence, Kafka, SSE, and upload contracts remain intact except additive media-type fields.
- Unknown media defaults to image scanning.
- Full Chat and mini-chat continue using shared chat behavior.

---

### Task 1: Backend Central Media Policy

**Files:**
- Create: 'src/main/java/com/dauducbach/clone/modules/media/configuration/MediaPolicyProperties.java'
- Create: 'src/main/java/com/dauducbach/clone/modules/media/configuration/MediaPolicyConfiguration.java'
- Create: 'src/main/java/com/dauducbach/clone/modules/media/dto/response/MediaUploadPolicyResponse.java'
- Create: 'src/main/java/com/dauducbach/clone/modules/media/controller/MediaPolicyController.java'
- Modify: 'src/main/resources/application.yaml'
- Test: 'src/test/java/com/dauducbach/clone/modules/media/configuration/MediaPolicyPropertiesTest.java'
- Test: 'src/test/java/com/dauducbach/clone/modules/media/controller/MediaPolicyControllerTest.java'

**Interfaces:**
- Produces: MediaPolicyProperties.imageMaxBytes(), videoMaxBytes(), audioMaxBytes().
- Produces: GET /media/upload-policy with imageMaxBytes, videoMaxBytes, and audioMaxBytes.

- [ ] **Step 1: Write failing property and controller tests**

Assert that DataSize values map to exact byte counts and the controller response mirrors the property bean.

- [ ] **Step 2: Run RED tests**

Run:

    mvn -Dtest=MediaPolicyPropertiesTest,MediaPolicyControllerTest test

Expected: compilation failure because the property and controller types do not exist.

- [ ] **Step 3: Implement minimal property bean, endpoint, and YAML**

Use environment-backed defaults for app.media.limits.image=100MB, app.media.limits.video=100MB, and app.media.limits.audio=50MB. Reject zero or negative values during bean construction/startup.

- [ ] **Step 4: Run GREEN tests**

Run the same Maven command and expect all tests to pass.

- [ ] **Step 5: Commit only Task 1 files**

Commit message: feat: centralize media upload policy.

### Task 2: Image-Only Moderation Across Post, Comment, Story, and Avatar

**Files:**
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/service/post/MediaModerationProvider.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/service/post/PostMediaModerationOrchestrator.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/dto/request/MediaUploadRequest.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/service/post/ImageScanWorker.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/service/comment/CommentService.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/service/comment/CommentMediaModerationOrchestrator.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/post/service/story/StoryMediaService.java'
- Modify: 'src/main/java/com/dauducbach/clone/modules/user/service/MediaForProfile.java'
- Modify: 'src/main/java/com/dauducbach/clone/utils/MediaScanUtils.java'
- Test: 'src/test/java/com/dauducbach/clone/modules/post/service/post/MediaModerationProviderTest.java'
- Test: 'src/test/java/com/dauducbach/clone/modules/post/service/post/PostMediaModerationOrchestratorTest.java'
- Test: 'src/test/java/com/dauducbach/clone/modules/post/service/comment/CommentMediaModerationOrchestratorTest.java'
- Test: 'src/test/java/com/dauducbach/clone/modules/post/service/story/StoryMediaServiceTest.java'
- Test: 'src/test/java/com/dauducbach/clone/modules/user/service/MediaForProfileTest.java'
- Test: 'src/test/java/com/dauducbach/clone/utils/MediaScanUtilsTest.java'

**Interfaces:**
- Consumes: central byte limits from Task 1.
- Produces: scan(mediaUrl, publicId, declaredType), where VIDEO returns approved without invoking MediaScanUtils.
- Produces: additive MediaUploadRequest.resourceType.

- [ ] **Step 1: Write failing media decision tests**

Cover explicit video, case-insensitive video, video URL inference, explicit image, unknown type, exact 100 MB, and over 100 MB. Verify scanner zero interactions for video and one interaction for image.

- [ ] **Step 2: Run RED provider tests**

    mvn -Dtest=MediaModerationProviderTest test

Expected: fail because current scan always invokes MediaScanUtils and limits are hard-coded at 50/500 MB.

- [ ] **Step 3: Implement shared kind resolution and configured size checks**

Resolve explicit type first, then URL extension, defaulting to IMAGE. Return an explicit approved decision for video and log the bypass. Keep Cloudinary metadata validation and persistence unchanged.

- [ ] **Step 4: Write failing flow tests**

Add Post, Comment, and Story tests proving video completes success without scanner calls; keep image rejection assertions. Add resourceType serialization/parsing assertions for Comment and mediaType assertions for Story.

- [ ] **Step 5: Run RED flow tests**

    mvn -Dtest=PostMediaModerationOrchestratorTest,CommentMediaModerationOrchestratorTest,StoryMediaServiceTest,MediaForProfileTest test

Expected: video cases fail because current orchestrators call scan unconditionally or do not propagate media type.

- [ ] **Step 6: Integrate type-aware moderation**

Post passes PostMediaScanItem.resourceType. Comment adds and propagates MediaUploadRequest.resourceType. Story publishes and consumes mediaType. Avatar passes IMAGE explicitly. MediaScanUtils uses the central image limit instead of the separate 10 MB setting.

- [ ] **Step 7: Run GREEN backend moderation tests**

Run provider and flow test commands plus MediaScanUtilsTest; expect all tests to pass.

- [ ] **Step 8: Commit only Task 2 files**

Commit message: fix: scan images only in media moderation.

### Task 3: Chat Backend Uses Central Limits

**Files:**
- Modify: 'src/main/java/com/dauducbach/clone/modules/chat/service/ChatMessageValidator.java'
- Modify: 'src/test/java/com/dauducbach/clone/modules/chat/service/ChatMessageValidatorTest.java'

**Interfaces:**
- Consumes: MediaPolicyProperties from Task 1.
- Preserves: five-minute voice duration and existing request errors.

- [ ] **Step 1: Write failing configurable-boundary tests**

Construct validator with a test policy and prove image/video at 100 MB pass, values one byte larger fail, and audio retains 50 MB.

- [ ] **Step 2: Run RED**

    mvn -Dtest=ChatMessageValidatorTest test

Expected: configured-boundary tests fail because validator uses static constants.

- [ ] **Step 3: Inject MediaPolicyProperties**

Replace static size constants with policy methods; do not change MIME, dimension, duration, or URL validation.

- [ ] **Step 4: Run GREEN**

Run the same command and expect all tests to pass.

- [ ] **Step 5: Commit only Task 3 files**

Commit message: refactor: apply central media limits to chat.

### Task 4: Frontend Shared Runtime Policy

**Files:**
- Create: '../social_media_FE/src/shared/media/mediaUploadPolicy.ts'
- Create: '../social_media_FE/src/shared/media/mediaUploadPolicy.test.ts'
- Create or modify: '../social_media_FE/src/shared/media/index.ts'
- Modify: '../social_media_FE/src/features/post/screens/PostCreationStudio.tsx'
- Modify: '../social_media_FE/src/features/post/components/PostSurfaces.tsx'
- Modify: '../social_media_FE/src/features/story/screens/StoryCreatorStudio.tsx'
- Modify: '../social_media_FE/src/features/chat/hooks/useChatMediaComposer.ts'
- Modify: relevant frontend tests for each surface.

**Interfaces:**
- Produces: DEFAULT_MEDIA_UPLOAD_POLICY.
- Produces: getMediaUploadPolicy(): Promise<MediaUploadPolicy>.
- Produces: currentMediaUploadPolicy(): MediaUploadPolicy.
- Produces: validateMediaFile(file, kind, policy): string or null.

- [ ] **Step 1: Write failing policy tests**

Cover valid API response, malformed response fallback, request failure fallback, single-request cache, exact 100 MB acceptance, and one-byte-over rejection.

- [ ] **Step 2: Run RED**

    npm test -- src/shared/media/mediaUploadPolicy.test.ts

Expected: import failure because shared policy module does not exist.

- [ ] **Step 3: Implement cached policy and validator**

Use safe defaults of 100 MB image, 100 MB video, and 50 MB audio. Start a non-blocking fetch on first use; validation always has a synchronous current policy.

- [ ] **Step 4: Run GREEN shared tests**

Run the same Vitest command and expect all tests to pass.

- [ ] **Step 5: Write failing surface tests**

Assert Post, Story, Comment, and shared Chat composer accept exact 100 MB image/video and reject one byte over. Assert Comment upload payload includes resourceType.

- [ ] **Step 6: Replace local constants and messages**

Use the shared validator in every upload surface. Format error text from policy size instead of hard-coding 50/500. Keep shared Chat hook behavior so full and mini-chat remain equivalent.

- [ ] **Step 7: Run GREEN frontend scoped tests**

    npm test -- src/shared/media src/features/post src/features/story/screens/StoryCreatorStudio.test.tsx src/features/chat
    npm run typecheck

Expected: all scoped tests and typecheck pass.

- [ ] **Step 8: Commit only Task 4 files in frontend repo**

Commit message: feat: use runtime media upload policy.

### Task 5: Integrated Verification

**Files:**
- No production files unless verification exposes a scoped regression.

- [ ] **Step 1: Search for stale hard-coded limits**

Search application code for 50 MB image and 500 MB video constants/messages. Only intentional 50 MB audio default may remain.

- [ ] **Step 2: Run backend scoped suite**

    mvn -Dtest=MediaPolicyPropertiesTest,MediaPolicyControllerTest,MediaModerationProviderTest,PostMediaModerationOrchestratorTest,CommentMediaModerationOrchestratorTest,StoryMediaServiceTest,MediaForProfileTest,MediaScanUtilsTest,ChatMessageValidatorTest test

- [ ] **Step 3: Run backend package**

    mvn -DskipTests package

- [ ] **Step 4: Run frontend scoped suite and build**

    npm test -- src/shared/media src/features/post src/features/story src/features/chat
    npm run typecheck
    npm run build

- [ ] **Step 5: Check scoped diffs**

Run git diff check for only task commits in both repositories and verify unrelated working-tree changes remain unstaged.
