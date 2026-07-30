# Post Detail Items And Music Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Return ordered post item data from the backend and render item captions and resumable shared/per-item music in post detail.

**Architecture:** A post-owned query service composes `PostDetails`, ordered `PostItem`, `Media`, and transformed music metadata into one response. The React detail modal hydrates this response and owns carousel audio state so playback survives item changes.

**Tech Stack:** Java 21, Spring WebFlux, R2DBC, Reactor, React 18, TypeScript, Vite.

## Global Constraints

- Keep persistence and post business logic in `modules/post`.
- Preserve the existing `GET /posts/{postId}` route.
- Shared music and per-item music are mutually exclusive.
- Shared music continues across image transitions; per-item music resumes independently.

---

### Task 1: Post Detail Response Contract

**Files:**
- Create: `src/main/java/com/dauducbach/clone/modules/post/dto/response/PostDetailResponse.java`
- Create: `src/main/java/com/dauducbach/clone/modules/post/dto/response/PostItemResponse.java`
- Create: `src/main/java/com/dauducbach/clone/modules/post/dto/response/PostMediaResponse.java`
- Create: `src/main/java/com/dauducbach/clone/modules/post/dto/response/PostMusicResponse.java`
- Create: `src/main/java/com/dauducbach/clone/modules/post/service/PostDetailQueryService.java`
- Test: `src/test/java/com/dauducbach/clone/modules/post/service/PostDetailQueryServiceTest.java`

**Interfaces:**
- Consumes: `PostService.getPostById`, `PostItemRepository.findByPostIdOrderByOrderNumberAsc`, `MediaRepository.findById`, `MusicService.getMusicById`.
- Produces: `Mono<PostDetailResponse> getPostDetail(String postId)`.

- [ ] **Step 1: Write a failing query-service test**

Verify item order, item captions, media mapping, transformed item music, and transformed shared music.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `.\mvnw.cmd -Dtest=PostDetailQueryServiceTest test`

- [ ] **Step 3: Implement the response records and query service**

Compose the response reactively and degrade optional music lookup failures to no music.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `.\mvnw.cmd -Dtest=PostDetailQueryServiceTest test`

### Task 2: Post Detail Endpoint

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/controller/PostController.java`
- Modify: `src/test/java/com/dauducbach/clone/modules/post/controller/PostModuleControllerTest.java`

**Interfaces:**
- Consumes: `PostDetailQueryService.getPostDetail`.
- Produces: `ApiResponse<PostDetailResponse>` from `GET /posts/{postId}`.

- [ ] **Step 1: Update the controller test for the detail response**
- [ ] **Step 2: Route the endpoint through `PostDetailQueryService`**
- [ ] **Step 3: Run controller test coverage**

### Task 3: Frontend Contract And Playback

**Files:**
- Modify: `social_media_FE/src/types.ts`
- Modify: `social_media_FE/src/App.tsx`
- Modify: `social_media_FE/src/styles.css`

**Interfaces:**
- Consumes: `GET /posts/{postId}` returning `PostDetailResponse`.
- Produces: hydrated detail carousel with item captions and shared/per-item playback state.

- [ ] **Step 1: Add detail DTO and enriched media types**
- [ ] **Step 2: Hydrate the modal from the detail endpoint**
- [ ] **Step 3: Add shared and per-item playback coordination**
- [ ] **Step 4: Add white media stage and thought-bubble caption styles**
- [ ] **Step 5: Build the frontend**

Run: `npm run build`

### Task 4: Verification And Documentation

**Files:**
- Modify: `docs/frontend-backend-change-summary.md`

- [ ] **Step 1: Build the backend without test compilation if unrelated legacy tests fail**

Run: `.\mvnw.cmd -Dmaven.test.skip=true package`

- [ ] **Step 2: Record the API and UI changes in the project summary**
