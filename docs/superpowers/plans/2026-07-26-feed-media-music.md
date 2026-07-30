# Feed Media And Music Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add display-aware feed media delivery and shared/per-item music playback.

**Architecture:** Cache original Cloudinary URLs and raw item metadata in the feed module, then transform copies while hydrating an API response. Map enriched feed items directly into Post models and keep playback local to each visibility-aware carousel.

**Tech Stack:** Java 21, Spring WebFlux, Reactor, Redis, React, TypeScript, Vite, Cloudinary delivery URLs.

## Global Constraints

- Do not store transformed media URLs in `post_details` feed cache.
- Preserve existing API fields and add optional music/item fields.
- Only one visible feed post may attempt audio playback at a time.

---

### Task 1: Backend feed media representation

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/constant/MediaDisplayType.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/post/service/PostDetailQueryService.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/feed/dto/cache/FeedPostDetailsCache.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/feed/dto/response/FeedItemResponse.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/feed/service/FeedService.java`

- [ ] Add `FEED(1360, 1700, "fill", "auto")`.
- [ ] Build raw post-item details for cache without applying delivery transformations.
- [ ] Cache raw item/music metadata with a schema version.
- [ ] Transform copied legacy and item media URLs during feed response hydration.

### Task 2: API composition

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/feed/controller/FeedController.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/frontend/controller/FrontendHomeController.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/frontend/service/HomeScreenService.java`

- [ ] Accept `mediaType=FEED` on feed and home routes.
- [ ] Route Discover and Friends through the same display-aware item hydrator.

### Task 3: Frontend feed playback

**Files:**
- Modify: `src/App.tsx`
- Modify: `src/styles.css`

- [ ] Map feed items and music into the existing `Post` model.
- [ ] Send `mediaType=FEED` from the home request.
- [ ] Add visibility-aware shared/per-item playback with saved positions and video suppression.
- [ ] Render compact mute and music attribution controls.

### Task 4: Documentation and builds

**Files:**
- Modify: `docs/frontend-backend-change-summary.md`

- [ ] Document the cache boundary, FEED transformer, API parameter, and playback behavior.
- [ ] Run `./mvnw.cmd -Dmaven.test.skip=true package`.
- [ ] Run `npm run build` in `social_media_FE`.