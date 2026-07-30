# Feed Media Delivery And Music Design

## Goal

Deliver Cloudinary media optimized for the home feed and play shared or per-item post music with the same continuity rules as Post Detail.

## Backend

`MediaDisplayType.FEED` targets the 680x850 CSS feed frame at DPR 2 using 1360x1700 `c_fill,g_auto`. `FeedPostDetailsCache` stores only original media URLs plus raw post-item and music metadata. A schema version invalidates older cache payloads that lack item music. `FeedService` transforms legacy media and item media only while building a response for the requested display type.

The feed response includes post-level music and ordered post items. Both Discover and Friends use the same FeedService hydration path. `/home` and `/feed` accept `mediaType`, defaulting to `FEED`.

## Frontend

The home request sends `mediaType=FEED`. Feed mapping prefers ordered post items and keeps legacy media as fallback. Each carousel owns one audio element. Shared music keeps one timeline across image changes; per-item music keeps independent positions. Video pauses music. An IntersectionObserver permits playback only while the post media is sufficiently visible, preventing concurrent feed audio.

## Verification

Build the Spring Boot backend with tests skipped and build the Vite TypeScript frontend. Verify cache DTOs contain raw URLs and response mapping is the only feed transformation boundary.