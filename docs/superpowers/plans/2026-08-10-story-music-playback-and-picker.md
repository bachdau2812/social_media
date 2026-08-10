# Story Music Playback and Picker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give notification-opened and Highlight Stories the same music playback data as Home-tray Stories, and make Story/Post creation share one music track browser.

**Architecture:** A backend `StoryPlaybackHydrator` bulk-resolves Story music metadata into an explicit archive response DTO used by tray, archive, and Highlight flows. The frontend keeps `StoryViewer` as the only playback engine and extracts the Post track list into a shared `MusicTrackBrowser` rendered by both creation studios.

**Tech Stack:** Java 21, Spring WebFlux, Spring Data R2DBC, Reactor, JUnit 5, Mockito, React 19, TypeScript, Vitest, Testing Library, Vite.

## Global Constraints

- Keep existing routes, request bodies, Music SSE events, and SpotiFLAC flow unchanged.
- Do not automatically fetch Music while viewing a Story.
- Missing/unfetched Music must not prevent Story display; image Stories fall back to five seconds.
- Reuse `StoryViewer` and `useStoryPlayback`; do not create entry-point-specific audio players.
- Post and Story must render the same shared track-list component.
- Preserve unrelated dirty worktree changes and stage only task-owned files.

---

### Task 1: Shared Backend Story Playback Projection

**Files:**
- Create: `src/main/java/com/dauducbach/clone/modules/post/dto/story/response/StoryArchiveResponse.java`
- Create: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryPlaybackHydrator.java`
- Create: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryPlaybackHydratorTest.java`

**Interfaces:**
- Consumes: `UserStories`, `MusicsRepository.findAllById(Iterable<String>)`, `StoryMusicSegmentPolicy.durationSeconds(String, Long, Long)`.
- Produces: `Mono<StoryArchiveResponse> hydrate(UserStories story, String transformedMediaUrl)` and `Mono<List<StoryArchiveResponse>> hydrateAll(List<UserStories> stories, Function<UserStories, String> mediaUrlResolver)`.

- [ ] **Step 1: Write failing hydrator tests**

Cover Music fallback, persisted URL preference, missing Music, duration, and input order:

```java
@Test
void resolvesCatalogMusicAndPreservesStoryOrder() {
    when(musicsRepository.findAllById(any(Iterable.class)))
            .thenReturn(Flux.just(music("music-2", "/two.mp3"), music("music-1", "/one.mp3")));

    StepVerifier.create(hydrator.hydrateAll(
                    List.of(story("story-1", "music-1", null), story("story-2", "music-2", null)),
                    UserStories::getMediaUrl))
            .assertNext(items -> {
                assertThat(items).extracting(StoryArchiveResponse::id)
                        .containsExactly("story-1", "story-2");
                assertThat(items.getFirst().musicUrl()).isEqualTo("/one.mp3");
                assertThat(items.getFirst().durationSeconds()).isEqualTo(30L);
            })
            .verifyComplete();
}
```

- [ ] **Step 2: Run RED test**

Run: `mvn -Dtest=StoryPlaybackHydratorTest test`

Expected: compilation failure because `StoryPlaybackHydrator` and `StoryArchiveResponse` do not exist.

- [ ] **Step 3: Add explicit archive response DTO**

Define a record containing the current archive JSON fields plus `musicName` and `durationSeconds`:

```java
public record StoryArchiveResponse(
        String id, String userId, String mediaUrl, String mediaType,
        String musicId, String musicUrl, String musicName,
        Long musicStart, Long musicEnd, Long durationSeconds,
        String publicationId, Integer publicationOrder, Integer publicationItemCount,
        String status, Instant createdAt, Instant expiredAt, Boolean viewerSeen
) {}
```

- [ ] **Step 4: Implement bulk playback hydration**

Normalize distinct Music IDs, load them once with `findAllById`, index by ID, then map original Stories in order. Resolve URL as `firstNonBlank(story.getMusicUrl(), music.getSongUrl())`; calculate duration with `StoryMusicSegmentPolicy`; never fail when Music is missing.

- [ ] **Step 5: Run GREEN test**

Run: `mvn -Dtest=StoryPlaybackHydratorTest test`

Expected: all hydrator tests pass.

- [ ] **Step 6: Commit Task 1**

```powershell
git add src/main/java/com/dauducbach/clone/modules/post/dto/story/response/StoryArchiveResponse.java src/main/java/com/dauducbach/clone/modules/post/service/story/StoryPlaybackHydrator.java src/test/java/com/dauducbach/clone/modules/post/service/story/StoryPlaybackHydratorTest.java
git commit -m "feat: hydrate story music playback"
```

---

### Task 2: Apply Hydration to Tray, Archive, and Highlight APIs

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/dto/story/response/StoryHighlightResponse.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryTrayQueryService.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryMediaService.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/post/service/story/StoryLibraryService.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/post/controller/story/StoryMediaController.java`
- Modify: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryTrayQueryServiceTest.java`
- Modify: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryMediaServiceTest.java`
- Modify: `src/test/java/com/dauducbach/clone/modules/post/service/story/StoryLibraryServiceTest.java`

**Interfaces:**
- Consumes: Task 1 `StoryPlaybackHydrator` and `StoryArchiveResponse`.
- Produces: `PageResponse<StoryArchiveResponse>` from archive endpoints and `List<StoryArchiveResponse>` in `StoryHighlightResponse.stories()`.

- [ ] **Step 1: Write failing archive and Highlight tests**

Assert both responses contain catalog-resolved `musicUrl`, `musicName`, segment, and duration when `UserStories.musicUrl` is null but `musicId` exists. Update tray tests to verify its existing playback contract through the shared hydrator.

- [ ] **Step 2: Run RED service tests**

Run: `mvn "-Dtest=StoryTrayQueryServiceTest,StoryMediaServiceTest,StoryLibraryServiceTest" test`

Expected: failures because archive/Highlight still expose `UserStories` and do not invoke the hydrator.

- [ ] **Step 3: Change response types and integrate hydrator**

Use the hydrator after viewer-seen calculation and before building each response. `StoryMediaController` returns:

```java
Mono<ApiResponse<PageResponse<StoryArchiveResponse>>>
```

`StoryHighlightResponse` changes only its Java element type:

```java
List<StoryArchiveResponse> stories
```

Keep response property names and routes unchanged.

- [ ] **Step 4: Reuse playback projection in the tray**

Remove direct per-story Music repository lookup from `StoryTrayQueryService`. Convert hydrated playback fields plus identity/viewer reaction into `StoryTrayResponse` so Home behavior remains unchanged.

- [ ] **Step 5: Run GREEN backend Story tests**

Run: `mvn "-Dtest=StoryPlaybackHydratorTest,StoryTrayQueryServiceTest,StoryMediaServiceTest,StoryLibraryServiceTest" test`

Expected: all tests pass.

- [ ] **Step 6: Commit Task 2**

Stage only the listed backend files and commit with `feat: expose story music in archives`.

---

### Task 3: Preserve Archive Playback Data in Frontend Story Flows

**Files (frontend repository `../social_media_FE`):**
- Modify: `src/features/story/model/story.dto.ts`
- Modify: `src/features/story/model/story.mapper.ts`
- Modify: `src/features/story/model/story.mapper.test.ts`
- Modify: `src/features/notification/core/routeResolver.test.ts`
- Modify: `src/features/story/components/StoryViewer.test.tsx`

**Interfaces:**
- Consumes: backend `StoryArchiveResponse` JSON from Task 2.
- Produces: `archivedStoryToItem()` values with `musicUrl`, `musicName`, `musicStart`, `musicEnd`, and `durationSeconds` for `StoryViewer`.

- [ ] **Step 1: Write failing mapper tests**

```ts
it("preserves playback data for archived and highlighted stories", () => {
  const item = archivedStoryToItem({
    ...archive,
    musicId: "track-1",
    musicUrl: "/track-1.mp3",
    musicName: "Track One",
    musicStart: 12,
    musicEnd: 42,
    durationSeconds: 30,
  });
  expect(item).toMatchObject({
    musicUrl: "/track-1.mp3",
    musicName: "Track One",
    musicStart: 12,
    musicEnd: 42,
    durationSeconds: 30,
  });
});
```

- [ ] **Step 2: Run RED mapper test**

Run: `npm test -- src/features/story/model/story.mapper.test.ts`

Expected: `musicName`/`durationSeconds` are missing because mapper reads them only from tray DTOs.

- [ ] **Step 3: Generalize Story media source mapping**

Add nullable `musicName` and `durationSeconds` to `StoryArchiveDto`. Map these properties from the common source rather than the optional tray-only argument. Keep viewer reaction tray-specific.

- [ ] **Step 4: Add notification and Viewer playback coverage**

Extend `routeResolver.test.ts` to retain the single-Story notification destination assertion, and extend `StoryViewer.test.tsx` to render an `archivedStoryToItem` result containing archive playback fields. Assert the existing viewer creates one audio element with the hydrated URL and segment. The Highlight entry point uses the same `archivedStoryToItem` function, so the mapper regression test covers its DTO conversion without adding a second audio player.

- [ ] **Step 5: Run GREEN Story tests**

Run: `npm test -- src/features/story src/features/notification`

Expected: focused Story and notification tests pass.

- [ ] **Step 6: Commit Task 3 in frontend repository**

Stage only the four listed frontend files and commit with `fix: preserve music for archived stories`.

---

### Task 4: Extract and Reuse the Music Track Browser

**Files (frontend repository `../social_media_FE`):**
- Create: `src/shared/music/MusicTrackBrowser.tsx`
- Create: `src/shared/music/MusicTrackBrowser.css`
- Create: `src/shared/music/MusicTrackBrowser.test.tsx`
- Modify: `src/shared/music/index.ts`
- Modify: `src/features/post/screens/PostCreationStudio.tsx`
- Modify: `src/features/post/styles/post-media.css`
- Modify: `src/features/post/screens/PostCreationStudio.test.tsx`
- Modify: `src/features/story/screens/StoryCreatorStudio.tsx`
- Modify: `src/features/story/screens/StoryCreatorMobileFirst.css`
- Modify: `src/features/story/screens/StoryCreatorStudio.test.tsx`

**Interfaces:**
- Consumes: `MusicDto`, loading/pagination/selection/preview/fetch state and callbacks.
- Produces: `MusicTrackBrowser` exported from `src/shared/music/index.ts`.

- [ ] **Step 1: Write failing shared component test**

Render fetched and unfetched tracks and assert artwork, title, artist, duration, Preview/Pause, Fetch/Processing, Select/selected, empty/loading, query, and Load more callbacks.

- [ ] **Step 2: Run RED shared component test**

Run: `npm test -- src/shared/music/MusicTrackBrowser.test.tsx`

Expected: import failure because `MusicTrackBrowser` does not exist.

- [ ] **Step 3: Implement shared browser and styles**

Export this prop-driven component:

```ts
export type MusicTrackBrowserProps = {
  tracks: MusicDto[];
  query: string;
  loading: boolean;
  loadingMore: boolean;
  hasMore: boolean;
  selectedId: string | null;
  previewingId: string | null;
  fetchingTrackIds: Set<string>;
  onFetch(track: MusicDto): void;
  onQueryChange(value: string): void;
  onLoadMore(): void;
  onPreview(track: MusicDto): void;
  onSelect(track: MusicDto): void;
  onClose(): void;
};
```

Move only track-list CSS from the Post stylesheet into the shared stylesheet. Preserve class names to prevent a Post visual regression.

- [ ] **Step 4: Replace Post's local TrackBrowser**

Delete local `TrackBrowser` and `MusicArtwork` implementations, import the shared component, and wire existing callbacks/state without changing Post selection semantics.

- [ ] **Step 5: Replace Story's bespoke result list**

Render `MusicTrackBrowser` inside the Story overlay. Adapt existing Story callbacks:

- `onFetch` -> existing `fetchTrack`;
- `onPreview` -> existing segment-aware preview callback;
- `onSelect` -> existing `selectMusic`;
- `onQueryChange` -> `setMusicQuery`;
- `onLoadMore` -> existing `loadMoreMusic`.

Keep Story's `MusicSegmentEditor` and per-draft selection unchanged.

- [ ] **Step 6: Run GREEN component and studio tests**

Run: `npm test -- src/shared/music/MusicTrackBrowser.test.tsx src/features/post/screens/PostCreationStudio.test.tsx src/features/story/screens/StoryCreatorStudio.test.tsx`

Expected: all tests pass and both studios contain the shared browser.

- [ ] **Step 7: Commit Task 4 in frontend repository**

Stage only Task 4 files and commit with `refactor: share music track browser`.

---

### Task 5: Cross-Surface Verification

**Files:** No production files unless verification reveals an in-scope regression.

**Interfaces:** Verifies Tasks 1-4 as one flow.

- [ ] **Step 1: Run backend focused tests**

Run:

```powershell
mvn "-Dtest=StoryPlaybackHydratorTest,StoryTrayQueryServiceTest,StoryMediaServiceTest,StoryLibraryServiceTest" test
```

Expected: zero failures and zero errors.

- [ ] **Step 2: Build backend**

Run: `mvn -DskipTests package`

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Run frontend focused tests**

Run:

```powershell
npm test -- src/features/story src/features/notification src/shared/music/MusicTrackBrowser.test.tsx src/features/post/screens/PostCreationStudio.test.tsx
```

Expected: all selected suites pass.

- [ ] **Step 4: Typecheck and build frontend**

Run:

```powershell
npm run typecheck
npm run build
```

Expected: both commands exit zero.

- [ ] **Step 5: Inspect scoped diffs**

Run `git diff --check` in both repositories and confirm only task-owned files are staged/committed. Document any unrelated pre-existing warnings without modifying those files.
