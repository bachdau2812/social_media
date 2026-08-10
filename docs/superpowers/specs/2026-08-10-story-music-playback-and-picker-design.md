# Story Music Playback and Picker Design

## Goal

Make Story playback consistent regardless of how the viewer opens it. A Story opened from a notification or a profile Highlight must receive the same music URL, selected segment, and playback duration as the same Story opened from the Home tray.

Make the Story creation music browser use the same track-list presentation and interaction behavior as the Post creation music browser.

## Current Problem

The Home Story tray resolves `user_stories.music_id` through the Music repository, falls back from `user_stories.music_url` to `musics.song_url`, and calculates the Story duration from `music_start` and `music_end`.

The archive endpoint used by notification navigation and the Highlight endpoint return `UserStories` directly. Story creation intentionally persists `music_id` while sending `music_url = null`, so these responses usually contain a null `musicUrl`. They also do not expose `durationSeconds`. The frontend therefore opens the correct image or video but has no audio source and falls back to five seconds for an image Story.

The Post and Story creation studios separately implement their track-list markup. Their API and fetch flows are similar, but the Story list omits artwork and other presentation behavior already present in the Post list.

## Backend Design

### Shared playback hydration

Add a `StoryPlaybackHydrator` in the Post Story service package. It accepts either one `UserStories` value or a list and returns playback projections without mutating persistence entities.

For a list, the hydrator will:

1. Collect distinct, nonblank `musicId` values.
2. Load the corresponding Music rows as a batch.
3. Preserve the original Story order.
4. Resolve the audio URL by preferring a nonblank `user_stories.music_url`, then a nonblank `musics.song_url`.
5. Resolve the display name from the Music display name, single name, or slug name.
6. Calculate `durationSeconds` through the existing `StoryMusicSegmentPolicy`.

The Home tray, Story archive, and Highlight hydration will all use this component. This removes the three surfaces' ability to drift while avoiding one Music query per Story.

### Response contracts

Introduce explicit Story playback/archive response DTOs instead of returning `UserStories` entities from archive and Highlight APIs. The JSON remains compatible with the frontend's current Story archive contract and adds the playback fields that are currently missing:

- `musicName`
- `durationSeconds`

Existing fields such as `musicId`, `musicUrl`, `musicStart`, and `musicEnd` retain their names and types. Routes and request contracts do not change.

`StoryHighlightResponse.stories` will become a list of the shared archive response DTO. The archive page will use the same DTO.

### Media delivery

Story media URLs continue to use the existing Story delivery transformation. Music URLs are passed through according to the existing playback contract; this work does not add a fetch or upload operation.

## Frontend Playback Design

`StoryArchiveDto` will include the playback fields returned by the backend. The shared Story mapper will map music name and duration from both tray and archive inputs rather than treating them as tray-only fields.

Both entry points remain thin:

```text
Notification LIKE_STORY
  -> resolve Story destination
  -> archive API
  -> archivedStoryToItem
  -> StoryViewer
  -> useStoryPlayback

Profile Highlight
  -> Highlight API
  -> archivedStoryToItem
  -> StoryViewer
  -> useStoryPlayback
```

`StoryViewer` and `useStoryPlayback` remain the single playback implementation. They will receive `musicUrl`, `musicStart`, `musicEnd`, and `durationSeconds` instead of either entry point inventing playback logic.

## Shared Music Browser Design

Extract the track list currently embedded in `PostCreationStudio` into the shared music area:

```text
src/shared/music/
  MusicTrackBrowser.tsx
  MusicTrackBrowser.css
  MusicTrackBrowser.test.tsx
```

The shared browser receives Music DTOs and state/callback props. It owns the common presentation and interaction surface:

- artwork with fallback icon;
- track name, artist/category, and duration;
- popularity-ordered suggestions when the query is empty;
- search results;
- loading skeleton, empty state, and pagination;
- preview/pause;
- Fetch and Processing states;
- Select and selected states.

Post and Story retain their own parent state, selected-track editor, clip-segment state, and publication payloads. This prevents the extraction from coupling Post-wide music selection to per-Story music selection. Existing Music API, SSE events, and fetch commands remain unchanged.

## Fallback and Error Behavior

- A Story without `musicId` plays silently. An image Story uses the existing five-second fallback.
- A Story whose Music row is absent, unfetched, or lacks `songUrl` still opens normally and plays silently for the existing fallback duration.
- Opening a Story never initiates a Music fetch.
- A browser autoplay rejection keeps the Story open and follows the existing mute/user-interaction behavior.
- A failed archive or Highlight request keeps the existing unavailable-Story or toast behavior.
- Music browser fetch/search errors remain surface-owned and do not clear the selected Story/Post music.

## Testing

Backend tests will cover:

- resolving `musicId` to `musics.song_url`;
- preferring a persisted Story music URL;
- safe fallback for missing or unfetched Music;
- duration calculation from the selected segment;
- batch hydration preserving order;
- archive and Highlight responses exposing equivalent playback fields.

Frontend tests will cover:

- archive mapping preserving music URL, name, segment, and duration;
- notification-opened and Highlight-opened Stories reaching `StoryViewer` with playback data;
- shared browser artwork, metadata, suggested/search states, pagination, preview, Fetch/Processing, Select, and selected state;
- both Post and Story rendering the shared browser;
- existing Story playback behavior remaining valid.

Verification will include focused backend tests, focused frontend tests, frontend typecheck, frontend build, and backend package.

## Non-goals

- No database schema changes.
- No route or request-body changes.
- No automatic Music fetch during Story viewing.
- No changes to Music SSE or backend SpotiFLAC processing.
- No redesign of Story Viewer or either creation studio outside the shared track-list surface.
