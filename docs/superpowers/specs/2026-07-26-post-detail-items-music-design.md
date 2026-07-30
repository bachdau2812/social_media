# Post Detail Items And Music Design

## Goal

Return ordered `post_items` from the post detail API and render item captions and music correctly in the responsive post detail carousel.

## Backend Contract

`GET /posts/{postId}` returns a `PostDetailResponse` owned by the post module. It contains the post-level fields, optional shared music, and ordered item responses. Each item contains its `PostItem` fields, resolved media metadata, and optional transformed music playback metadata.

The query service reads `PostDetails`, then `PostItem` rows ordered by `orderNumber`, resolves each `mediaId`, and resolves music through `MusicService`. Music URLs are clipped with `CloudinaryMediaService.transformMusicUrl(start, end)`. Missing optional music does not fail the post; missing item media omits that broken item.

## Frontend Behavior

The detail modal fetches `/posts/{postId}` and merges the response with the feed author's engagement state. Media uses a white stage. The active item's caption appears as an animated monochrome thought bubble near the lower edge, clamps long text, and expands on hover or keyboard focus without being clipped.

Shared post music uses one audio timeline that continues while images change. It pauses while the active item is a video and resumes afterward. Per-item music uses an independent remembered playback position per item; switching away pauses it and switching back resumes from its saved position. Items without music remain silent. Closing detail stops playback.

## Error Handling

The modal keeps the feed data if detail hydration fails. Music playback failures are non-blocking because browser autoplay policy may reject playback before a user gesture. The media carousel and comments continue to work without audio.

## Verification

Add focused backend query-service coverage, compile the backend, and build the React TypeScript frontend.
