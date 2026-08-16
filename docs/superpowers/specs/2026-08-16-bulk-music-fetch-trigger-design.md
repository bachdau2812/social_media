# Bulk Music Fetch Trigger Design

## Goal

Add one authenticated bulk music-fetch API and one daily infrastructure scheduler. Both entry points use the same application service. The scheduler runs every day at 02:00 in `Asia/Ho_Chi_Minh` and requests the top 20 unfetched songs.

## Existing Context

The media module already supports fetching one Spotify track by its 22-character track ID. `SpotifyMusicFetchService` validates the catalog record, coordinates a Redis lock, schedules the download job, stores the fetched audio, and sends terminal SSE events to the requesting user. The existing `POST /app/musics/{trackId}/fetch` endpoint and its SSE behavior must remain unchanged.

The application already enables Spring scheduling through `@EnableScheduling`. Bulk selection and business rules belong to `modules/media`; only the scheduled adapter belongs to `infrastructure`.

## API Contract

Expose:

```http
POST /app/musics/fetch
Content-Type: application/json
```

The route uses the repository's existing global authentication requirement. It does not send SSE events.

Request body:

```json
{
  "type": "ARTIST",
  "fetchList": ["Son Tung M-TP", "Wxrdie"],
  "limit": 20
}
```

`type` is required and accepts:

- `ARTIST`: `fetchList` is a required, non-empty list of artist names. For each normalized artist, select at most `limit` unfetched songs by case-insensitive exact artist-name match, ordered by `popularity DESC`.
- `TOP`: `fetchList` is ignored. Select at most `limit` unfetched songs globally, ordered by `popularity DESC`.
- `SONG`: `fetchList` is a required, non-empty list of Spotify track IDs. Each ID must contain exactly 22 base-62 characters. `limit` is ignored.

For `ARTIST` and `TOP`, `limit` defaults to 20 and must be between 1 and 100 inclusive. Request list entries are trimmed, blank entries are removed, and duplicates are removed while retaining first-seen order. If normalization leaves an `ARTIST` or `SONG` list empty, the request is invalid.

The endpoint returns `202 Accepted` after attempting to enqueue every selected track. Its response includes the normalized request type, counts, and an ordered result per unique track. Per-track statuses are `STARTED`, `PROCESSING`, `ALREADY_FETCHED`, or `FAILED`. A per-track failure contains a safe message and does not terminate the rest of the batch.

Batch-level validation errors return the existing application error envelope with HTTP 400. These include a missing or unsupported type, a missing required list, an empty normalized required list, an invalid SONG track ID, and an out-of-range limit.

## Components and Boundaries

Add request/response DTOs and a fetch-type enum under the media module. Add a focused bulk orchestration service under `modules/media/service/music`. It owns validation, candidate selection, stable de-duplication, sequential enqueueing, per-track error isolation, and response summarization.

Extend `MusicsRepository` with focused reactive queries for:

- top unfetched songs ordered by popularity;
- unfetched songs for a case-insensitive exact artist match ordered by popularity.

The bulk service calls a silent entry point in `SpotifyMusicFetchService`. Silent fetch reuses the existing validation, catalog lookup, Redis lock, queue, download, persistence, cleanup, and concurrency guarantees, but it does not register an SSE waiter and does not publish immediate or terminal SSE events. The existing user-facing entry point continues to register and notify SSE waiters exactly as before.

`MusicsController` delegates the new route to the bulk orchestration service and wraps the result in `ApiResponse`. It contains no selection or fetch business rules.

Add a scheduler component under `com.dauducbach.clone.infrastructure`. Its scheduled method invokes the bulk service with `type=TOP`, no fetch list, and `limit=20` using cron `0 0 2 * * *` and zone `Asia/Ho_Chi_Minh`.

## Data Flow

1. The controller accepts and validates the request shape, then delegates to the bulk service.
2. The bulk service normalizes request fields and selects candidate track IDs from the repository according to the type.
3. Candidate IDs are de-duplicated in stable order. For ARTIST, each artist receives its own limit before cross-artist de-duplication.
4. The service invokes silent fetch sequentially for each track so it does not create an uncontrolled burst against the bounded fetch queue.
5. Each successful enqueue status or isolated failure is recorded in the ordered batch result.
6. The controller returns the summarized result with HTTP 202.

The scheduler enters the same flow at step 2 with a fixed TOP request. It subscribes to the reactive result, logs the status totals, and logs terminal errors so the Spring scheduling thread is not terminated.

## Error and Concurrency Behavior

Invalid batch input fails before repository selection or enqueueing. Repository selection failures fail the whole request because no reliable candidate set exists. Once candidate processing starts, an error for one track becomes `FAILED` for that track and processing continues.

The existing distributed lock remains the authority for duplicate concurrent jobs. A race between selection and enqueue can therefore result in `PROCESSING` or `ALREADY_FETCHED`, both of which are valid batch outcomes. Queue saturation is represented as a per-track `FAILED` result and does not bypass the configured bounded scheduler.

No raw downstream errors, credentials, filesystem paths, or internal service details are exposed in the API response. Detailed failures remain in server logs.

## Logging

Log one bulk start event with type and normalized request sizes, and one completion event with counts for every result status. Per-track failures log the track ID and exception type/message using the project's structured log style. The scheduler logs its start, summary, and terminal failure.

## Testing

Use JUnit 5, Mockito, Reactor Test, and focused source-contract tests where annotation metadata is the behavior under test.

Tests cover:

- request defaults and validation for all three types;
- repository query contracts for `fetched = false`, artist matching, popularity ordering, and limits;
- per-artist quota, TOP selection, SONG order, normalization, and stable de-duplication;
- sequential enqueueing and continuation after a per-track failure;
- batch response counts and safe failure messages;
- controller delegation and HTTP 202 response;
- silent fetch preserving the existing lock/job flow without any SSE publication;
- regression coverage for the existing single-track endpoint and SSE behavior;
- scheduler cron/zone metadata and invocation with TOP, no list, and limit 20.

Run focused media and infrastructure tests first, followed by the complete Maven test suite.

## Out of Scope

- Discovering new Spotify catalog songs or artists not already present in the `musics` table.
- Adding an admin role model or changing global endpoint authorization.
- Changing queue capacity, retry policies, download formats, or artifact storage.
- Changing the existing single-track fetch endpoint or its SSE contract.
