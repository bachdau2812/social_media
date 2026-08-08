# Music Fetch Observability Design

## Goal

Make every backend boundary of `POST /musics/{trackId}/fetch` observable so a missing or stalled fetch can be located without exposing credentials, Redis ownership tokens, raw CLI output, or temporary filesystem paths.

## Current finding

The frontend click path and existing frontend/backend behavior tests pass. The backend currently emits logs mainly for terminal failures and selected cleanup/heartbeat errors. It does not log request ingress, security rejection, catalog state, lock decision, queue admission, normal job start, successful processing stages, or accepted response. Consequently, an ordinary request can appear to do nothing even while it is being rejected before the controller or progressing normally.

## Architecture

### Request boundary

Add a narrowly scoped WebFlux filter for `POST /musics/{trackId}/fetch`. It logs:

- request arrival with method, sanitized track ID, origin presence, and trace context;
- terminal HTTP status and elapsed milliseconds;
- cancellation or pipeline error.

The filter must run early enough to observe requests rejected by Security or CORS and must not modify the request or response.

### Controller and application service

`MusicsController` logs authentication rejection and authenticated delegation. `MusicService` logs delegation result or error. These entries distinguish pre-controller rejection from application-layer failure.

### Coordinator stages

`SpotifyMusicFetchService` logs structured stage transitions for:

1. request validation and catalog lookup;
2. already-fetched versus unfetched decision;
3. waiter registration and terminal-cache reuse;
4. Redis lock acquired or busy;
5. queue accepted or rejected;
6. job started;
7. thumbnail lookup when required;
8. SpotiFLAC download started/completed;
9. ffprobe metadata started/completed;
10. Cloudinary upload started/completed;
11. database persistence completed;
12. success/failure SSE dispatch;
13. cleanup and job completion.

Existing error handling, Redis locking, queueing, upload, persistence, SSE payloads, and API contracts remain unchanged.

## Logging contract

- Use the existing pipe-delimited format, for example `|SpotifyMusicFetchService|job|stage=download|status=started|trackId={}`.
- Log stable identifiers and state only: `trackId`, response/fetch status, waiter count where available, HTTP status, and elapsed time.
- Never log cookies, authorization data, Redis lock tokens, raw SpotiFLAC/ffprobe output, Cloudinary secrets, SSE payload bodies, lyrics/metadata, or temporary paths.
- Expected state transitions are `INFO`; lock contention and recoverable best-effort problems are `WARN`; terminal failures are `ERROR`.

## Testing

- Add a focused filter test proving matching requests are observed without changing the downstream response and unrelated requests are ignored.
- Add source/behavior regression coverage for the controller/service/coordinator logging boundaries where practical.
- Run existing music fetch frontend tests to confirm both Post and Story surfaces remain unchanged.
- Run backend music controller/service/coordinator tests and a Maven package build.

## Files

- Create `src/main/java/com/dauducbach/clone/modules/media/configuration/MusicFetchRequestLoggingFilter.java`.
- Create its focused test under `src/test/java/com/dauducbach/clone/modules/media/configuration/`.
- Modify `MusicsController.java`, `MusicService.java`, and `SpotifyMusicFetchService.java`.
- Modify `MusicsControllerTest.java`, `MusicServiceTest.java`, and `SpotifyMusicFetchServiceTest.java` to cover the new logging boundaries without changing business contracts.
