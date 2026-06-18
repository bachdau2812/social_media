# Business Notes

> Rule for future work: read this file before answering or changing code in this project.

## Project Overview

This is a reactive social media backend built with Spring Boot 3.5, Java 21, WebFlux, Spring Security, R2DBC MySQL, Redis Reactive, Elasticsearch, Kafka/Reactor Kafka, Cloudinary, Firebase, Mail, Gson, Jsoup, and Log4j2.

The app base path is `/app` and server port is `8888`. The main package is `com.dauducbach.clone`.

## Runtime And Config

- `application.yaml` configures MySQL R2DBC at `r2dbc:mysql://localhost:3306/ins_clone`, Redis at `localhost:6379`, Elasticsearch at `localhost:9200`, Kafka at `localhost:9092`, Gmail SMTP, OAuth2 providers, JWT duration, and Cloudinary.
- Most secrets are environment variables: `MYSQL_USERNAME`, `MYSQL_PASSWORD`, `REDIS_PASSWORD`, `ELASTICSEARCH_USERNAME`, `ELASTICSEARCH_PASSWORD`, OAuth client secrets, `JWT_SIGNER_KEY`, mail credentials, and Cloudinary secrets.
- `KafkaConfig` creates a `KafkaSender<String, String>` using String keys and JSON string values.
- `SecurityConfig` uses WebFlux security, JWT resource server, OAuth2 login for Google/Facebook/GitHub, cookie bearer token conversion, custom auth entry point returning `ApiResponse`, and CORS for local frontend ports `5173` and `5000`.
- `TraceIdFilter` and the global exception layer preserve trace id in responses.

## Shared Conventions

- API responses should use `ApiResponse<T>` with `code`, `message`, `traceId`, and `result`.
- Business errors should use `AppException` with an `ErrorCode`; do not return ad hoc error strings from controllers.
- `GlobalExceptionHandler` catches `AppException` and returns JSON with the enum code/message and HTTP status.
- Common entity types are `EntityType.USER`, `EntityType.POST`, and `EntityType.COMMENT`.
- Kafka JSON helpers live in `GsonUtils` and `KafkaUtils`; prefer them for Kafka string payload parsing or safe field extraction.
- Repository package is currently named `repositoty` across modules. Keep it consistent unless doing a dedicated rename refactor.

## Modules

### Auth

- Handles local credential registration/login, email verification, password reset, refresh tokens, logout, JWT verification, introspection, OAuth2 login success/failure, and social account loading.
- Public endpoints are under `/auth/**`, `/login/**`, and `/oauth2/**`.
- User provider constants live in `modules.auth.constant.UserProvider`.

### User

- Manages user profile details, phone, job, high school, university, social media, user vectors, and follower relationships.
- Follow/unfollow validates self-follow and duplicate relationships, writes to DB, and publishes Kafka events.
- Follower/following listing uses manual page/size/offset and dedicated response DTOs.
- Profile media operations live in `MediaForProfile`: avatar upload, story upload, profile music selection, current avatar, profile media history, and story history.
- Avatar and story uploads reuse the same media validation pattern as post/comment: accept request, publish scan event, process asynchronously, save media metadata after scan passes, emit SSE for success/failure, then publish success events for notifications.
- Story creation may include `musicUrl`, `musicStart`, and `musicEnd`; start/end are seconds, must be provided together, and `musicEnd` must be greater than `musicStart`.
- Profile media rows use `media.owner_id = userId` for `AVATAR`, `STORY`, and `FEATURE_MUSIC`. Existing post media keeps `media.owner_id = postId`; user-level post media listing must query through `post_details`.
- Profile music selection writes `user_music` and also records a `FEATURE_MUSIC` row in `media` so profile music history can be served from the media table.
- Music catalog APIs live under `/app/musics`: manual create, paginated list/search/filter, detail by id, and Jamendo import.
- User search lives under `/app/user-details/search` and returns paginated `userId` strings. DB search always matches `username`; optional `filter` is split by `+`, whitespace, or comma and only whitelisted fields may expand matching (`hobby`, `living_in`/`live_in`, `hometown`, `city`, `sex`).
- Jamendo import only accepts `api.jamendo.com` HTTP(S) URLs, parses the `results` array, skips existing track ids, uploads audio to Cloudinary, stores `Musics.songUrl` from Cloudinary `secure_url`, and records uploaded audio in `media` with `owner_type = MUSIC` and `owner_id = Jamendo track id`.
- Jamendo import should be resilient per track: one failed download/upload/save must increase `failedCount` without failing the whole batch.

### Cloudinary Media URL

- Cloudinary delivery URLs follow `https://res.cloudinary.com/<cloud_name>/<asset_type>/<delivery_type>/<transformations>/<version>/<public_id>.<ext>`.
- `CloudinaryUtils` should be used when appending transformations such as audio story segments (`so_<start>,du_<duration>`) so URL handling stays centralized for image, video, and audio media.

### Post

- Manages post creation/update/delete/list/detail, media upload/signature, Cloudinary metadata, image scan worker, vector indexing, comments, SSE broadcasts, and likes.
- Post content is sanitized with Jsoup before persistence.
- Post creation writes a pending record, stores wait-for-upload state in Redis, and publishes `check_media_event`.
- Media scan downloads are buffered by WebClient with configurable limit `post.media.scan.max-in-memory-size` and default `10MB`. Scan API URL is configured by `post.media.scan.api-url`.
- Multipart filename sent to scan API must be a safe basename, not the full Cloudinary `publicId`, because `publicId` may contain `/`.
- Comments support root replies and child replies, content validation, Redis count increments/decrements, media scan events, immediate success SSE/Kafka events for text comments.
- Likes support POST and COMMENT targets, duplicate prevention, count/status checks, paged liked target ids, and `like_event` publishing.
- Post like/comment counts are cache-aside counters in Redis. Read count from cache first; on cache miss or expired key, load from DB and set Redis before returning/updating.
- Post search lives under `/app/posts/search` and returns paginated `postId` strings. DB search matches `content` and `hashtag` for `APPROVED` posts only.

### Notification

- Supports notification settings, email notifications, push tokens, Firebase push, templates, and notification events.
- SMS is explicitly unsupported and should throw `NOTIFICATION_TYPE_NOT_SUPPORTED`.
- Default notification settings are permissive when no setting row exists.

### Infrastructure

- `UserAuditService` and `UserActivities` capture cross-module user activity/audit style data.
- Elasticsearch vector entities exist for posts and user details.
- Semantic search uses `GetVectorEmbedding` first, then Elasticsearch `script_score` cosine similarity with threshold `0.80`; Elasticsearch query scores add `+1.0`, so the minimum score is `1.80`. User semantic search reads `user_long_term_vector` in `user_detail_vector`; post semantic search reads `content_vector` in `post_vector`.

## Like Business Rules

- Valid target types are only `POST` and `COMMENT`.
- Like input comes from a controller as actor id plus target id/type. Current API passes `actorId` in the route and `targetId`, `targetType` in `LikeRequest`.
- Before liking, validate target type, ensure target exists in DB, ensure the actor has not already liked it, save the `likes` record, then publish `LikeEventPayload` to Kafka topic `like_event`.
- Unlike requires an existing like row; missing row throws `LIKE_NOT_FOUND`.
- Has-liked returns a boolean based on actor id, target id, and target type.
- Count likes for POST reads Redis cache first, then DB on cache miss. Count likes for COMMENT reads DB directly unless a dedicated comment-like cache is introduced.
- Like/unlike for POST must ensure the Redis count key exists before DB mutation, then increment/decrement the key after mutation. The ensure/update sequence must run under a Redis lock per post count key to avoid race conditions.
- Liked posts are returned as paginated target id strings using `PageResponse<T>`.

## Comment Count Business Rules

- `countCommentsByPostId` reads Redis cache first and falls back to DB only when the cache key is missing or expired.
- Creating/deleting a comment must ensure the post comment count cache exists before DB mutation, then increment/decrement the cache after mutation.
- Cache initialization and count updates must run under a Redis lock per post count key to avoid concurrent requests overwriting each other's counts.

# Backend Development Standards

## Architecture Rules

* Follow Clean Architecture and Domain-Oriented Design principles.
* Preserve existing module boundaries:

    * auth
    * user
    * post
    * notification
    * commons
    * configuration
    * infrastructure
    * utils
* No module may directly access another module's repository.
* Cross-module communication must happen through services, events, or APIs.
* Keep controllers thin.
* Keep services focused on business use cases.
* Keep repositories focused on data access only.
* Avoid business logic in controllers, repositories, entities, filters, and interceptors.
* Prefer composition over inheritance.
* Prefer explicit code over abstractions.
* Prefer maintainability over cleverness.
* Prefer modifying existing patterns over introducing new architectural styles.

---

## Controller Rules

* Controllers only:

    * Parse requests.
    * Validate request format.
    * Call services.
    * Return ApiResponse.
* Never place business validation inside controllers.
* Never access repositories directly from controllers.
* Keep controller methods small and focused.
* One endpoint should represent one business use case.
* Delegate all business decisions to services.

---

## Service Rules

* Services own business logic.
* Services should expose use-case-oriented methods.
* Avoid CRUD-style pass-through services.
* Validate business constraints inside services.
* Normalize enum-like string values at service boundaries.
* Use AppException and ErrorCode for predictable failures.
* Map infrastructure failures to module-specific ErrorCodes.
* Never swallow exceptions unless explicitly defined as best-effort behavior.
* Keep service methods focused on a single business operation.
* Extract large flows into private helper methods.
* Validate ownership and permissions before mutations.
* Make retryable operations explicitly idempotent.

---
### CODE BASE RULE

## Reactive Programming Rules
* Use Mono and Flux end-to-end.
* Never call block().
* Never call subscribe() inside services.
* Never use toIterable().
* Never introduce blocking JPA repositories.
* Never call blocking APIs inside reactive chains.
* Use map, flatMap, switchIfEmpty, then, and onErrorMap intentionally.
* Keep reactive chains readable.
* Extract complex chains into named methods.
* Support backpressure where appropriate.
* Never expose unbounded Flux APIs without pagination or streaming strategy.

---

## DTO Rules
* Use immutable records by default.
* Use Lombok classes only when builders or mutability are necessary.
* Separate:
    * Request DTOs
    * Response DTOs
    * Event DTOs
    * Persistence Models
* Never expose entities directly through APIs.
* Keep DTOs transport-focused.

---

## Entity Rules
* Entities represent persistence state.
* Avoid embedding business workflows inside entities.
* Use optimistic locking for concurrent updates.
* Keep entity relationships explicit.
* Avoid unnecessary lazy-loading issues.
* Keep persistence concerns isolated.

---

## Repository Rules
* Repositories only perform data access.
* Repositories must not contain business logic.
* Repository methods should be use-case-oriented.
* Validate pagination before querying.
* Use deterministic sorting.
* Add indexes for frequently queried fields.
* Avoid N+1 query patterns.
* Custom queries require integration tests.

---

## Validation Rules
* Validate:

    * Required fields
    * Page size
    * Ownership
    * Business invariants
* Reject invalid input early.
* Fail fast.
* Never trust client input.

---

## Error Handling Rules
* Use AppException and ErrorCode for all predictable failures.
* Log unexpected failures.
* Return safe messages to clients.
* Never expose:
    * SQL errors
    * Stack traces
    * Internal implementation details
    * Secrets
* Keep error responses consistent.

---

## Security Rules

* Never log:
    * Passwords
    * Tokens
    * OTPs
    * Secrets
    * Authorization headers
* Apply least-privilege principles.
* Validate authorization before protected actions.
* Sanitize user-controlled input before logging.
* Treat all external input as untrusted.

---

## Kafka Rules
* Keep topic names as constants.
* Events must be immutable.
* Version event schemas when introducing breaking changes.
* Publish events only after successful business operations.
* Consumers must tolerate duplicate messages.
* Design consumers to be idempotent.
* Never silently ignore publishing or consumption failures.
* Add contract tests for shared event payloads.

---

## Cache Rules
* Centralize cache keys.
* Namespace cache keys by module.
* Define TTL explicitly.
* Never use infinite cache without justification.
* Invalidate cache after mutations.
* Test cache invalidation behavior.
* Never treat cache as the source of truth.

---
## Configuration Rules

* Prefer @ConfigurationProperties over scattered property lookups.
* Use strongly typed configuration.
* Fail startup when critical configuration is missing.
* Separate environment-specific configuration.
* Keep configuration centralized.

---

## Logging Rules
* Use structured logging.
* Include:

    * Request ID
    * Correlation ID
    * User ID (when safe)
* Log:

    * Business failures
    * Infrastructure failures
    * Important state transitions
* Avoid noisy logs.
* Logs should be actionable.

---

## Testing Rules
### General

* If it is not tested, it is not done.
* Every new feature requires tests.
* Every bug fix requires a regression test.
* Test behavior, not implementation.

### Unit Tests

* Cover:
    * Business validations
    * Service rules
    * ErrorCode mappings
    * Edge cases
* Use StepVerifier for reactive testing.
* Never test reactive flows using block().

### Controller Tests

* Verify:
    * Request validation
    * Response mapping
    * Error handling
    * Security behavior

### Repository Tests

* Verify:
    * Custom queries
    * Pagination
    * Sorting
    * Constraints
    * Index assumptions

### Kafka Tests

* Verify:
    * Event schema
    * Serialization
    * Topic routing
    * Error handling

### Cache Tests

* Verify:
    * Key generation
    * TTL
    * Invalidation
    * Cache hit/miss behavior

### Integration Tests

* Verify:
    * End-to-end business flows
    * Database interactions
    * Event publication
    * Security rules

### Test Quality

* Keep tests deterministic.
* Avoid random failures.
* Avoid timing dependencies.
* Use fixed clocks.
* Use builders and factories.
* Prefer focused tests over large scenario tests.

---

## Automated Testing Rules

* Generate tests together with implementation.
* Run all relevant tests before considering work complete.
* Fix failing tests before adding new functionality.
* Never disable tests to make builds pass.
* Every bug fix must include a regression test.
* Every public API must have happy-path and failure-path coverage.
* Maintain meaningful coverage for:

    * Services
    * Security
    * Event flows
    * Repository logic
* Do not optimize for coverage percentage alone.
* Prioritize business-critical paths.
---

## Code Quality Rules

* No dead code.
* No commented-out code.
* No unused imports.
* No magic strings.
* No magic numbers.
* Prefer constants and enums.
* Keep methods short and focused.
* Keep classes cohesive.
* Avoid premature optimization.
* Optimize only when measured.

---

## Documentation Rules

* Public APIs must be documented.
* Error responses must be documented.
* Breaking changes require migration notes.
* Significant architectural changes require ADRs.
* Keep documentation synchronized with implementation.

---

## AI Agent Rules

Before generating code:

1. Understand existing architecture.
2. Search for existing implementations first.
3. Reuse existing utilities before creating new ones.
4. Preserve consistency with surrounding code.
5. Generate tests alongside implementation.
6. Run relevant tests.
7. Fix failing tests before completion.
8. Never introduce blocking code into reactive flows.
9. Never bypass ErrorCode/AppException conventions.
10. Prefer the smallest change that solves the problem.
11. Do not refactor unrelated code unless explicitly requested.
12. Keep generated code aligned with existing project patterns.

## Logging Rules

- Use structured and consistent logging format:

  `log.info("|CommentService|getCommentById|commentId={}", commentId);`

- Log format should follow:

  `|<ClassName>|<MethodName>|<key1>=<value1>|<key2>=<value2>`

- Examples:

      log.info("|PostService|createPost|userId={}|title={}", userId, title);

      log.warn("|AuthService|login|email={}|reason=invalid_password", email);

      log.error("|NotificationService|sendNotification|userId={}|notificationId={}",
          userId,
          notificationId,
          ex
      );

- Add logs only for:
    - Business-critical operations.
    - State changes.
    - External service calls.
    - Kafka publish/consume operations.
    - Error and warning scenarios.

- Never log:
    - Passwords.
    - Tokens.
    - Authorization headers.
    - Secrets.
    - Full request/response payloads.

- Use `info` for successful business operations.
- Use `warn` for expected business failures.
- Use `error` for unexpected system failures.

- Include business identifiers (`userId`, `postId`, `commentId`, etc.) whenever available.

- Avoid meaningless logs such as:

      log.info("Start createPost");
      log.info("End createPost");
      log.info("Processing...");
---

## Comment Rules

- All code comments must be written in Vietnamese.
- Only add comments when they provide business or technical context that is not obvious from the code.
- Do not comment obvious code behavior.

---

## Final Principle

Prefer boring, explicit, testable, maintainable code over clever abstractions.
