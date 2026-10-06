# Post popularity API and rollout

## Required server preparation

Connections use the application's `spring.kafka`, `spring.data.redis`, and `spring.r2dbc` settings. Use forwarded ports; no local Docker is required. Environment overrides work with the `serv` profile too:

```text
SPRING_PROFILES_ACTIVE=serv
SPRING_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
SPRING_DATA_REDIS_HOST=127.0.0.1
SPRING_DATA_REDIS_PORT=6379
SPRING_R2DBC_URL=r2dbc:mysql://127.0.0.1:3306/ins_clone
```

Supply credentials through environment or existing private configuration. Do not add them to this file. Kafka must also advertise broker addresses reachable through the forwards; bootstrap reachability alone is insufficient.

1. Apply existing `vector_interaction_outbox_and_audit.sql` if not already installed, then `post_interaction_receipts.sql` to the application database. No migration is auto-applied by this feature.
2. Provision `post_interaction` with `retention.ms=604800000`; `like_event`/`comment_success_event` must retain replayable source contracts. Raw publishers now carry stable LIKE:/COMMENT: IDs. New comments also carry `popularityOccurredAt` without changing vector `occurredAt`.
3. Provision `post_popularity_updates` with `cleanup.policy=compact`; provision `post_popularity_updates.DLT` and `post_popularity_invalid`. Use suitable partition count/replication/min.insync.replicas for the server. DLT partitions must cover output partitions. EOSv2 needs transaction support on the broker.
4. Configure `post.popularity.partitions` before initial deployment, then keep it stable. The topology derives postId from payloads, including actor-keyed existing sources.
5. Enable shared `vector.outbox.enabled=true` on the dispatcher deployment. The API uses the existing outbox; there is no second relay.

## Enable order and rollback

All new flags default false. Enable separately:

```text
POST_POPULARITY_INGESTION_ENABLED=true
POST_POPULARITY_STREAMS_ENABLED=true
POST_POPULARITY_PROJECTION_ENABLED=true
POST_POPULARITY_MIXED_FEED_ENABLED=true
POST_POPULARITY_CURSOR_SECRET=<environment secret at least32characters>
POST_POPULARITY_APPLICATION_ID=<environment>-post-popularity-v1
```

Order: schema/topics/outbox -> ingestion -> shadow Streams -> projection -> cursor-ready mixed feed. For shadow use alternate `post.popularity.output-topic`, `invalid-topic`, `redis-key`, projection-group-id and application-id; all worker replicas in one deployment share the application-id. Output listener subscribes the configured output topic.

Keep Kafka Streams state.dir on stable worker storage via `spring.kafka.streams.properties.state.dir`; monitor restore/disk/lag. Changes to window, topic partitions, threshold/contribution semantics or state schema require controlled algorithm/state migration. A new historical replay may interleave source topics differently and drop late windows differently; do not claim it reproduces all old promotion timestamps. Historical rebuild uses a shadow namespace, accepted event-time and sufficient backfill grace/retention; never replace expiry with rebuild-now.

Rollback by disabling mixed-feed to restore the old feed. Disable ingestion/Streams/projection separately if needed; keep outbox and source data for recovery. Do not remove the shared vector outbox tables or unrelated ledger data.

## API contract

```http
POST /posts/interaction
Content-Type: application/json

{
  "postId": "<approved post ID>",
  "isClick": true,
  "viewTime": 45,
  "eventId": "0d6355c6-c969-4b6c-9f6c-bdbb521188ae",
  "impressionId": "ccf976e2-21e5-4ead-b19e-a0f7d79e2c76"
}
```

Authenticated actor only. viewTime is integer foreground-visible seconds0..3600; isClick is JSON boolean. Click+1, dwell≤30:+0,30<t≤60:+1,t>60:+2. Unknown actorId/score fields have no authority.

- HTTP202 after durable receipt/outbox commit, result eventId/computedScore/duplicate. Same eventId/body returns original receipt, no new score. Changed body for same actor/eventId409. Disabled ingestion503. Invalid body400; noneligible post404.
- eventId identifies one report; reuse it only for retry. impressionId identifies one display episode; subsequent cumulative reports use a new eventId and the same impressionId. Maximum3 contribution/impression; click OR + max dwell are merged, so click-only then dwell-only keeps both facts. Report span≤2h by accepted event-time.
- Retry contract8d. Cleanup receipts in batches1000/hour; Kafka raw topic7d. Published shared outbox/ledger cleanup is outside this feature. New impression IDs may create new legitimate scores; v1 does not introduce a per-actor/post cooldown or claim click telemetry cannot be forged.

## Popularity and feed

Hopping5h/hop5m/grace10m; strict score>N. DefaultN20 is experimental and needs traffic calibration. Like targetPOST+1; approved comment/reply+1. Unlike/delete does not subtract positive activity; feed eligibility remains the authority for post deletion/archive.

One qualification per active48h period. Redis member postId,score=popularSince; same-score tie reverse byte-lex postId. Reads filter expiry even when no new events arrive; scheduler removes expired members. New activity does not refresh an active period, and each overlapping window does not renew it.

Late activity predating the previous expiry cannot initiate a new period: the current triggering contribution must also follow that expiry. Impression facts retain both accepted time extremes, so reverse-order reports cannot bypass the2h span. Period replay fences remain at least8d and are removed in batches of1000 on one-minute stream-time punctuation; idle stores are cleaned when stream-time advances again. Size the cleanup capacity against traffic and monitor state/changelog disk usage.

Projection waits up to30s solely on its dedicated Kafka consumer thread. The container retries5times after the first failure and sends unrecoverable records to the explicit `<output-topic>.DLT` destination. DLT publishing requires broker acknowledgement; failed DLT publishing leaves the source record retryable. Provision and monitor this topic and replay repaired records when necessary. This wait never runs on a WebFlux request thread.

Main feed defaults20/max50, friendQuota=ceil(limit/2),popularQuota=floor(limit/2). Sources are mutual-friend original posts and global popular IDs. Up to10+10, no borrowing; interleave friend/popular and append any remaining source tail. Dedup/seen/eligibility filtering precedes quota. A popular source error returns available friend quota and retains popular anchor for retry; viewer read does not ZREM the shared ZSET.

`GET /feed` and `GET /home?tab=DISCOVER` accept optional cursor; result adds nextCursor. `/home?tab=FRIENDS` retains page and repost activity behavior. Media display type, actor and algorithm are bound to the signed cursor; expired409,invalid400. Cursor session20m, explicit refresh without cursor starts new session. Client must pass nextCursor for load-more and must not assume an underfilled/empty page means exhausted if hasMore=true. Batch40/scan400 bounds each source; source anchors progress through rejected candidates. Retry response may differ because seen/eligibility are live.

## Verification via forwarded services

Unit/topology tests need no server. Real integration tests are opt-in, no Testcontainers/Docker dependency:

```text
POPULARITY_TEST_KAFKA_BOOTSTRAP=127.0.0.1:9092
POPULARITY_TEST_REDIS_HOST=127.0.0.1
POPULARITY_TEST_REDIS_PORT=6379
POPULARITY_TEST_REDIS_USERNAME=<optional ACL user>
POPULARITY_TEST_REDIS_PASSWORD=<private environment>
POPULARITY_TEST_MYSQL_HOST=127.0.0.1
POPULARITY_TEST_MYSQL_PORT=3306
POPULARITY_TEST_MYSQL_USER=<test-capable user>
POPULARITY_TEST_MYSQL_PASSWORD=<private environment>
```

Kafka test creates/deletes only `test-popularity-<UUID>-*` topics/internal state; Redis touches/deletes one UUID-prefixed key; MySQL test creates/drops one `test_popularity_<UUID>` database and needs CREATE/DROP privileges. Application database/tables/normal topic data are never modified by these tests.

Run `.\mvnw.cmd test` and `.\mvnw.cmd clean package`. Broker test verifies committed output/restart/dedup; real MySQL test verifies concurrent receipts and rollback; Redis test runs actual Lua expiry/tie/cursor checks. Look at outbox age, stream lag, Kafka late-drop metrics, quarantine/DLT, Redis latency/cardinality and feed underfill/scan exhaustion before rollout.

For focused feature verification while existing vector tests have API/constructor drift, run `.\scripts\test-post-popularity.ps1`. It derives a temporary verification POM from the actual project and uses the Maven wrapper, without changing the main POM's test selection. The optional `-Tests` argument selects individual included classes. Full-suite failures must still be resolved before release; a focused pass does not certify the entire application. Integration variables above enable the same checks through server forwards.
