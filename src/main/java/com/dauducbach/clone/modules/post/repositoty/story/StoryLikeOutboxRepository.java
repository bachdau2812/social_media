package com.dauducbach.clone.modules.post.repositoty.story;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.entity.story.StoryLikeOutboxEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Objects;

@Repository
@Slf4j
@RequiredArgsConstructor
public class StoryLikeOutboxRepository {
    private final DatabaseClient databaseClient;

    public Mono<Integer> enqueue(
            String interactionId,
            String storyId,
            String actorId,
            String ownerId,
            Instant createdAt
    ) {
        return databaseClient.sql("""
                        INSERT INTO story_like_outbox (
                            interaction_id, story_id, actor_id, owner_id,
                            created_at, attempt_count, next_attempt_at
                        ) VALUES (
                            :interactionId, :storyId, :actorId, :ownerId,
                            :createdAt, 0, CURRENT_TIMESTAMP(6)
                        )
                        ON DUPLICATE KEY UPDATE interaction_id = interaction_id
                        """)
                .bind("interactionId", interactionId)
                .bind("storyId", storyId)
                .bind("actorId", actorId)
                .bind("ownerId", ownerId)
                .bind("createdAt", createdAt)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue);
    }

    public Mono<StoryLikeOutboxEntry> enqueueRequired(
            String interactionId,
            String storyId,
            String actorId,
            String ownerId,
            Instant createdAt
    ) {
        return enqueue(interactionId, storyId, actorId, ownerId, createdAt)
                .then(findByInteractionId(interactionId))
                .filter(entry -> matchesIntent(entry, interactionId, storyId, actorId, ownerId))
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.LIKE_CREATE_FAILED,
                        "Story Like notification intent could not be persisted")))
                .doOnNext(entry -> log.info(
                        "|StoryLikeOutboxRepository|enqueue|verified|storyId={}|actorId={}|ownerId={}|interactionId={}",
                        storyId, actorId, ownerId, interactionId));
    }

    private Mono<StoryLikeOutboxEntry> findByInteractionId(String interactionId) {
        return databaseClient.sql("""
                        SELECT interaction_id, story_id, actor_id, owner_id,
                               created_at, attempt_count, next_attempt_at,
                               lease_token, lease_until
                        FROM story_like_outbox
                        WHERE interaction_id = :interactionId
                        """)
                .bind("interactionId", interactionId)
                .map((row, metadata) -> StoryLikeOutboxEntry.builder()
                        .interactionId(row.get("interaction_id", String.class))
                        .storyId(row.get("story_id", String.class))
                        .actorId(row.get("actor_id", String.class))
                        .ownerId(row.get("owner_id", String.class))
                        .createdAt(row.get("created_at", Instant.class))
                        .attemptCount(valueOrZero(row.get("attempt_count", Integer.class)))
                        .nextAttemptAt(row.get("next_attempt_at", Instant.class))
                        .leaseToken(row.get("lease_token", String.class))
                        .leaseUntil(row.get("lease_until", Instant.class))
                        .build())
                .one();
    }

    private boolean matchesIntent(
            StoryLikeOutboxEntry entry,
            String interactionId,
            String storyId,
            String actorId,
            String ownerId
    ) {
        return Objects.equals(interactionId, entry.getInteractionId())
                && Objects.equals(storyId, entry.getStoryId())
                && Objects.equals(actorId, entry.getActorId())
                && Objects.equals(ownerId, entry.getOwnerId());
    }

    public Flux<StoryLikeOutboxEntry> leaseDue(String leaseToken, Instant leaseUntil, int batchSize) {
        Mono<Long> lease = databaseClient.sql("""
                        UPDATE story_like_outbox
                        SET lease_token = :leaseToken,
                            lease_until = :leaseUntil
                        WHERE interaction_id IN (
                            SELECT interaction_id
                            FROM (
                                SELECT interaction_id
                                FROM story_like_outbox
                                WHERE next_attempt_at <= CURRENT_TIMESTAMP(6)
                                  AND (lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP(6))
                                ORDER BY created_at, interaction_id
                                LIMIT :batchSize
                            ) due
                        )
                        """)
                .bind("leaseToken", leaseToken)
                .bind("leaseUntil", leaseUntil)
                .bind("batchSize", batchSize)
                .fetch()
                .rowsUpdated();

        return lease.thenMany(databaseClient.sql("""
                        SELECT interaction_id, story_id, actor_id, owner_id,
                               created_at, attempt_count, next_attempt_at,
                               lease_token, lease_until
                        FROM story_like_outbox
                        WHERE lease_token = :leaseToken
                        ORDER BY created_at, interaction_id
                        """)
                .bind("leaseToken", leaseToken)
                .map((row, metadata) -> StoryLikeOutboxEntry.builder()
                        .interactionId(row.get("interaction_id", String.class))
                        .storyId(row.get("story_id", String.class))
                        .actorId(row.get("actor_id", String.class))
                        .ownerId(row.get("owner_id", String.class))
                        .createdAt(row.get("created_at", Instant.class))
                        .attemptCount(valueOrZero(row.get("attempt_count", Integer.class)))
                        .nextAttemptAt(row.get("next_attempt_at", Instant.class))
                        .leaseToken(row.get("lease_token", String.class))
                        .leaseUntil(row.get("lease_until", Instant.class))
                        .build())
                .all());
    }

    public Mono<Integer> acknowledge(String interactionId, String leaseToken) {
        return databaseClient.sql("""
                        DELETE FROM story_like_outbox
                        WHERE interaction_id = :interactionId
                          AND lease_token = :leaseToken
                        """)
                .bind("interactionId", interactionId)
                .bind("leaseToken", leaseToken)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue);
    }

    public Mono<Integer> retry(String interactionId, String leaseToken, Instant nextAttemptAt) {
        return databaseClient.sql("""
                        UPDATE story_like_outbox
                        SET attempt_count = attempt_count + 1,
                            next_attempt_at = :nextAttemptAt,
                            lease_token = NULL,
                            lease_until = NULL
                        WHERE interaction_id = :interactionId
                          AND lease_token = :leaseToken
                        """)
                .bind("nextAttemptAt", nextAttemptAt)
                .bind("interactionId", interactionId)
                .bind("leaseToken", leaseToken)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue);
    }

    private static int valueOrZero(Integer value) {
        return value == null ? 0 : value;
    }
}
