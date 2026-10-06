package com.dauducbach.clone.modules.feed.repositoty;

import com.dauducbach.clone.modules.feed.entity.FeedInteractionProcessing;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface FeedInteractionProcessingRepository extends ReactiveCrudRepository<FeedInteractionProcessing, String> {
    Mono<FeedInteractionProcessing> findByOperationId(String operationId);
    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('PREPARED','SKIP_PREPARED','SHORT_APPLIED') ORDER BY canonical_offset")
    Flux<FeedInteractionProcessing> findPending(String userId);

    /** Includes expired snapshot cursor commits, excludes permanent SKIPPED records. */
    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('SHORT_APPLIED','COMPLETED') ORDER BY canonical_offset DESC LIMIT 1")
    Mono<FeedInteractionProcessing> findLatestApplied(String userId);

    /** Progress continuity includes permanent skips independently of vector application. */
    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('SHORT_APPLIED','COMPLETED','SKIPPED') ORDER BY canonical_offset DESC LIMIT 1")
    Mono<FeedInteractionProcessing> findLatestAcknowledged(String userId);

    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('SHORT_APPLIED','COMPLETED') ORDER BY canonical_offset")
    Flux<FeedInteractionProcessing> findApplied(String userId);
}
