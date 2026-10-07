package com.dauducbach.clone.modules.personalization.infrastructure.persistence;

import com.dauducbach.clone.modules.personalization.model.PreferenceInteractionProcessing;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PreferenceInteractionProcessingRepository extends ReactiveCrudRepository<PreferenceInteractionProcessing, String> {
    Mono<PreferenceInteractionProcessing> findByOperationId(String operationId);
    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('PREPARED','SKIP_PREPARED','SHORT_APPLIED') ORDER BY canonical_offset")
    Flux<PreferenceInteractionProcessing> findPending(String userId);

    /** Includes expired snapshot cursor commits, excludes permanent SKIPPED records. */
    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('SHORT_APPLIED','COMPLETED') ORDER BY canonical_offset DESC LIMIT 1")
    Mono<PreferenceInteractionProcessing> findLatestApplied(String userId);

    /** Progress continuity includes permanent skips independently of vector application. */
    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('SHORT_APPLIED','COMPLETED','SKIPPED') ORDER BY canonical_offset DESC LIMIT 1")
    Mono<PreferenceInteractionProcessing> findLatestAcknowledged(String userId);

    @Query("SELECT * FROM feed_interaction_processing WHERE user_id = :userId AND status IN ('SHORT_APPLIED','COMPLETED') ORDER BY canonical_offset")
    Flux<PreferenceInteractionProcessing> findApplied(String userId);
}
