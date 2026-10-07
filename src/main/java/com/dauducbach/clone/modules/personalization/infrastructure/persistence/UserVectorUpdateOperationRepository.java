package com.dauducbach.clone.modules.personalization.infrastructure.persistence;

import com.dauducbach.clone.modules.personalization.model.UserVectorUpdateOperation;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserVectorUpdateOperationRepository extends ReactiveCrudRepository<UserVectorUpdateOperation, Long> {
    Mono<UserVectorUpdateOperation> findByOperationKey(String operationKey);
    @Query("SELECT * FROM user_vector_update_operations WHERE user_id = :userId AND status IN ('PREPARED','ES_APPLIED') ORDER BY id")
    Flux<UserVectorUpdateOperation> findPending(String userId);
    // CONFLICTED reprepare reuses an old ID; captured ES sequence orders actual applied attempts.
    @Query("SELECT * FROM user_vector_update_operations WHERE user_id = :userId AND status = 'COMPLETED' AND operation_kind IN ('PROFILE','LONG_TERM') ORDER BY COALESCE(baseline_seq_no, -1) DESC LIMIT 1")
    Mono<UserVectorUpdateOperation> findLatestCompletedVector(String userId);
}
