package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.infrastructure.vector.UserVectorCoordinator;
import com.dauducbach.clone.infrastructure.vector.VectorRedisState;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.function.Supplier;

/** Retain an ES deletion fence even if SQL deletion succeeded on an earlier failed attempt. */
@Service
@RequiredArgsConstructor
public class UserVectorCleanupService {
    private final UserDetailsRepository users;
    private final UserVectorStore store;
    private final UserVectorOperationService operations;
    private final UserVectorCoordinator coordinator;
    private final VectorRedisState redis;

    public Mono<Void> deleteUser(String userId) {
        return deleteUser(userId, Mono::empty);
    }

    /** Domain cache maintenance runs after SQL success even when the following vector cleanup fails. */
    public Mono<Void> deleteUser(String userId, Supplier<Mono<Void>> afterSqlDeletion) {
        return coordinator.withUserLock(userId, lease -> coordinator.requireOwner(lease)
                .then(Mono.defer(() -> users.deleteById(userId)))
                .then(Mono.defer(afterSqlDeletion))
                .then(coordinator.requireOwner(lease))
                .then(Mono.defer(() -> store.retainDeletionTombstone(userId)))
                .then(operations.cancelForDeleted(lease))
                .then(redis.clearDeleted(lease)));
    }
}
