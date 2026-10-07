package com.dauducbach.clone.modules.personalization.profile;

import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorRedisState;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.user.publicapi.UserDeletionCleanup;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.function.Supplier;

/** Retain an ES deletion fence even if SQL deletion succeeded on an earlier failed attempt. */
@Service
@RequiredArgsConstructor
public class UserVectorCleanupService implements UserDeletionCleanup {
    private final UserVectorStore store;
    private final UserVectorOperationService operations;
    private final UserVectorCoordinator coordinator;
    private final VectorRedisState redis;

    /** User supplies its persistence/cache actions; vector cleanup runs under the shared lease. */
    @Override
    public Mono<Void> deleteUser(String userId, Supplier<Mono<Void>> deleteUserRecord,
            Supplier<Mono<Void>> afterSqlDeletion) {
        return coordinator.withUserLock(userId, lease -> coordinator.requireOwner(lease)
                .then(Mono.defer(deleteUserRecord))
                .then(Mono.defer(afterSqlDeletion))
                .then(coordinator.requireOwner(lease))
                .then(Mono.defer(() -> store.retainDeletionTombstone(userId)))
                .then(operations.cancelForDeleted(lease))
                .then(redis.clearDeleted(lease)));
    }
}
