package com.dauducbach.clone.modules.user.publicapi;

import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/** Allows a capability adapter to coordinate owned cleanup with user-record deletion. */
public interface UserDeletionCleanup {
    Mono<Void> deleteUser(String userId, Supplier<Mono<Void>> deleteUserRecord,
            Supplier<Mono<Void>> afterSqlDeletion);
}
