package com.dauducbach.clone.modules.personalization.interactions;

import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease;
import reactor.core.publisher.Mono;

/** Internal lease-scoped recovery capability used by personalization orchestration. */
public interface PreferenceInteractionRecovery {
    Mono<Void> reconcilePending(VectorLease lease);

    Mono<Void> requireContinuity(VectorLease lease);
}
