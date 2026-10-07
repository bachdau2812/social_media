package com.dauducbach.clone.modules.personalization.recovery;

import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease;
import com.dauducbach.clone.modules.personalization.interactions.PreferenceInteractionRecovery;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Coordinates recovery order for a user's persisted preference state under the caller's lease. */
@Service
@RequiredArgsConstructor
public class PreferenceRecoveryCoordinator {
    private final UserVectorOperationService journal;
    private final PreferenceInteractionRecovery shortTerm;

    public Mono<Void> reconcilePending(VectorLease lease) {
        return Mono.defer(() -> journal.reconcilePending(lease))
                // Reject a rolled-back cursor before replay is allowed to mutate its snapshot.
                .then(Mono.defer(() -> shortTerm.requireContinuity(lease)))
                .then(Mono.defer(() -> shortTerm.reconcilePending(lease)))
                // Verify the recovered cursor before any caller reads or writes a preference snapshot.
                .then(Mono.defer(() -> shortTerm.requireContinuity(lease)));
    }
}
