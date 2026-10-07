package com.dauducbach.clone.modules.personalization.publicapi;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/** Runs feed queue mutations under the same versioned preference lease used by vector updates. */
public interface PreferenceQueueConsistency {
    <T> Mono<T> withSnapshotLease(String userId, Function<PreferenceQueueLease, Mono<T>> work);

    Mono<PreferenceSnapshot> load(PreferenceQueueLease lease);

    Mono<Long> version(PreferenceQueueLease lease);
}
