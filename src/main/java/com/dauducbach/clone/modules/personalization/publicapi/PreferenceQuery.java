package com.dauducbach.clone.modules.personalization.publicapi;

import reactor.core.publisher.Mono;

/** Reads one coherent preference view without exposing leases or persistence details. */
public interface PreferenceQuery {
    Mono<PreferenceSnapshot> load(String userId);
}
