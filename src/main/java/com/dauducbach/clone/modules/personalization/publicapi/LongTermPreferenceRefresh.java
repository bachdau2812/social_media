package com.dauducbach.clone.modules.personalization.publicapi;

import reactor.core.publisher.Mono;

import java.time.Instant;

/** Triggers a bounded long-term preference projection refresh. */
public interface LongTermPreferenceRefresh {
    Mono<PreferenceRefreshResult> refreshLongTermVectors(String from, String to, String userId);

    record PreferenceRefreshResult(String userId, Instant from, Instant to, Instant refreshedAt, String status) { }
}
