package com.dauducbach.clone.modules.audit.publicapi;

import reactor.core.publisher.Flux;

import java.time.Instant;

public interface InteractionHistoryQuery {
    Flux<InteractionHistoryEntry> findPostInteractionsBetween(Instant from, Instant to);
}
