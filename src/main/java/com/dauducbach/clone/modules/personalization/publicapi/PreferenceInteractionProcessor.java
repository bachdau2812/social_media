package com.dauducbach.clone.modules.personalization.publicapi;

import reactor.core.publisher.Mono;

/** Application boundary for applying and recovering interaction-derived preference state. */
public interface PreferenceInteractionProcessor {
    Mono<Void> apply(PreferenceInteraction interaction, CanonicalInteractionPosition position);

}
