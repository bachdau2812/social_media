package com.dauducbach.clone.modules.personalization.publicapi;

/** Kafka transport position persisted for replay ordering and continuity checks. */
public record CanonicalInteractionPosition(String topic, String generation, int partition, long offset) {
    public CanonicalInteractionPosition {
        if (topic == null || topic.isBlank() || generation == null || generation.isBlank() || partition < 0 || offset < 0)
            throw new IllegalArgumentException("Invalid canonical position");
    }
}
