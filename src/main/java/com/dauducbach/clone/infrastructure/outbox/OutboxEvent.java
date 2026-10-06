package com.dauducbach.clone.infrastructure.outbox;

import java.time.Instant;

public record OutboxEvent(String eventId, String topic, String recordKey, String payload,
                          String aggregateId, long sequence, Instant createdAt) {}
