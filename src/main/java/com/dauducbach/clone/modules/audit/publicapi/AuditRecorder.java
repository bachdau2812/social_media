package com.dauducbach.clone.modules.audit.publicapi;

import reactor.core.publisher.Mono;

public interface AuditRecorder {
    /** Records a non-critical audit fact; persistence failure is intentionally best-effort. */
    Mono<Void> record(AuditEntry entry);

    /** Records a durable, idempotent post interaction; persistence failure must reach the caller. */
    Mono<Void> recordRequiredInteraction(AuditEntry entry);
}
