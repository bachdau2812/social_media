package com.dauducbach.clone.modules.audit.publicapi;

import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable audit history projection consumed by personalization workflows. */
public record InteractionHistoryEntry(
        String auditId,
        String sourceEventId,
        String actorId,
        AuditActionType action,
        String resourceType,
        String resourceId,
        String status,
        Map<String, Object> metadata,
        Instant createdAt) {

    public InteractionHistoryEntry {
        metadata = metadata == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }
}
