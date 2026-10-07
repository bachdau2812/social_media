package com.dauducbach.clone.modules.audit.publicapi;

import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;

/** Input contract for recording an audit fact without exposing the persistence entity. */
public record AuditEntry(
        String actorId,
        String actorType,
        AuditActionType action,
        String resourceType,
        String resourceId,
        String status,
        String metadataJson,
        String sourceEventId) {

    public AuditEntry(String actorId, AuditActionType action, String resourceType, String resourceId,
                      String status, String metadataJson, String sourceEventId) {
        this(actorId, null, action, resourceType, resourceId, status, metadataJson, sourceEventId);
    }
}
