package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryEntry;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryQuery;
import com.dauducbach.clone.modules.audit.repository.AuditLogsRepository;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class AuditInteractionQueryService implements InteractionHistoryQuery {
    private static final Logger log = LoggerFactory.getLogger(AuditInteractionQueryService.class);

    AuditLogsRepository auditLogsRepository;

    @Override
    public Flux<InteractionHistoryEntry> findPostInteractionsBetween(Instant from, Instant to) {
        if (from == null || to == null || !from.isBefore(to)) {
            return Flux.error(new IllegalArgumentException("Audit range requires from before to"));
        }

        return auditLogsRepository.findPostInteractionsBetween(from, to)
                .map(this::toHistoryEntry)
                .doOnComplete(() -> log.info("|AuditInteractionQueryService|findPostInteractionsBetween|from={}|to={}",
                        from, to))
                .doOnError(error -> log.error("|AuditInteractionQueryService|findPostInteractionsBetween|failed|from={}|to={}",
                        from, to, error));
    }

    private InteractionHistoryEntry toHistoryEntry(AuditLogs audit) {
        java.util.Map<String, Object> metadata = GsonUtils.getGson().fromJson(
                GsonUtils.fromString(audit.getMetadata()), new TypeToken<java.util.Map<String, Object>>() { }.getType());
        return new InteractionHistoryEntry(audit.getId(), audit.getSourceEventId(), audit.getActorId(),
                audit.getAction(), audit.getResourceType(), audit.getResourceId(), audit.getStatus(), metadata,
                audit.getCreatedAt());
    }
}
