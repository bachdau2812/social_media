package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.repositoty.AuditLogsRepository;
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
public class AuditInteractionQueryService {
    private static final Logger log = LoggerFactory.getLogger(AuditInteractionQueryService.class);

    AuditLogsRepository auditLogsRepository;

    public Flux<AuditLogs> findPostInteractionsBetween(Instant from, Instant to) {
        if (from == null || to == null || !from.isBefore(to)) {
            return Flux.empty();
        }

        return auditLogsRepository.findPostInteractionsBetween(from, to)
                .doOnComplete(() -> log.info("|AuditInteractionQueryService|findPostInteractionsBetween|from={}|to={}",
                        from, to))
                .onErrorResume(error -> {
                    log.error("|AuditInteractionQueryService|findPostInteractionsBetween|failed|from={}|to={}|error={}",
                            from, to, error.getMessage());
                    return Flux.empty();
                });
    }
}
