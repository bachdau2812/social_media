package com.dauducbach.clone.modules.audit.repositoty;

import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.Instant;

@Repository
public interface AuditLogsRepository extends R2dbcRepository<AuditLogs, String> {
    @Query("""
            SELECT * FROM audit_logs
            WHERE status = 'SUCCESS'
              AND action IN ('LIKE_POST', 'COMMENT_POST')
              AND created_at >= :from
              AND created_at < :to
            ORDER BY actor_id ASC, created_at ASC
            """)
    Flux<AuditLogs> findPostInteractionsBetween(Instant from, Instant to);
}
