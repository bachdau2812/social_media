package com.dauducbach.clone.modules.audit.entity;

import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

@Table("audit_logs")
public class AuditLogs {
    @Id
    String id;
    String actorId;

    @Builder.Default
    String actorType = "USER";

    AuditActionType action;

    String resourceType;

    String resourceId;

    @Builder.Default
    String status = "SUCCESS";

    String metadata;

    @Builder.Default
    Instant createdAt = Instant.now();
}
