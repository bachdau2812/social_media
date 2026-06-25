package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.repositoty.AuditLogsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserAuditServiceTest {
    @Mock
    AuditLogsRepository auditLogsRepository;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    R2dbcEntityTemplate r2dbcEntityTemplate;

    @Test
    void saveFillsDefaultsAndPersistsAuditLog() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.save(AuditLogs.builder()
                        .actorId("user-1")
                        .action(AuditActionType.LOGIN)
                        .resourceType("AUTH_SESSION")
                        .resourceId("user-1")
                        .build()))
                .verifyComplete();

        ArgumentCaptor<AuditLogs> captor = ArgumentCaptor.forClass(AuditLogs.class);
        verify(r2dbcEntityTemplate.insert(AuditLogs.class)).using(captor.capture());
        AuditLogs saved = captor.getValue();
        assertThat(saved.getId()).isNotBlank();
        assertThat(saved.getStatus()).isEqualTo("SUCCESS");
        assertThat(saved.getActorType()).isEqualTo("USER");
        assertThat(saved.getCreatedAt()).isNotNull();
    }

    @Test
    void handleLikeEventStoresCommentLikeAudit() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        service.handleLikeEvent("""
                {"actorId":"user-1","targetId":"comment-1","targetType":"COMMENT","targetOwnerId":"owner-1","postId":"post-1","parentCommentId":"","likeCount":3}
                """);

        ArgumentCaptor<AuditLogs> captor = ArgumentCaptor.forClass(AuditLogs.class);
        verify(r2dbcEntityTemplate.insert(AuditLogs.class), timeout(1000)).using(captor.capture());
        AuditLogs saved = captor.getValue();
        assertThat(saved.getAction()).isEqualTo(AuditActionType.LIKE_COMMENT);
        assertThat(saved.getActorId()).isEqualTo("user-1");
        assertThat(saved.getResourceType()).isEqualTo("COMMENT");
        assertThat(saved.getResourceId()).isEqualTo("comment-1");
        assertThat(saved.getMetadata()).contains("post-1");
    }
}
