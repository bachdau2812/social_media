package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.repository.AuditLogsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

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
    void recordBestEffortFillsDefaultsAndPersistsAuditEntry() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.record(new AuditEntry("user-1", AuditActionType.LOGIN,
                        "AUTH_SESSION", "user-1", "SUCCESS", "{}", null)))
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
    void recordRequiredInteractionPersistsStableIdentityAndMetadata() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        Instant occurredAt = Instant.parse("2026-10-01T12:30:00Z");

        StepVerifier.create(service.recordRequiredInteraction(new AuditEntry("user-1", AuditActionType.LIKE_POST,
                        "POST", "post-1", "SUCCESS",
                        "{\"postId\":\"post-1\",\"sourceId\":\"like-1\",\"occurredAt\":\"" + occurredAt + "\"}",
                        "event-1")))
                .verifyComplete();

        ArgumentCaptor<AuditLogs> captor = ArgumentCaptor.forClass(AuditLogs.class);
        verify(r2dbcEntityTemplate.insert(AuditLogs.class)).using(captor.capture());
        AuditLogs saved = captor.getValue();
        assertThat(saved.getActorId()).isEqualTo("user-1");
        assertThat(saved.getAction()).isEqualTo(AuditActionType.LIKE_POST);
        assertThat(saved.getResourceId()).isEqualTo("post-1");
        assertThat(saved.getSourceEventId()).isEqualTo("event-1");
        assertThat(saved.getMetadata()).contains("like-1", occurredAt.toString());
    }

    @Test
    void recordRequiredInteractionPropagatesPersistenceFailure() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        RuntimeException databaseError = new RuntimeException("database unavailable");
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenReturn(Mono.error(databaseError));

        StepVerifier.create(service.recordRequiredInteraction(new AuditEntry("user-1", AuditActionType.LIKE_POST,
                        "POST", "post-1", "SUCCESS", "{}", "event-1")))
                .expectErrorMatches(error -> error == databaseError)
                .verify();
    }

    @Test
    void recordBestEffortKeepsItsExistingFailurePolicy() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenReturn(Mono.error(new IllegalStateException("audit database unavailable")));

        StepVerifier.create(service.record(new AuditEntry("user-1", AuditActionType.LOGIN,
                        "AUTH_SESSION", "user-1", "SUCCESS", "{}", null)))
                .verifyComplete();
    }

    @Test
    void duplicateInteractionIsAcceptedOnlyWhenIdentityAndMetadataMatch() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        String metadata = "{\"postId\":\"post-1\",\"sourceId\":\"like-1\",\"occurredAt\":\"2026-10-01T12:30:00Z\"}";
        DuplicateKeyException duplicate = new DuplicateKeyException("duplicate event id");
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenReturn(Mono.error(duplicate));
        when(auditLogsRepository.findBySourceEventId("event-1")).thenReturn(Mono.just(AuditLogs.builder()
                .actorId("user-1").action(AuditActionType.LIKE_POST).resourceType("POST")
                .resourceId("post-1").status("SUCCESS").metadata(metadata).sourceEventId("event-1").build()));

        StepVerifier.create(service.recordRequiredInteraction(new AuditEntry("user-1", AuditActionType.LIKE_POST,
                        "POST", "post-1", "SUCCESS", metadata, "event-1")))
                .verifyComplete();

        when(auditLogsRepository.findBySourceEventId("event-2")).thenReturn(Mono.just(AuditLogs.builder()
                .actorId("someone-else").action(AuditActionType.LIKE_POST).resourceType("POST")
                .resourceId("post-1").status("SUCCESS").metadata(metadata).sourceEventId("event-2").build()));
        StepVerifier.create(service.recordRequiredInteraction(new AuditEntry("user-1", AuditActionType.LIKE_POST,
                        "POST", "post-1", "SUCCESS", metadata, "event-2")))
                .expectErrorMatches(error -> error == duplicate)
                .verify();
    }

    @Test
    void handleStorySuccessEventStoresStoryUploadAudit() {
        UserAuditService service = new UserAuditService(auditLogsRepository, r2dbcEntityTemplate);
        when(r2dbcEntityTemplate.insert(eq(AuditLogs.class)).using(any(AuditLogs.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        service.handleStorySuccessEvent("""
                {"userId":"user-1","storyId":"story-1","mediaId":"media-1","mediaType":"IMAGE","musicUrl":""}
                """);

        ArgumentCaptor<AuditLogs> captor = ArgumentCaptor.forClass(AuditLogs.class);
        verify(r2dbcEntityTemplate.insert(AuditLogs.class), timeout(1000)).using(captor.capture());
        AuditLogs saved = captor.getValue();
        assertThat(saved.getAction()).isEqualTo(AuditActionType.UPLOAD_STORY);
        assertThat(saved.getActorId()).isEqualTo("user-1");
        assertThat(saved.getResourceType()).isEqualTo("STORY");
        assertThat(saved.getResourceId()).isEqualTo("story-1");
        assertThat(saved.getMetadata()).contains("media-1");
    }
}
