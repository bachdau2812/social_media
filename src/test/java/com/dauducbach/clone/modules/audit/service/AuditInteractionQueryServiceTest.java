package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryEntry;
import com.dauducbach.clone.modules.audit.repository.AuditLogsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuditInteractionQueryServiceTest {
    @Mock
    AuditLogsRepository repository;

    @Test
    void returnsAnImmutableHistoryProjectionRatherThanAuditEntity() {
        Instant from = Instant.parse("2026-10-01T00:00:00Z");
        Instant to = from.plusSeconds(3600);
        AuditLogs audit = AuditLogs.builder().id("audit-1").sourceEventId("event-1").actorId("user-1")
                .action(AuditActionType.LIKE_POST).resourceType("POST").resourceId("post-1")
                .status("SUCCESS").metadata("{\"postId\":\"post-1\",\"count\":2}")
                .createdAt(from.plusSeconds(30)).build();
        when(repository.findPostInteractionsBetween(from, to)).thenReturn(Flux.just(audit));

        StepVerifier.create(new AuditInteractionQueryService(repository).findPostInteractionsBetween(from, to))
                .assertNext(entry -> {
                    assertThat(entry).isInstanceOf(InteractionHistoryEntry.class);
                    assertThat(entry.auditId()).isEqualTo("audit-1");
                    assertThat(entry.sourceEventId()).isEqualTo("event-1");
                    assertThat(entry.actorId()).isEqualTo("user-1");
                    assertThat(entry.action()).isEqualTo(AuditActionType.LIKE_POST);
                    assertThat(entry.resourceId()).isEqualTo("post-1");
                    assertThat(entry.metadata()).containsEntry("postId", "post-1").containsEntry("count", 2.0d);
                    assertThat(entry.createdAt()).isEqualTo(audit.getCreatedAt());
                    assertThatThrownBy(() -> entry.metadata().put("another", "value"))
                            .isInstanceOf(UnsupportedOperationException.class);
                })
                .verifyComplete();
    }

    @Test
    void rejectsAnInvalidRangeBeforeQueryingPersistence() {
        Instant value = Instant.parse("2026-10-01T00:00:00Z");

        StepVerifier.create(new AuditInteractionQueryService(repository).findPostInteractionsBetween(value, value))
                .expectError(IllegalArgumentException.class)
                .verify();
    }
}
