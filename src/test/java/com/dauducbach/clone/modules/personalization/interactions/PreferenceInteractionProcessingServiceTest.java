package com.dauducbach.clone.modules.personalization.interactions;

import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.personalization.infrastructure.persistence.PreferenceInteractionProcessingRepository;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease;
import com.dauducbach.clone.modules.personalization.model.PreferenceInteractionProcessing;
import com.dauducbach.clone.modules.personalization.publicapi.CanonicalInteractionPosition;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteraction;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PreferenceInteractionProcessingServiceTest {
    private static final String EVENT_ID = "event-1";
    private static final String USER_ID = "user-1";

    @Test
    void reconcilesBeforeReloadingAndApplyingTheCanonicalEvent() {
        PreferenceInteractionProcessingRepository ledger = mock(PreferenceInteractionProcessingRepository.class);
        AuditRecorder audit = mock(AuditRecorder.class);
        ShortTermPreferenceVectorService vectors = mock(ShortTermPreferenceVectorService.class);
        UserVectorCoordinator coordinator = mock(UserVectorCoordinator.class);
        PreferenceRecoveryCoordinator recovery = mock(PreferenceRecoveryCoordinator.class);
        UserExistenceQuery users = mock(UserExistenceQuery.class);
        VectorLease lease = new VectorLease(USER_ID, "lease-token");

        PreferenceInteraction event = new PreferenceInteraction(
                EVENT_ID, USER_ID, "post-1", "LIKE", "source-1", Instant.parse("2026-10-07T04:00:00Z"));
        CanonicalInteractionPosition position = new CanonicalInteractionPosition("topic", "generation-1", 0, 7);
        PreferenceInteractionProcessing completed = completed(event, position);
        when(ledger.findById(EVENT_ID)).thenReturn(Mono.just(completed), Mono.just(completed));
        when(audit.recordRequiredInteraction(any())).thenReturn(Mono.empty());
        when(recovery.reconcilePending(lease)).thenReturn(Mono.empty());
        when(coordinator.withUserLock(eq(USER_ID), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Function<VectorLease, Mono<Void>> work = invocation.getArgument(1);
            return work.apply(lease);
        });

        PreferenceInteractionProcessingService service = new PreferenceInteractionProcessingService(
                ledger, audit, vectors, coordinator, recovery, users);

        StepVerifier.create(service.apply(event, position)).verifyComplete();

        InOrder order = inOrder(ledger, recovery);
        order.verify(ledger).findById(EVENT_ID);
        order.verify(recovery).reconcilePending(lease);
        order.verify(ledger).findById(EVENT_ID);
        verify(coordinator).withUserLock(eq(USER_ID), any());
    }

    private PreferenceInteractionProcessing completed(
            PreferenceInteraction event,
            CanonicalInteractionPosition position) {
        PreferenceInteractionProcessing row = new PreferenceInteractionProcessing();
        row.setSourceEventId(event.eventId());
        row.setUserId(event.userId());
        row.setPostId(event.postId());
        row.setAction(event.action());
        row.setSourceId(event.sourceId());
        row.setOccurredAt(event.occurredAt().truncatedTo(ChronoUnit.MICROS));
        row.setCanonicalTopic(position.topic());
        row.setCanonicalGeneration(position.generation());
        row.setCanonicalPartition(position.partition());
        row.setCanonicalOffset(position.offset());
        row.setStatus("COMPLETED");
        return row;
    }
}
