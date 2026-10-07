package com.dauducbach.clone.modules.personalization.longterm;

import com.dauducbach.clone.modules.personalization.infrastructure.redis.InMemoryVectorRedisState;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryEntry;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryQuery;
import com.dauducbach.clone.modules.post.publicapi.PostFeedQuery;
import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.model.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import com.dauducbach.clone.modules.personalization.interactions.InteractionWeightPolicy;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LongTermPreferenceRefreshServiceTest {
    @Mock InteractionHistoryQuery auditInteractionQueryService;
    @Mock PostFeedQuery postFeedQueryService;
    @Mock UserVectorQueryService userVectorQueryService;
    @Mock UserExistenceQuery users;
    @Mock UserVectorOperationService operations;
    @Mock PreferenceRecoveryCoordinator recovery;

    @Test
    void scheduledRangeRefreshIsReturnedAsColdWork() {
        when(auditInteractionQueryService.findPostInteractionsBetween(any(), any())).thenReturn(Flux.empty());
        Mono<Void> scheduledWork = newService().updateYesterdayLongTermVectors();

        verify(auditInteractionQueryService, never()).findPostInteractionsBetween(any(), any());
        StepVerifier.create(scheduledWork).verifyComplete();
        verify(auditInteractionQueryService).findPostInteractionsBetween(any(), any());
    }

    @Test
    void refreshLongTermVectorsReturnsNoSignalForAnEmptyRange() {
        LongTermPreferenceRefreshService service = newService();
        Instant from = Instant.parse("2026-06-21T00:00:00Z");
        Instant to = Instant.parse("2026-06-22T00:00:00Z");
        when(auditInteractionQueryService.findPostInteractionsBetween(from, to)).thenReturn(Flux.empty());

        StepVerifier.create(service.refreshLongTermVectors(from.toString(), to.toString(), null))
                .assertNext(response -> {
                    assertThat(response.userId()).isNull();
                    assertThat(response.from()).isEqualTo(from);
                    assertThat(response.to()).isEqualTo(to);
                    assertThat(response.status()).isEqualTo("SKIPPED_NO_SIGNAL");
                    assertThat(response.refreshedAt()).isNotNull();
                })
                .verifyComplete();
        verify(auditInteractionQueryService).findPostInteractionsBetween(from, to);
    }

    @Test
    void refreshForUserUsesOnlyThatUsersSuccessfulPostInteractions() {
        LongTermPreferenceRefreshService service = newService();
        Instant from = Instant.parse("2026-06-21T00:00:00Z");
        Instant to = Instant.parse("2026-06-22T00:00:00Z");
        InteractionHistoryEntry target = audit("user-1", "post-1");
        InteractionHistoryEntry other = audit("user-2", "post-2");
        when(auditInteractionQueryService.findPostInteractionsBetween(from, to)).thenReturn(Flux.just(target, other));
        when(users.exists("user-1")).thenReturn(Mono.just(true));
        when(operations.findByOperationKey(anyString())).thenReturn(Mono.empty());
        when(recovery.reconcilePending(any())).thenReturn(Mono.empty());
        when(postFeedQueryService.getRecommendationVector("post-1")).thenReturn(Mono.just(basis(0)));
        when(userVectorQueryService.getSnapshot("user-1")).thenReturn(Mono.just(new UserDetailVector()));
        when(operations.applyLongTerm(any(), anyString(), any(), any(), anyString(), any(), any()))
                .thenReturn(Mono.just(new UserVectorUpdateOperation()));

        StepVerifier.create(service.refreshLongTermVectors(from.toString(), to.toString(), " user-1 "))
                .assertNext(response -> {
                    assertThat(response.userId()).isEqualTo("user-1");
                    assertThat(response.status()).isEqualTo("COMPLETED");
                })
                .verifyComplete();

        verify(postFeedQueryService).getRecommendationVector("post-1");
        verify(postFeedQueryService, never()).getRecommendationVector("post-2");
        ArgumentCaptor<List<Double>> desired = ArgumentCaptor.forClass(List.class);
        verify(operations).applyLongTerm(any(), anyString(), any(), desired.capture(), anyString(), any(), any());
        assertThat(desired.getValue()).isEqualTo(basis(0));
    }

    private LongTermPreferenceRefreshService newService() {
        UserVectorCoordinator coordinator = new UserVectorCoordinator(new InMemoryVectorRedisState());
        return new LongTermPreferenceRefreshService(auditInteractionQueryService, postFeedQueryService, userVectorQueryService,
                users, coordinator, recovery, operations, new InteractionWeightPolicy(1.0, 0.4));
    }

    private InteractionHistoryEntry audit(String actorId, String postId) {
        return new InteractionHistoryEntry("audit-" + postId, null, actorId, AuditActionType.LIKE_POST,
                "POST", postId, "SUCCESS", java.util.Map.of(), Instant.parse("2026-06-21T01:00:00Z"));
    }

    private List<Double> basis(int coordinate) {
        java.util.ArrayList<Double> vector = new java.util.ArrayList<>(java.util.Collections.nCopies(768, 0.0));
        vector.set(coordinate, 1.0);
        return vector;
    }
}
