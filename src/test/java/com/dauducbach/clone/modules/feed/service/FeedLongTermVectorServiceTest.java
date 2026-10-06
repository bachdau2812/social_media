package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.infrastructure.vector.InMemoryVectorRedisState;
import com.dauducbach.clone.infrastructure.vector.UserVectorCoordinator;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.AuditInteractionQueryService;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.modules.user.entity.UserDetailVector;
import com.dauducbach.clone.modules.user.entity.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.service.UserVectorOperationService;
import com.dauducbach.clone.modules.user.service.UserVectorQueryService;
import com.dauducbach.clone.modules.user.service.UserVectorStore;
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
class FeedLongTermVectorServiceTest {
    @Mock AuditInteractionQueryService auditInteractionQueryService;
    @Mock PostFeedQueryService postFeedQueryService;
    @Mock UserVectorQueryService userVectorQueryService;
    @Mock UserDetailsRepository users;
    @Mock UserVectorOperationService operations;
    @Mock FeedInteractionProcessingService shortTermProcessing;

    @Test
    void refreshLongTermVectorsReturnsNoSignalForAnEmptyRange() {
        FeedLongTermVectorService service = newService();
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
        FeedLongTermVectorService service = newService();
        Instant from = Instant.parse("2026-06-21T00:00:00Z");
        Instant to = Instant.parse("2026-06-22T00:00:00Z");
        AuditLogs target = audit("user-1", "post-1");
        AuditLogs other = audit("user-2", "post-2");
        when(auditInteractionQueryService.findPostInteractionsBetween(from, to)).thenReturn(Flux.just(target, other));
        when(users.existsById("user-1")).thenReturn(Mono.just(true));
        when(operations.findByOperationKey(anyString())).thenReturn(Mono.empty());
        when(operations.reconcilePending(any())).thenReturn(Mono.empty());
        when(shortTermProcessing.reconcilePending(any())).thenReturn(Mono.empty());
        when(shortTermProcessing.requireContinuity(any())).thenReturn(Mono.empty());
        when(postFeedQueryService.getPostRecommendationVector("post-1")).thenReturn(Mono.just(basis(0)));
        when(userVectorQueryService.getSnapshot("user-1")).thenReturn(Mono.just(new UserDetailVector()));
        when(operations.applyLongTerm(any(), anyString(), any(), any(), anyString(), any(), any()))
                .thenReturn(Mono.just(new UserVectorUpdateOperation()));

        StepVerifier.create(service.refreshLongTermVectors(from.toString(), to.toString(), " user-1 "))
                .assertNext(response -> {
                    assertThat(response.userId()).isEqualTo("user-1");
                    assertThat(response.status()).isEqualTo("COMPLETED");
                })
                .verifyComplete();

        verify(postFeedQueryService).getPostRecommendationVector("post-1");
        verify(postFeedQueryService, never()).getPostRecommendationVector("post-2");
        ArgumentCaptor<List<Double>> desired = ArgumentCaptor.forClass(List.class);
        verify(operations).applyLongTerm(any(), anyString(), any(), desired.capture(), anyString(), any(), any());
        assertThat(desired.getValue()).isEqualTo(basis(0));
    }

    private FeedLongTermVectorService newService() {
        UserVectorCoordinator coordinator = new UserVectorCoordinator(new InMemoryVectorRedisState());
        return new FeedLongTermVectorService(auditInteractionQueryService, postFeedQueryService, userVectorQueryService,
                users, coordinator, operations, shortTermProcessing, new FeedInteractionWeightPolicy(1.0, 0.4));
    }

    private AuditLogs audit(String actorId, String postId) {
        return AuditLogs.builder().actorId(actorId).action(AuditActionType.LIKE_POST).resourceId(postId)
                .status("SUCCESS").build();
    }

    private List<Double> basis(int coordinate) {
        java.util.ArrayList<Double> vector = new java.util.ArrayList<>(java.util.Collections.nCopies(768, 0.0));
        vector.set(coordinate, 1.0);
        return vector;
    }
}
