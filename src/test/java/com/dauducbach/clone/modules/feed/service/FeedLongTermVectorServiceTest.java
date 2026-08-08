package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.AuditInteractionQueryService;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import com.dauducbach.clone.modules.user.service.UserVectorQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
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
    @Mock
    AuditInteractionQueryService auditInteractionQueryService;
    @Mock
    PostFeedQueryService postFeedQueryService;
    @Mock
    UserVectorQueryService userVectorQueryService;
    @Mock
    UserDetailsService userDetailsService;
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    ReactiveValueOperations<String, String> valueOperations;

    @Test
    void updateLongTermVectorsForRangeBlendsAndSavesNormalizedVector() {
        FeedVectorService feedVectorService = new FeedVectorService(redisTemplate, postFeedQueryService, userVectorQueryService);
        FeedLongTermVectorService service = new FeedLongTermVectorService(
                auditInteractionQueryService,
                postFeedQueryService,
                userVectorQueryService,
                userDetailsService,
                feedVectorService,
                redisTemplate
        );
        Instant from = Instant.parse("2026-06-21T00:00:00Z");
        Instant to = Instant.parse("2026-06-22T00:00:00Z");
        AuditLogs log = AuditLogs.builder()
                .actorId("user-1")
                .action(AuditActionType.LIKE_POST)
                .resourceId("post-1")
                .status("SUCCESS")
                .build();

        when(auditInteractionQueryService.findPostInteractionsBetween(from, to)).thenReturn(Flux.just(log));
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("user_long_term_vector_snapshot:user-1")).thenReturn(Mono.empty());
        when(userVectorQueryService.getLongTermOrUserVector("user-1")).thenReturn(Mono.just(List.of(0.0, 1.0)));
        when(valueOperations.set(eq("user_long_term_vector_snapshot:user-1"), anyString(), any())).thenReturn(Mono.just(true));
        when(postFeedQueryService.getPostVector("post-1")).thenReturn(Mono.just(List.of(1.0, 0.0)));
        when(userVectorQueryService.saveLongTermVector(eq("user-1"), any())).thenReturn(Mono.empty());
        when(redisTemplate.delete("user_long_term_vector_snapshot:user-1")).thenReturn(Mono.just(1L));

        StepVerifier.create(service.updateLongTermVectorsForRange(from, to))
                .verifyComplete();

        ArgumentCaptor<List<Double>> vectorCaptor = ArgumentCaptor.forClass(List.class);
        verify(userVectorQueryService).saveLongTermVector(eq("user-1"), vectorCaptor.capture());
        assertThat(vectorLength(vectorCaptor.getValue())).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.000001));
        assertThat(vectorCaptor.getValue().get(1)).isGreaterThan(vectorCaptor.getValue().get(0));
    }

    @Test
    void refreshLongTermVectorsRunsCustomRangeAndReturnsCompletedResponse() {
        FeedVectorService feedVectorService = new FeedVectorService(redisTemplate, postFeedQueryService, userVectorQueryService);
        FeedLongTermVectorService service = new FeedLongTermVectorService(
                auditInteractionQueryService,
                postFeedQueryService,
                userVectorQueryService,
                userDetailsService,
                feedVectorService,
                redisTemplate
        );
        Instant from = Instant.parse("2026-06-21T00:00:00Z");
        Instant to = Instant.parse("2026-06-22T00:00:00Z");

        when(auditInteractionQueryService.findPostInteractionsBetween(from, to)).thenReturn(Flux.empty());

        StepVerifier.create(service.refreshLongTermVectors(from.toString(), to.toString(), null))
                .assertNext(response -> {
                    assertThat(response.userId()).isNull();
                    assertThat(response.from()).isEqualTo(from);
                    assertThat(response.to()).isEqualTo(to);
                    assertThat(response.status()).isEqualTo("COMPLETED");
                    assertThat(response.refreshedAt()).isNotNull();
                })
                .verifyComplete();

        verify(auditInteractionQueryService).findPostInteractionsBetween(from, to);
    }

    @Test
    void refreshLongTermVectorsWithUserIdOnlyUpdatesThatUser() {
        FeedVectorService feedVectorService = new FeedVectorService(redisTemplate, postFeedQueryService, userVectorQueryService);
        FeedLongTermVectorService service = new FeedLongTermVectorService(
                auditInteractionQueryService,
                postFeedQueryService,
                userVectorQueryService,
                userDetailsService,
                feedVectorService,
                redisTemplate
        );
        Instant from = Instant.parse("2026-06-21T00:00:00Z");
        Instant to = Instant.parse("2026-06-22T00:00:00Z");
        AuditLogs targetUserLog = AuditLogs.builder()
                .actorId("user-1")
                .action(AuditActionType.LIKE_POST)
                .resourceId("post-1")
                .status("SUCCESS")
                .build();
        AuditLogs otherUserLog = AuditLogs.builder()
                .actorId("user-2")
                .action(AuditActionType.LIKE_POST)
                .resourceId("post-2")
                .status("SUCCESS")
                .build();

        when(userDetailsService.getUserDetailsById("user-1")).thenReturn(Mono.just(UserDetails.builder().userId("user-1").build()));
        when(auditInteractionQueryService.findPostInteractionsBetween(from, to)).thenReturn(Flux.just(targetUserLog, otherUserLog));
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("user_long_term_vector_snapshot:user-1")).thenReturn(Mono.empty());
        when(userVectorQueryService.getLongTermOrUserVector("user-1")).thenReturn(Mono.just(List.of(0.0, 1.0)));
        when(valueOperations.set(eq("user_long_term_vector_snapshot:user-1"), anyString(), any())).thenReturn(Mono.just(true));
        when(postFeedQueryService.getPostVector("post-1")).thenReturn(Mono.just(List.of(1.0, 0.0)));
        when(userVectorQueryService.saveLongTermVector(eq("user-1"), any())).thenReturn(Mono.empty());
        when(redisTemplate.delete("user_long_term_vector_snapshot:user-1")).thenReturn(Mono.just(1L));

        StepVerifier.create(service.refreshLongTermVectors(from.toString(), to.toString(), " user-1 "))
                .assertNext(response -> {
                    assertThat(response.userId()).isEqualTo("user-1");
                    assertThat(response.status()).isEqualTo("COMPLETED");
                })
                .verifyComplete();

        verify(userDetailsService).getUserDetailsById("user-1");
        verify(postFeedQueryService).getPostVector("post-1");
        verify(postFeedQueryService, never()).getPostVector("post-2");
        verify(userVectorQueryService).saveLongTermVector(eq("user-1"), any());
        verify(userVectorQueryService, never()).saveLongTermVector(eq("user-2"), any());
    }

    private double vectorLength(List<Double> vector) {
        return Math.sqrt(vector.stream().mapToDouble(value -> value * value).sum());
    }
}

