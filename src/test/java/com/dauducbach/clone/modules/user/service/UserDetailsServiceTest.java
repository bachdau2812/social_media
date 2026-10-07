package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.profile.application.ProfileCache;
import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserDetailsServiceTest {
    @Mock UserDetailsRepository userDetailsRepository;
    @Mock R2dbcEntityTemplate entityTemplate;
    @Mock ProfileCache profileCache;
    @Mock AuditRecorder auditRecorder;
    @Mock UserProfileVectorEventPublisher vectorEventPublisher;
    @Mock com.dauducbach.clone.modules.user.publicapi.UserDeletionCleanup vectorCleanup;

    @Test
    void cacheWriteFailureDoesNotFailCreatedProfileOrSkipVectorEvent() {
        UserDetailsService service = newService();
        ReactiveInsertOperation.ReactiveInsert<UserDetails> insert = mock(ReactiveInsertOperation.ReactiveInsert.class);
        UserDetails details = UserDetails.builder().userId("user-1").username("bach").build();
        when(entityTemplate.insert(UserDetails.class)).thenReturn(insert);
        when(insert.using(any(UserDetails.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(profileCache.put(details)).thenReturn(Mono.error(new IllegalStateException("redis unavailable")));
        when(vectorEventPublisher.publishRefreshEventForCreatedUser(
                anyString(), anyString(), anyString(), anyString(), any(UserDetails.class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.insertUserDetails(details))
                .expectNext(details)
                .verifyComplete();

        verify(vectorEventPublisher).publishRefreshEventForCreatedUser(
                "user-1", "USER_DETAILS", "CREATE", "user-1", details);
    }

    @Test
    void cacheReadFailureFallsBackToDatabaseAndKeepsCacheBestEffort() {
        UserDetailsService service = newService();
        UserDetails details = UserDetails.builder().userId("user-1").username("bach").build();
        when(profileCache.find("user-1")).thenReturn(Mono.error(new IllegalStateException("redis unavailable")));
        when(userDetailsRepository.findById("user-1")).thenReturn(Mono.just(details));
        when(profileCache.put(details)).thenReturn(Mono.error(new IllegalStateException("redis unavailable")));

        StepVerifier.create(service.getUserDetailsById("user-1"))
                .expectNext(details)
                .verifyComplete();
    }

    @Test
    void cacheHitAvoidsDatabaseRead() {
        UserDetailsService service = newService();
        UserDetails details = UserDetails.builder().userId("user-1").username("bach").build();
        when(profileCache.find("user-1")).thenReturn(Mono.just(details));

        StepVerifier.create(service.getUserDetailsById("user-1"))
                .expectNext(details)
                .verifyComplete();

        org.mockito.Mockito.verifyNoInteractions(userDetailsRepository);
    }

    private UserDetailsService newService() {
        return new UserDetailsService(
                userDetailsRepository,
                entityTemplate,
                profileCache,
                auditRecorder,
                vectorEventPublisher,
                vectorCleanup);
    }
}
