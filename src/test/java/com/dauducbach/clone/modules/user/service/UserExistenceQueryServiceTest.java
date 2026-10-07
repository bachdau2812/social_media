package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserExistenceQueryServiceTest {
    @Test
    void delegatesExistenceCheckToUserRepository() {
        UserDetailsRepository repository = mock(UserDetailsRepository.class);
        when(repository.existsById("user-1")).thenReturn(Mono.just(true));

        StepVerifier.create(new UserExistenceQueryService(repository).exists("user-1"))
                .expectNext(true)
                .verifyComplete();
    }
}
