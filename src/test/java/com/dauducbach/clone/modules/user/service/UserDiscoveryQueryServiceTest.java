package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UserDiscoveryQueryServiceTest {
    @Test
    void projectsOnlyDiscoveryFieldsAndDelegatesIdentityHydration() {
        UserDetailsRepository users = mock(UserDetailsRepository.class);
        UserDiscoveryHydrator hydrator = mock(UserDiscoveryHydrator.class);
        UserDiscoveryQueryService query = new UserDiscoveryQueryService(users, hydrator);
        UserDetails details = new UserDetails();
        details.setUserId("user-1");
        details.setLivingIn("HCMC");
        details.setHometown("Hue");
        details.setHobbyList(List.of("music", "hiking"));
        when(users.findById("user-1")).thenReturn(Mono.just(details));
        when(users.findAllUserIds()).thenReturn(Flux.just("user-1"));

        StepVerifier.create(query.findProfile("user-1"))
                .assertNext(profile -> {
                    assertThat(profile.userId()).isEqualTo("user-1");
                    assertThat(profile.livingIn()).isEqualTo("HCMC");
                    assertThat(profile.hometown()).isEqualTo("Hue");
                    assertThat(profile.hobbies()).containsExactly("music", "hiking");
                })
                .verifyComplete();

        UserDiscoveryResponse response = new UserDiscoveryResponse("user-1", "bach", "Bach", "/avatar",
                false, false, false, "NONE");
        when(hydrator.hydrate("viewer-1", "user-1")).thenReturn(Mono.just(response));
        StepVerifier.create(query.hydrate("viewer-1", "user-1")).expectNext(response).verifyComplete();
        StepVerifier.create(query.findAllUserIds()).expectNext("user-1").verifyComplete();
        verify(hydrator).hydrate("viewer-1", "user-1");
    }
}
