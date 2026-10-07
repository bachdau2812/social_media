package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.dauducbach.clone.modules.user.repository.UserFollowerRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserDiscoveryHydratorTest {
    @Mock
    UserIdentityQuery userIdentityQuery;
    @Mock
    UserFollowerRepository followerRepository;

    @Test
    void hydratesAvatarAndMutualRelationship() {
        UserDiscoveryHydrator hydrator = new UserDiscoveryHydrator(
                userIdentityQuery,
                followerRepository
        );
        when(userIdentityQuery.findIdentity("target-1")).thenReturn(Mono.just(new UserIdentity(
                "target-1", "bach", "Dau Duc Bach", "https://cdn/avatar-transformed.jpg")));
        when(followerRepository.existsByFollowerIdAndFollowingId("viewer-1", "target-1"))
                .thenReturn(Mono.just(true));
        when(followerRepository.existsByFollowerIdAndFollowingId("target-1", "viewer-1"))
                .thenReturn(Mono.just(true));

        StepVerifier.create(hydrator.hydrate("viewer-1", "target-1"))
                .assertNext(result -> {
                    assertThat(result.username()).isEqualTo("bach");
                    assertThat(result.fullName()).isEqualTo("Dau Duc Bach");
                    assertThat(result.avatarUrl()).isEqualTo("https://cdn/avatar-transformed.jpg");
                    assertThat(result.viewerFollowsUser()).isTrue();
                    assertThat(result.userFollowsViewer()).isTrue();
                    assertThat(result.friend()).isTrue();
                    assertThat(result.relationship()).isEqualTo("FRIEND");
                })
                .verifyComplete();
    }
}
