package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.repositoty.UserFollowerRepository;
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
    UserDetailsService userDetailsService;
    @Mock
    MediaForProfile mediaForProfile;
    @Mock
    UserFollowerRepository followerRepository;

    @Test
    void hydratesAvatarAndMutualRelationship() {
        UserDiscoveryHydrator hydrator = new UserDiscoveryHydrator(
                userDetailsService,
                mediaForProfile,
                followerRepository
        );
        UserDetails details = UserDetails.builder()
                .userId("target-1")
                .username("bach")
                .fullName("Dau Duc Bach")
                .build();
        Media avatar = Media.builder()
                .secureUrl("https://cdn/avatar-transformed.jpg")
                .build();

        when(userDetailsService.getUserDetailsById("target-1")).thenReturn(Mono.just(details));
        when(mediaForProfile.getCurrentAvatar("target-1", MediaDisplayType.AVATAR)).thenReturn(Mono.just(avatar));
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
