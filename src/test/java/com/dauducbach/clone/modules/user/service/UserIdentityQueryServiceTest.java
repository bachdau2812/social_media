package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.media.publicapi.MediaUrlDelivery;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserIdentityQueryServiceTest {
    @Mock
    UserDetailsService userDetailsService;
    @Mock
    MediaCatalog mediaCatalog;
    @Mock
    MediaUrlDelivery mediaUrlDelivery;

    private UserIdentityQueryService service() {
        return new UserIdentityQueryService(userDetailsService, mediaCatalog, mediaUrlDelivery);
    }

    @Test
    void resolvesBatchIdentityInRequestedOrderWithOneDetailsAndAvatarRead() {
        when(userDetailsService.getUserDetailsByIds(List.of("user-b", "user-a")))
                .thenReturn(Flux.just(
                        UserDetails.builder().userId("user-a").username("alice").fullName("Alice A").build(),
                        UserDetails.builder().userId("user-b").username("bob").fullName("Bob B").build()));
        when(mediaCatalog.findCurrentAvatars(List.of("user-b", "user-a")))
                .thenReturn(Flux.just(
                        avatar("user-a", "https://cdn.example/alice.jpg"),
                        avatar("user-b", "https://cdn.example/bob.jpg")));

        StepVerifier.create(service().findIdentities(List.of("user-b", "user-a", "user-b", " ")))
                .expectNextMatches(identity -> identity.userId().equals("user-b")
                        && identity.username().equals("bob")
                        && identity.fullName().equals("Bob B")
                        && identity.avatarUrl().equals("https://cdn.example/bob.jpg"))
                .expectNextMatches(identity -> identity.userId().equals("user-a")
                        && identity.username().equals("alice")
                        && identity.fullName().equals("Alice A")
                        && identity.avatarUrl().equals("https://cdn.example/alice.jpg"))
                .verifyComplete();

        verify(userDetailsService).getUserDetailsByIds(List.of("user-b", "user-a"));
        verify(mediaCatalog).findCurrentAvatars(List.of("user-b", "user-a"));
    }

    @Test
    void emptyBatchDoesNotCallProfileOrMediaAdapters() {
        StepVerifier.create(service().findIdentities(List.of()))
                .verifyComplete();

        org.mockito.Mockito.verifyNoInteractions(userDetailsService, mediaCatalog);
    }

    @Test
    void missingAvatarDoesNotHideExistingIdentity() {
        when(userDetailsService.getUserDetailsById("user-a")).thenReturn(Mono.just(
                UserDetails.builder().userId("user-a").username("alice").fullName("Alice A").build()));
        when(mediaCatalog.findCurrentAvatar("user-a")).thenReturn(Mono.error(new IllegalStateException("media unavailable")));

        StepVerifier.create(service().findIdentity("user-a"))
                .expectNextMatches(identity -> identity.userId().equals("user-a")
                        && identity.username().equals("alice")
                        && identity.avatarUrl().isEmpty())
                .verifyComplete();
    }

    private MediaAssetView avatar(String ownerId, String secureUrl) {
        return new MediaAssetView(
                "avatar-" + ownerId, "public-" + ownerId, 0, 0, "jpg", "image", 1,
                null, secureUrl, ownerId, null, null, null, null, null, null);
    }
}
