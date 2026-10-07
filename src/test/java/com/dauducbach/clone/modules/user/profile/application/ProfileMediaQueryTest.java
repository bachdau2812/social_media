package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProfileMediaQueryTest {
    @Mock MediaCatalog mediaCatalog;
    @Mock MediaAssets mediaAssets;

    @Test
    void appliesAvatarDisplayTransformAtProfileQueryBoundary() {
        ProfileMediaQuery query = new ProfileMediaQuery(mediaCatalog, mediaAssets);
        MediaAssetView avatar = new MediaAssetView(
                "avatar-1", "avatar", 0, 0, "jpg", "image", 1,
                "https://cdn.example/avatar.jpg", "https://cdn.example/avatar.jpg",
                "user-1", OwnerType.AVATAR, null, null, null, null, null);
        when(mediaCatalog.findCurrentAvatar("user-1")).thenReturn(Mono.just(avatar));
        when(mediaAssets.transformDeliveryUrl("https://cdn.example/avatar.jpg", MediaDisplayType.AVATAR))
                .thenReturn("https://cdn.example/avatar-small.jpg");

        StepVerifier.create(query.getCurrentAvatar("user-1", null))
                .expectNextMatches(result -> result.url().endsWith("avatar-small.jpg")
                        && result.secureUrl().endsWith("avatar-small.jpg"))
                .verifyComplete();
    }

    @Test
    void mediaHistoryKeepsOwnerFilterAndRequestedPage() {
        ProfileMediaQuery query = new ProfileMediaQuery(mediaCatalog, mediaAssets);
        PageResponse<MediaAssetView> page = PageResponse.of(List.of(), 2, 25, 10);
        when(mediaCatalog.findProfileMedia("user-1", OwnerType.POST, 2, 25)).thenReturn(Mono.just(page));

        StepVerifier.create(query.getUploadedMedia("user-1", OwnerType.POST, 2, 25))
                .expectNext(page)
                .verifyComplete();

        verify(mediaCatalog).findProfileMedia("user-1", OwnerType.POST, 2, 25);
    }
}
