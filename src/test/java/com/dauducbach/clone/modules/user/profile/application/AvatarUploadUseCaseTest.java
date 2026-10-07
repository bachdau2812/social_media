package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaInspection;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.google.gson.JsonObject;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvatarUploadUseCaseTest {
    @Mock UserIdentityQuery userIdentityQuery;
    @Mock AvatarMediaEventPublisher avatarMediaEventPublisher;
    @Mock UserSsePublisher userSsePublisher;
    @Mock MediaAssets mediaAssets;
    @Mock MediaInspection mediaInspection;
    @Mock AuditRecorder auditRecorder;

    @Test
    void uploadPublishesScanRequestWithResolvedPublicId() {
        AvatarUploadUseCase useCase = newUseCase();
        when(userIdentityQuery.exists("user-1")).thenReturn(Mono.just(true));
        when(avatarMediaEventPublisher.requestAvatarScan(
                "user-1",
                "https://res.cloudinary.com/demo/image/upload/v1781617130/social_network_posts/avatar_1.jpg",
                "social_network_posts/avatar_1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(useCase.uploadAvatar(new AvatarUploadRequest(
                        "user-1",
                        "https://res.cloudinary.com/demo/image/upload/v1781617130/social_network_posts/avatar_1.jpg")))
                .expectNextMatches(response -> response.userId().equals("user-1")
                        && response.ownerType().equals(OwnerType.AVATAR.name())
                        && response.status().equals("PENDING_SCAN"))
                .verifyComplete();
    }

    @Test
    void approvedAvatarIsRegisteredAndPublishedBeforeApprovalSse() {
        AvatarUploadUseCase useCase = newUseCase();
        String url = "https://res.cloudinary.com/demo/image/upload/v1/folder/avatar.png";
        MediaAssetView saved = avatar("media-1", "folder/avatar", url);
        when(mediaInspection.inspect(url, "folder/avatar"))
                .thenReturn(Mono.just(new MediaInspection.Result(false)));
        when(mediaAssets.registerCloudinaryAsset("folder/avatar", "user-1", OwnerType.AVATAR))
                .thenReturn(Mono.just(saved));
        when(avatarMediaEventPublisher.publishAvatarUpdated("user-1", url, "media-1"))
                .thenReturn(Mono.empty());
        when(userSsePublisher.sendToUser(eq("user-1"), eq("avatar_upload_event"), anyString()))
                .thenAnswer(invocation -> {
                    JsonObject payload = GsonUtils.fromString(invocation.getArgument(2));
                    assertThat(payload.get("result").getAsString()).isEqualTo("APPROVED");
                    assertThat(payload.get("publicId").getAsString()).isEqualTo("folder/avatar");
                    return Mono.empty();
                });

        StepVerifier.create(useCase.reviewAvatar(new AvatarMediaReviewRequested("user-1", url, "folder/avatar")))
                .verifyComplete();

        var order = inOrder(mediaAssets, avatarMediaEventPublisher, userSsePublisher);
        order.verify(mediaAssets).registerCloudinaryAsset("folder/avatar", "user-1", OwnerType.AVATAR);
        order.verify(avatarMediaEventPublisher).publishAvatarUpdated("user-1", url, "media-1");
        order.verify(userSsePublisher).sendToUser(eq("user-1"), eq("avatar_upload_event"), anyString());
    }

    @Test
    void rejectedAvatarWaitsForCleanupBeforeSseAndAudit() {
        AvatarUploadUseCase useCase = newUseCase();
        String url = "https://cdn.example/avatar.png";
        when(mediaInspection.inspect(url, "avatar")).thenReturn(Mono.just(new MediaInspection.Result(true)));
        when(mediaAssets.deleteAsset("avatar")).thenReturn(Mono.empty());
        when(userSsePublisher.sendToUser(eq("user-1"), eq("avatar_upload_event"), anyString()))
                .thenReturn(Mono.empty());
        when(auditRecorder.record(any())).thenReturn(Mono.empty());

        StepVerifier.create(useCase.reviewAvatar(new AvatarMediaReviewRequested("user-1", url, "avatar")))
                .verifyComplete();

        var order = inOrder(mediaAssets, userSsePublisher, auditRecorder);
        order.verify(mediaAssets).deleteAsset("avatar");
        order.verify(userSsePublisher).sendToUser(eq("user-1"), eq("avatar_upload_event"), anyString());
        order.verify(auditRecorder).record(any());
        verify(mediaAssets, never()).registerCloudinaryAsset(any(), any(), any());
    }

    @Test
    void missingScanEventIsIgnored() {
        AvatarUploadUseCase useCase = newUseCase();

        StepVerifier.create(useCase.reviewAvatar(new AvatarMediaReviewRequested(" ", " ", "")))
                .verifyComplete();

        org.mockito.Mockito.verifyNoInteractions(mediaInspection, mediaAssets, userSsePublisher, auditRecorder);
    }

    private AvatarUploadUseCase newUseCase() {
        lenient().when(auditRecorder.record(any())).thenReturn(Mono.empty());
        return new AvatarUploadUseCase(
                userIdentityQuery,
                avatarMediaEventPublisher,
                userSsePublisher,
                mediaAssets,
                mediaInspection,
                auditRecorder);
    }

    private MediaAssetView avatar(String id, String publicId, String url) {
        return new MediaAssetView(id, publicId, 0, 0, null, null, 0, url, url,
                "user-1", OwnerType.AVATAR, null, null, null, null, null);
    }
}
