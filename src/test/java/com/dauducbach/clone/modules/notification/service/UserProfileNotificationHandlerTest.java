package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import com.dauducbach.clone.modules.user.service.UserFollowerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

import java.util.List;

@ExtendWith(MockitoExtension.class)
class UserProfileNotificationHandlerTest {
    @Mock
    NotificationService notificationService;
    @Mock
    NotificationTemplatesRepository notificationTemplatesRepository;
    @Mock
    UserFollowerService userFollowerService;
    @Mock
    UserIdentityQueryService userIdentityQueryService;

    @ParameterizedTest
    @EnumSource(value = UserActionType.class, names = {"AVATAR_UPDATE", "UP_STORY"})
    void avatarAndStoryNotifyEveryFollowerPageWithoutSelfOrDuplicates(UserActionType action) {
        UserProfileNotificationHandler handler = newHandler();
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100)).thenReturn(Mono.just(followers(true, "viewer-1", "owner-1")));
        when(userFollowerService.getFollowers("owner-1", 1, 100)).thenReturn(Mono.just(followers(false, "viewer-1", "viewer-2")));
        when(notificationTemplatesRepository.findByActionType(action)).thenReturn(Mono.just(NotificationTemplates.builder()
                .actionType(action).template("{{USERNAME}} updated media").build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        if (action == UserActionType.AVATAR_UPDATE) {
            handler.handleAvatarUpdateEvent("{\"userId\":\"owner-1\",\"avatarUrl\":\"https://cdn/avatar.png\",\"mediaId\":\"media-1\"}").join();
        } else {
            handler.handleStorySuccessEvent("{\"userId\":\"owner-1\",\"storyId\":\"story-1\"}").join();
        }

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService).sendNotification(captor.capture());
        assertThat(captor.getValue().getRecipientIds()).containsExactly("viewer-1", "viewer-2");
        assertThat(captor.getValue().getActionType()).isEqualTo(action);
        verify(userFollowerService).getFollowers("owner-1", 1, 100);
    }

    @Test
    void avatarNotificationHasDefaultTextWhenNoTemplateIsConfigured() {
        UserProfileNotificationHandler handler = newHandler();
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100)).thenReturn(Mono.just(followers(false, "viewer-1")));
        when(notificationTemplatesRepository.findByActionType(UserActionType.AVATAR_UPDATE)).thenReturn(Mono.empty());
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));
        handler.handleAvatarUpdateEvent("{\"userId\":\"owner-1\",\"mediaId\":\"media-1\"}").join();
        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService).sendNotification(captor.capture());
        assertThat(captor.getValue().getContent()).isEqualTo("Bach đã cập nhật ảnh đại diện.");
        assertThat(captor.getValue().getDedupKey()).isEqualTo("AVATAR_UPDATE_UPLOAD:media-1");
    }

    @Test
    void followerLookupFailureFailsTheListenerInsteadOfSendingToOnlyOnePage() {
        UserProfileNotificationHandler handler = newHandler();
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100)).thenReturn(Mono.just(followers(true, "viewer-1")));
        when(userFollowerService.getFollowers("owner-1", 1, 100)).thenReturn(Mono.error(new IllegalStateException("lookup failed")));
        assertThatThrownBy(() -> handler.handleAvatarUpdateEvent("{\"userId\":\"owner-1\"}").join())
                .hasCauseInstanceOf(IllegalStateException.class);
        verify(notificationService, never()).sendNotification(any());
    }

    @Test
    void notificationDeliveryFailureFailsTheKafkaListener() {
        UserProfileNotificationHandler handler = newHandler();
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100)).thenReturn(Mono.just(followers(false, "viewer-1")));
        when(notificationTemplatesRepository.findByActionType(UserActionType.AVATAR_UPDATE)).thenReturn(Mono.just(NotificationTemplates.builder()
                .actionType(UserActionType.AVATAR_UPDATE).template("{{USERNAME}} updated avatar").build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.error(new IllegalStateException("send failed")));
        assertThatThrownBy(() -> handler.handleAvatarUpdateEvent("{\"userId\":\"owner-1\"}").join())
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private UserProfileNotificationHandler newHandler() {
        return new UserProfileNotificationHandler(notificationService, notificationTemplatesRepository, userFollowerService, userIdentityQueryService);
    }

    private FollowerListResponse followers(boolean hasNext, String... ids) {
        return FollowerListResponse.builder().followers(java.util.Arrays.stream(ids)
                .map(id -> FollowerListResponse.FollowerInfo.builder().userId(id).build()).toList())
                .hasNextPage(hasNext).build();
    }

    @Test
    void handleFollowEventSendsFollowNotificationToFollowedUser() {
        UserProfileNotificationHandler handler = new UserProfileNotificationHandler(
                notificationService,
                notificationTemplatesRepository,
                userFollowerService,
                userIdentityQueryService
        );

        when(userIdentityQueryService.resolveUsername("follower-1")).thenReturn(Mono.just("Bach"));
        when(notificationTemplatesRepository.findByActionType(UserActionType.FOLLOW_EVENT))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(1)
                        .actionType(UserActionType.FOLLOW_EVENT)
                        .template("{{USERNAME}} da theo doi ban")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleFollowEvent("""
                {"followerId":"follower-1","followingId":"following-1"}
                """);

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000)).sendNotification(captor.capture());

        NotificationRequest request = captor.getValue();
        assertThat(request.getActionType()).isEqualTo(UserActionType.FOLLOW_EVENT);
        assertThat(request.getRecipientIds()).containsExactly("following-1");
        assertThat(request.getNotificationType()).isEqualTo(NotificationType.PUSH);
        assertThat(request.getContent()).isEqualTo("Bach da theo doi ban");
    }

    @Test
    void handleStoryEventDeduplicatesByPublicationInsteadOfStoryItem() {
        UserProfileNotificationHandler handler = new UserProfileNotificationHandler(
                notificationService,
                notificationTemplatesRepository,
                userFollowerService,
                userIdentityQueryService
        );
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100))
                .thenReturn(Mono.just(FollowerListResponse.builder()
                        .followers(List.of(FollowerListResponse.FollowerInfo.builder().userId("viewer-1").build()))
                        .hasNextPage(false)
                        .build()));
        when(notificationTemplatesRepository.findByActionType(UserActionType.UP_STORY))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(2)
                        .actionType(UserActionType.UP_STORY)
                        .template("{{USERNAME}} da dang Story")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleStorySuccessEvent("""
                {"storyId":"story-2","userId":"owner-1","publicationId":"publication-1","publicationOrder":2,"publicationItemCount":3}
                """);

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000)).sendNotification(captor.capture());
        NotificationRequest request = captor.getValue();
        assertThat(request.getEntityId()).isEqualTo("story-2");
        assertThat(request.getDedupKey()).isEqualTo("UP_STORY:publication-1");
        assertThat(request.getMetadata()).containsEntry("PUBLICATION_ID", "publication-1");
        assertThat(request.getRecipientIds()).containsExactly("viewer-1");
    }
}
