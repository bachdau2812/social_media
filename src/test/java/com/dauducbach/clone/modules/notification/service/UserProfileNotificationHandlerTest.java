package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import com.dauducbach.clone.modules.user.service.UserFollowerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserProfileNotificationHandlerTest {
    @Mock
    NotificationService notificationService;
    @Mock
    NotificationTemplatesRepository notificationTemplatesRepository;
    @Mock
    UserFollowerService userFollowerService;
    @Mock
    UserDetailsService userDetailsService;

    @Test
    void handleFollowEventSendsFollowNotificationToFollowedUser() {
        UserProfileNotificationHandler handler = new UserProfileNotificationHandler(
                notificationService,
                notificationTemplatesRepository,
                userFollowerService,
                userDetailsService
        );

        when(userDetailsService.getUserDetailsById("follower-1"))
                .thenReturn(Mono.just(UserDetails.builder().userId("follower-1").username("Bach").build()));
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
}
