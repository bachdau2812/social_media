package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.repository.UserNotificationSettingRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {
    @Mock EmailService emailService;
    @Mock PushNotificationService pushNotificationService;
    @Mock UserNotificationSettingRepository notificationSettingRepository;

    @Test
    void carriesMetadataAndExplicitDeepLinkToPushService() {
        NotificationService service = new NotificationService(
                emailService, pushNotificationService, notificationSettingRepository);
        Map<String, String> metadata = Map.of("POST_ID", "post-1", "COMMENT_ID", "comment-1");
        NotificationRequest request = NotificationRequest.builder()
                .actorId("actor-1")
                .actionType(UserActionType.COMMENT)
                .entityId("comment-1")
                .entityType("COMMENT")
                .recipientIds(List.of("recipient-1"))
                .title("Bình luận mới")
                .content("A đã bình luận về bài viết của bạn")
                .metadata(metadata)
                .deepLink("/posts/post-1?commentId=comment-1")
                .notificationType(NotificationType.PUSH)
                .build();

        when(notificationSettingRepository.findById("recipient-1")).thenReturn(Mono.empty());
        when(pushNotificationService.sendPushNotification(any())).thenReturn(Mono.just("OK"));

        StepVerifier.create(service.sendNotification(request))
                .expectNext("Notifications processed successfully")
                .verifyComplete();

        ArgumentCaptor<NotificationForService> captor = ArgumentCaptor.forClass(NotificationForService.class);
        verify(pushNotificationService).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getMetadata()).isEqualTo(metadata);
        assertThat(captor.getValue().getDeepLink()).isEqualTo("/posts/post-1?commentId=comment-1");
        assertThat(captor.getValue().getHtmlContent())
                .isEqualTo("A đã bình luận về bài viết của bạn");
    }
}
