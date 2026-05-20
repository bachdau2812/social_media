package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.UserNotificationSetting;
import com.dauducbach.clone.modules.notification.repository.UserNotificationSettingRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Collections;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)

public class NotificationService {
    private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

    EmailService emailService;
    PushNotificationService pushNotificationService;
    UserNotificationSettingRepository notificationSettingRepository;

    public Mono<String> sendNotification(NotificationRequest request) {
        if (request.getNotificationType() == NotificationType.SMS) {
            return Mono.error(new AppException(ErrorCode.NOTIFICATION_TYPE_NOT_SUPPORTED));
        }

        var recipients = request.getRecipientIds() == null ? Collections.<String>emptyList() : request.getRecipientIds();
        if (recipients.isEmpty()) {
            return Mono.just("No recipients to process");
        }

        return processRecipients(request, recipients);
    }

    private Mono<String> processRecipients(NotificationRequest request, List<String> recipients) {
        Mono<Void> chain = Mono.empty();

        for (String recipientEmail : recipients) {
            chain = chain.then(
                    resolveNotificationSetting(recipientEmail)
                            .flatMap(setting -> {
                                if (!isEnabledForRecipient(setting, request)) {
                                    logger.info("|NotificationService|sendNotification|skip recipientEmail={} due to notification settings", recipientEmail);
                                    return Mono.empty();
                                }

                                return routeNotification(request, recipientEmail);
                            })
                            .onErrorResume(throwable -> {
                                logger.error("|NotificationService|sendNotification|recipientEmail={}|error={}", recipientEmail, throwable.getMessage());
                                return Mono.empty();
                            })
                            .then()
            );
        }

        return chain.thenReturn("Notifications processed successfully");
    }

    private Mono<Void> routeNotification(NotificationRequest request, String recipientId) {
        NotificationForService handledRequest = NotificationForService.builder()
                .actorId(request.getActorId())
                .actionType(request.getActionType())
                .entityId(request.getEntityId())
                .entityType(request.getEntityType())
                .recipient(recipientId)
                .title(request.getTitle())
                .htmlContent(request.getContent())
                .notificationType(request.getNotificationType())
                .build();

        if (request.getNotificationType() == NotificationType.EMAIL) {
            return emailService.sendEmail(handledRequest).then();
        }

        if (request.getNotificationType() == NotificationType.PUSH) {
            return pushNotificationService.sendPushNotification(handledRequest).then();
        }

        return Mono.error(new AppException(ErrorCode.NOTIFICATION_TYPE_NOT_SUPPORTED));
    }

    private boolean isEnabledForRecipient(UserNotificationSetting setting, NotificationRequest request) {
        if (request.getNotificationType() == NotificationType.EMAIL) {
            return setting.isEmailNotification();
        }

        if (request.getNotificationType() == NotificationType.PUSH) {
            return setting.isPushNotification();
        }

        return false;
    }


    private Mono<UserNotificationSetting> resolveNotificationSetting(String userId) {
        return notificationSettingRepository.findById(userId)
                .switchIfEmpty(Mono.just(UserNotificationSetting.builder()
                        .userId(userId)
                        .pushNotification(true)
                        .emailNotification(true)
                        .likeMyPost(true)
                        .likeFriendPost(true)
                        .commentMyPost(true)
                        .commentFriendPost(true)
                        .newMessage(true)
                        .build()));
    }
}
