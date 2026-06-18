package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor

public class AuthModuleNotificationHandler {
    private static final Logger log = LoggerFactory.getLogger(AuthModuleNotificationHandler.class);
    NotificationService notificationService;
    UserCredentialsRepository userCredentialsRepository;
    NotificationTemplatesRepository notificationTemplatesRepository;

    @KafkaListener(topics = "auth_send_code", groupId = "notification-service")
    public void handleSendCodeForRegistration(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        String username = String.valueOf(payloadJson.get("username"));
        String email = String.valueOf(payloadJson.get("email"));
        String code = String.valueOf(payloadJson.get("code"));

        /// Build notification request
        var notificationRequest = NotificationRequest.builder()
                .actorId("register_notification" + username)
                .notificationType(NotificationType.EMAIL)
                .actionType(UserActionType.REGISTRATION)
                .entityId(null)
                .entityType(null)
                .recipientIds(List.of(email))
                .title("Registration Code")
                .build();

        notificationTemplatesRepository.findByActionType(notificationRequest.getActionType())
                .doOnSuccess(notificationTemplates -> log.info("|AuthModuleNotificationHandler|handleSendCodeForRegistration|fetched template for actionType={}|templateId={}", notificationRequest.getActionType(), notificationTemplates.getId()))
                .flatMap(notificationTemplates -> {
                    String processedHtml = notificationTemplates.getTemplate()
                            .replace("{{USERNAME}}", username)
                            .replace("{{REGISTRATION_CODE}}", code);

                    notificationRequest.setContent(processedHtml);

                    return notificationService.sendNotification(notificationRequest);
                })
                .subscribe();
    }

    @KafkaListener(topics = "forget_password_event", groupId = "notification-service")
    public void handleForgetPasswordEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        String code = String.valueOf(payloadJson.get("code"));
        String email = String.valueOf(payloadJson.get("email"));

        /// Build notification request
        var notificationRequest = NotificationRequest.builder()
                .actorId("forget_password_notification" + email)
                .notificationType(NotificationType.EMAIL)
                .actionType(UserActionType.FORGOT_PASSWORD)
                .entityId(null)
                .entityType(null)
                .recipientIds(List.of(email))
                .title("Forget Password Code")
                .build();

        ///  Send notification
        notificationTemplatesRepository.findByActionType(notificationRequest.getActionType())
                .doOnSuccess(notificationTemplates -> log.info("|AuthModuleNotificationHandler|handleForgetPasswordEvent|fetched template for actionType={}|templateId={}", notificationRequest.getActionType(), notificationTemplates.getId()))
                .flatMap(notificationTemplates -> {
                    String processedHtml = notificationTemplates.getTemplate()
                            .replace("{{EMAIL}}", email)
                            .replace("{{CODE}}", code);

                    notificationRequest.setContent(processedHtml);

                    return notificationService.sendNotification(notificationRequest);
                })
                .subscribe();

    }

    @KafkaListener(topics = "new_password_event", groupId = "notification-service")
    public void handleSendNewPasswordEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        String newPassword = String.valueOf(payloadJson.get("newPassword"));
        String email = String.valueOf(payloadJson.get("email"));

        /// Build notification request
        var notificationRequest = NotificationRequest.builder()
                .actorId("new_password_notification" + email)
                .notificationType(NotificationType.EMAIL)
                .actionType(UserActionType.RESET_PASSWORD)
                .entityId(null)
                .entityType(null)
                .recipientIds(List.of(email))
                .title("Your New Password")
                .build();

        ///  Send notification
        notificationTemplatesRepository.findByActionType(notificationRequest.getActionType())
                .doOnSuccess(notificationTemplates -> log.info("|AuthModuleNotificationHandler|handleSendNewPasswordEvent|fetched template for actionType={}|templateId={}", notificationRequest.getActionType(), notificationTemplates.getId()))
                .flatMap(notificationTemplates -> {
                    String processedHtml = notificationTemplates.getTemplate()
                            .replace("{{EMAIL}}", email)
                            .replace("{{NEW_PASSWORD}}", newPassword);

                    notificationRequest.setContent(processedHtml);

                    return notificationService.sendNotification(notificationRequest);
                })
                .subscribe();
    }

    @KafkaListener(topics = "new_password_and_username_event", groupId = "notification-service")
    public void handleSendNewPasswordAndUsernameEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        String newPassword = String.valueOf(payloadJson.get("newPassword"));
        String email = String.valueOf(payloadJson.get("email"));

        /// Build notification request
        var notificationRequest = NotificationRequest.builder()
                .actorId("new_password_and_username_notification" + email)
                .notificationType(NotificationType.EMAIL)
                .actionType(UserActionType.RESET_PASSWORD_AND_USERNAME)
                .entityId(null)
                .entityType(null)
                .recipientIds(List.of(email))
                .title("Your New Password")
                .build();

        ///  Send notification
        notificationTemplatesRepository.findByActionType(notificationRequest.getActionType())
                .doOnSuccess(notificationTemplates -> log.info("|AuthModuleNotificationHandler|handleSendNewPasswordEvent|fetched template for actionType={}|templateId={}", notificationRequest.getActionType(), notificationTemplates.getId()))
                .flatMap(notificationTemplates -> {
                    String processedHtml = notificationTemplates.getTemplate()
                            .replace("{{EMAIL}}", email)
                            .replace("{{NEW_PASSWORD}}", newPassword);

                    notificationRequest.setContent(processedHtml);

                    return notificationService.sendNotification(notificationRequest);
                })
                .subscribe();
    }

    @KafkaListener(topics = "profile_creation_event", groupId = "notification-service")
    public void handleProfileCreationEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        String username = payloadJson.get("username").toString();
        String email = String.valueOf(payloadJson.get("email"));

        /// Build notification request
        var notificationRequest = NotificationRequest.builder()
                .actorId("welcome_user" + email)
                .notificationType(NotificationType.EMAIL)
                .actionType(UserActionType.WELCOME_USER)
                .entityId(null)
                .entityType(null)
                .recipientIds(List.of(email))
                .title("Welcome to Our Service")
                .build();

        ///  Send notification
        notificationTemplatesRepository.findByActionType(notificationRequest.getActionType())
                .doOnSuccess(notificationTemplates -> log.info("|AuthModuleNotificationHandler|handleSendNewPasswordEvent|fetched template for actionType={}|templateId={}", notificationRequest.getActionType(), notificationTemplates.getId()))
                .flatMap(notificationTemplates -> {
                    String processedHtml = notificationTemplates.getTemplate()
                            .replace("{{USERNAME}}", username);

                    notificationRequest.setContent(processedHtml);

                    return notificationService.sendNotification(notificationRequest);
                })
                .subscribe();
    }
}
