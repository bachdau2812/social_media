package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.constants.NotificationStatus;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.dto.response.PushTokenRegisterResponse;
import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.NotificationPushToken;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import com.dauducbach.clone.modules.notification.repository.NotificationEventsRepository;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.UUID;

@Service
@FieldDefaults(level = lombok.AccessLevel.PRIVATE,  makeFinal = true)

public class PushNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);
    UserPushNotificationRepository userPushNotificationRepository;
    NotificationEventsRepository notificationEventsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    NotificationPushGateway notificationPushGateway;
    NotificationDestinationBuilder destinationBuilder;
    NotificationContentNormalizer contentNormalizer;
    NotificationMetadataCodec metadataCodec;
    FirebasePushMessageFactory pushMessageFactory;
    NotificationPersistenceFlow notificationPersistenceFlow;

    @Autowired
    public PushNotificationService(
            UserPushNotificationRepository userPushNotificationRepository,
            NotificationEventsRepository notificationEventsRepository,
            R2dbcEntityTemplate r2dbcEntityTemplate,
            NotificationPushGateway notificationPushGateway,
            NotificationDestinationBuilder destinationBuilder,
            NotificationContentNormalizer contentNormalizer,
            NotificationMetadataCodec metadataCodec,
            FirebasePushMessageFactory pushMessageFactory
    ) {
        this.userPushNotificationRepository = userPushNotificationRepository;
        this.notificationEventsRepository = notificationEventsRepository;
        this.r2dbcEntityTemplate = r2dbcEntityTemplate;
        this.notificationPushGateway = notificationPushGateway;
        this.destinationBuilder = destinationBuilder;
        this.contentNormalizer = contentNormalizer;
        this.metadataCodec = metadataCodec;
        this.pushMessageFactory = pushMessageFactory;
        this.notificationPersistenceFlow = new NotificationPersistenceFlow(
                userPushNotificationRepository, notificationEventsRepository, r2dbcEntityTemplate,
                notificationPushGateway, destinationBuilder, contentNormalizer, metadataCodec, pushMessageFactory);
    }

    PushNotificationService(
            UserPushNotificationRepository userPushNotificationRepository,
            NotificationEventsRepository notificationEventsRepository,
            R2dbcEntityTemplate r2dbcEntityTemplate
    ) {
        this(
                userPushNotificationRepository,
                notificationEventsRepository,
                r2dbcEntityTemplate,
                new FirebaseNotificationPushGateway(),
                new NotificationDestinationBuilder(),
                new NotificationContentNormalizer(),
                new NotificationMetadataCodec(new ObjectMapper()),
                new FirebasePushMessageFactory());
    }

    public Mono<PushTokenRegisterResponse> registerPushToken(PushTokenRegisterRequest request) {
        return Mono.defer(() -> {
            validatePushTokenRequest(request);

            String userId = request.userId().trim();
            String deviceId = normalizeOptional(request.deviceId());
            String deviceToken = request.deviceToken().trim();

            return userPushNotificationRepository.findByUserId(userId)
                    .flatMap(existing -> updatePushToken(existing, deviceId, deviceToken))
                    .switchIfEmpty(Mono.defer(() -> createPushToken(userId, deviceId, deviceToken)))
                    .map(this::toRegisterResponse)
                    .doOnSuccess(response -> logger.info("|PushNotificationService|registerPushToken|success|userId={}|tokenId={}",
                            response.userId(), response.id()))
                    .onErrorMap(error -> error instanceof AppException
                            ? error
                            : new AppException(
                                    ErrorCode.PUSH_TOKEN_SAVE_FAILED,
                                    String.format("Save push token failed for userId=%s", userId),
                                    error
                            ));
        });
    }

    public Mono<String> sendPushNotification(NotificationForService request) {
        return Mono.defer(() -> buildAndPersistNotification(request));
    }

    private Mono<String> buildAndPersistNotification(NotificationForService request) {
        return notificationPersistenceFlow.send(request);
    }

    private Mono<String> sendPushNotificationLegacy(NotificationForService request) {
        if (request.getRecipient() == null || request.getRecipient().isBlank()) {
            return Mono.error(new AppException(ErrorCode.SEND_PUSH_NOTIFICATION_FAILED, "Recipient userId is missing"));
        }

        return userPushNotificationRepository.findByUserId(request.getRecipient())
                .flatMap(notificationPushToken -> {
                    logger.info("|PushNotificationService|sendPushNotification|recipientId={}", request.getRecipient());

                    Notification noti = Notification.builder()
                            .setTitle(request.getTitle())
                            .setBody(request.getHtmlContent())
                            .build();

                    Message msg = Message.builder()
                            .setToken(notificationPushToken.getDeviceToken())
                            .setNotification(noti)
                            .build();

                    var entity = NotificationEvents.builder()
                            .id(UUID.randomUUID().toString())
                            .actorId(request.getActorId())
                            .entityType(request.getEntityType())
                            .entityId(request.getEntityId())
                            .actionType(request.getActionType())
                            .createdAt(Instant.now())
                            .build();

                    var userNotification = UserNotifications.builder()
                            .id(UUID.randomUUID().toString())
                            .userId(request.getRecipient())
                            .eventId(entity.getId())
                            .notificationStatus(NotificationStatus.UNREAD)
                            .readAt(null)
                            .createdAt(Instant.now())
                            .build();

                    return Mono.fromCallable(() -> {
                                FirebaseMessaging.getInstance().send(msg);
                                logger.info("|PushNotificationService|sendPushNotification|push notification sent successfully|recipientId={}", request.getRecipient());
                                return entity;
                            })
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(savedEntity -> r2dbcEntityTemplate.insert(NotificationEvents.class).using(savedEntity)
                                    .doOnSuccess(event -> logger.info("|PushNotificationService|sendPushNotification|notification event saved successfully|eventId={}", event.getId()))
                                    .then(r2dbcEntityTemplate.insert(UserNotifications.class).using(userNotification)
                                            .doOnSuccess(savedUserNoti -> logger.info("|PushNotificationService|sendPushNotification|user notification saved successfully|recipientId={}|notificationId={}", request.getRecipient(), savedUserNoti.getId()))
                                            .thenReturn("Push notifications sent successfully"))
                                    .onErrorResume(throwable -> {
                                        logger.info("|PushNotificationService|sendPushNotification|error when saving user notification|recipientId={}|error={}", request.getRecipient(), throwable.getMessage());

                                        return notificationEventsRepository.deleteById(entity.getId())
                                                .then(Mono.error(new AppException(ErrorCode.SEND_PUSH_NOTIFICATION_FAILED)));
                                    }))
                            .onErrorMap(throwable -> {
                                if (throwable instanceof AppException appException) {
                                    return appException;
                                }

                                logger.error("|PushNotificationService|sendPushNotification|error when sending push notification|recipientId={}|error={}", request.getRecipient(), throwable.getMessage());
                                return new AppException(ErrorCode.SEND_PUSH_NOTIFICATION_FAILED, "Send push notification failed", throwable);
                            });
                })
                .switchIfEmpty(Mono.fromSupplier(() -> {
                    logger.info("|PushNotificationService|sendPushNotification|no push token found for recipientId={}", request.getRecipient());
                    return "Push notifications sent successfully";
                }));
    }

    private void validatePushTokenRequest(PushTokenRegisterRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.PUSH_TOKEN_INVALID, "Request is required");
        }
        if (request.userId() == null || request.userId().isBlank()) {
            throw new AppException(ErrorCode.PUSH_TOKEN_INVALID, "userId is required");
        }
        if (request.deviceToken() == null || request.deviceToken().isBlank()) {
            throw new AppException(ErrorCode.PUSH_TOKEN_INVALID, "deviceToken is required");
        }
    }

    private Mono<NotificationPushToken> updatePushToken(NotificationPushToken existing, String deviceId, String deviceToken) {
        existing.setDeviceId(deviceId);
        existing.setDeviceToken(deviceToken);

        return userPushNotificationRepository.save(existing)
                .doOnSuccess(saved -> logger.info("|PushNotificationService|updatePushToken|success|userId={}|tokenId={}",
                        saved.getUserId(), saved.getId()));
    }

    private Mono<NotificationPushToken> createPushToken(String userId, String deviceId, String deviceToken) {
        NotificationPushToken pushToken = NotificationPushToken.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .deviceId(deviceId)
                .deviceToken(deviceToken)
                .createdAt(Instant.now())
                .build();

        return r2dbcEntityTemplate.insert(NotificationPushToken.class)
                .using(pushToken)
                .doOnSuccess(saved -> logger.info("|PushNotificationService|createPushToken|success|userId={}|tokenId={}",
                        saved.getUserId(), saved.getId()));
    }

    private PushTokenRegisterResponse toRegisterResponse(NotificationPushToken pushToken) {
        return new PushTokenRegisterResponse(
                pushToken.getId(),
                pushToken.getUserId(),
                pushToken.getDeviceId(),
                pushToken.getCreatedAt()
        );
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
