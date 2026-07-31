package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.constants.NotificationStatus;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import com.dauducbach.clone.modules.notification.repository.NotificationEventsRepository;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

final class NotificationPersistenceFlow {
    private static final Logger log = LoggerFactory.getLogger(NotificationPersistenceFlow.class);

    private final UserPushNotificationRepository tokenRepository;
    private final NotificationEventsRepository eventRepository;
    private final R2dbcEntityTemplate entityTemplate;
    private final NotificationPushGateway pushGateway;
    private final NotificationDestinationBuilder destinationBuilder;
    private final NotificationContentNormalizer contentNormalizer;
    private final NotificationMetadataCodec metadataCodec;
    private final FirebasePushMessageFactory pushMessageFactory;

    NotificationPersistenceFlow(
            UserPushNotificationRepository tokenRepository,
            NotificationEventsRepository eventRepository,
            R2dbcEntityTemplate entityTemplate,
            NotificationPushGateway pushGateway,
            NotificationDestinationBuilder destinationBuilder,
            NotificationContentNormalizer contentNormalizer,
            NotificationMetadataCodec metadataCodec,
            FirebasePushMessageFactory pushMessageFactory
    ) {
        this.tokenRepository = tokenRepository;
        this.eventRepository = eventRepository;
        this.entityTemplate = entityTemplate;
        this.pushGateway = pushGateway;
        this.destinationBuilder = destinationBuilder;
        this.contentNormalizer = contentNormalizer;
        this.metadataCodec = metadataCodec;
        this.pushMessageFactory = pushMessageFactory;
    }

    Mono<String> send(NotificationForService request) {
        validateRecipient(request);
        PreparedPushNotification prepared = prepare(request);
        return persist(prepared.event(), prepared.userNotification())
                .flatMap(inserted -> inserted
                        ? deliver(request, prepared)
                        : Mono.just("Duplicate notification skipped"))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.SEND_PUSH_NOTIFICATION_FAILED,
                                "Persist notification failed",
                                error));
    }

    private PreparedPushNotification prepare(NotificationForService request) {
        Instant createdAt = Instant.now();
        String eventId = UUID.randomUUID().toString();
        String notificationId = UUID.randomUUID().toString();
        String deepLink = firstNonBlank(request.getDeepLink(), destinationBuilder.build(request), "/");
        String content = contentNormalizer.normalize(request);
        String dedupKey = resolveDedupKey(request);
        request.setHtmlContent(content);
        request.setDeepLink(deepLink);
        request.setDedupKey(dedupKey);

        NotificationEvents event = NotificationEvents.builder()
                .id(eventId)
                .actorId(request.getActorId())
                .actionType(request.getActionType())
                .entityId(request.getEntityId())
                .entityType(request.getEntityType())
                .content(content)
                .metadata(metadataCodec.encode(request.getMetadata()))
                .deepLink(deepLink)
                .dedupKey(dedupKey)
                .createdAt(createdAt)
                .build();
        UserNotifications userNotification = UserNotifications.builder()
                .id(notificationId)
                .userId(request.getRecipient())
                .eventId(eventId)
                .notificationStatus(NotificationStatus.UNREAD)
                .createdAt(createdAt)
                .build();
        return new PreparedPushNotification(event, userNotification, deepLink, dedupKey);
    }

    private Mono<Boolean> persist(NotificationEvents event, UserNotifications userNotification) {
        boolean durableDedup = hasDurableDedupKey(event.getDedupKey());
        Mono<Boolean> duplicateCheck = durableDedup
                ? eventRepository.findByDedupKey(event.getDedupKey()).hasElement()
                : Mono.just(false);
        return duplicateCheck
                .flatMap(exists -> exists ? Mono.just(false) : entityTemplate.insert(NotificationEvents.class)
                .using(event)
                .doOnSuccess(saved -> log.info(
                        "|NotificationPersistenceFlow|persist|event saved|eventId={}",
                        saved.getId()))
                .then(entityTemplate.insert(UserNotifications.class)
                        .using(userNotification)
                        .doOnSuccess(saved -> log.info(
                                "|NotificationPersistenceFlow|persist|recipient linked|recipientId={}|notificationId={}",
                                saved.getUserId(), saved.getId()))
                        .onErrorResume(error -> eventRepository.deleteById(event.getId())
                                .then(Mono.error(error))))
                .thenReturn(true))
                .onErrorResume(DataIntegrityViolationException.class, error -> {
                    if (!durableDedup) return Mono.error(error);
                    log.info("|NotificationPersistenceFlow|persist|duplicate skipped|dedupKey={}", event.getDedupKey());
                    return Mono.just(false);
                });
    }

    private boolean hasDurableDedupKey(String dedupKey) {
        return dedupKey != null
                && (dedupKey.startsWith("UP_STORY:")
                || dedupKey.startsWith("LIKE_STORY:"));
    }

    private Mono<String> deliver(
            NotificationForService request,
            PreparedPushNotification prepared
    ) {
        return tokenRepository.findByUserId(request.getRecipient())
                .filter(token -> token.getDeviceToken() != null)
                .filter(token -> !token.getDeviceToken().isBlank())
                .flatMap(token -> pushGateway.send(pushMessageFactory.create(
                                token.getDeviceToken(),
                                prepared.userNotification().getId(),
                                prepared.dedupKey(),
                                prepared.deepLink(),
                                request))
                        .doOnSuccess(messageId -> log.info(
                                "|NotificationPersistenceFlow|deliver|sent|recipientId={}|messageId={}",
                                request.getRecipient(), messageId))
                        .thenReturn("Notification persisted and push sent")
                        .onErrorResume(error -> {
                            log.error(
                                    "|NotificationPersistenceFlow|deliver|failed|recipientId={}|error={}",
                                    request.getRecipient(), error.getMessage());
                            return Mono.just("Notification persisted; push delivery failed");
                        }))
                .switchIfEmpty(Mono.fromSupplier(() -> {
                    log.info(
                            "|NotificationPersistenceFlow|deliver|no token|recipientId={}",
                            request.getRecipient());
                    return "Notification persisted; no push token";
                }));
    }

    private void validateRecipient(NotificationForService request) {
        if (request == null) {
            throw new AppException(
                    ErrorCode.SEND_PUSH_NOTIFICATION_FAILED,
                    "Notification request is missing");
        }
        if (request.getRecipient() == null || request.getRecipient().isBlank()) {
            throw new AppException(
                    ErrorCode.SEND_PUSH_NOTIFICATION_FAILED,
                    "Recipient userId is missing");
        }
    }

    private String resolveDedupKey(NotificationForService request) {
        String metadataEventId = metadataValue(request.getMetadata(), "EVENT_ID");
        String explicit = firstNonBlank(
                request.getDedupKey(),
                metadataEventId);
        if (!explicit.isBlank()) {
            String recipientSuffix = ":" + request.getRecipient();
            return explicit.endsWith(recipientSuffix) ? explicit : explicit + recipientSuffix;
        }
        return String.join(":",
                request.getActionType() == null ? "NOTIFICATION" : request.getActionType().name(),
                request.getRecipient(),
                firstNonBlank(request.getEntityId(), "none"));
    }

    private String metadataValue(Map<String, String> metadata, String key) {
        if (metadata == null) {
            return "";
        }
        return metadata.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().equalsIgnoreCase(key))
                .map(Map.Entry::getValue)
                .filter(value -> value != null && !value.isBlank())
                .findFirst()
                .orElse("");
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }
}
