package com.dauducbach.clone.modules.notification.infrastructure.persistence;

import com.dauducbach.clone.modules.notification.delivery.NotificationPersistence;
import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import com.dauducbach.clone.modules.notification.repository.NotificationEventsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Repository
public class R2dbcNotificationPersistenceAdapter implements NotificationPersistence {
    private static final Logger log = LoggerFactory.getLogger(R2dbcNotificationPersistenceAdapter.class);

    private final NotificationEventsRepository eventRepository;
    private final R2dbcEntityTemplate entityTemplate;
    private final TransactionalOperator transactions;

    public R2dbcNotificationPersistenceAdapter(
            NotificationEventsRepository eventRepository,
            R2dbcEntityTemplate entityTemplate,
            TransactionalOperator transactions
    ) {
        this.eventRepository = eventRepository;
        this.entityTemplate = entityTemplate;
        this.transactions = transactions;
    }

    @Override
    public Mono<Boolean> persistIfNew(NotificationEvents event, UserNotifications recipient) {
        boolean sourceEventKeyPresent = hasValue(event.getSourceEventKey());
        boolean durableDedup = sourceEventKeyPresent || hasDurableDedupKey(event.getDedupKey());
        Mono<Boolean> duplicateCheck = sourceEventKeyPresent
                ? eventRepository.findBySourceEventKey(event.getSourceEventKey()).hasElement()
                    .flatMap(exists -> exists ? Mono.just(true)
                            : eventRepository.findByDedupKey(event.getDedupKey()).hasElement())
                : durableDedup
                    ? eventRepository.findByDedupKey(event.getDedupKey()).hasElement()
                    : Mono.just(false);

        return duplicateCheck
                .flatMap(exists -> exists ? Mono.just(false)
                        : transactions.transactional(insertEventAndRecipient(event, recipient)))
                .onErrorResume(DataIntegrityViolationException.class, error -> {
                    if (!durableDedup) {
                        return Mono.error(error);
                    }
                    return duplicateExists(event)
                            .flatMap(exists -> exists
                                    ? Mono.fromRunnable(() -> log.info(
                                            "|R2dbcNotificationPersistenceAdapter|duplicate skipped|dedupKey={}",
                                            event.getDedupKey())).thenReturn(false)
                                    : Mono.error(error));
                });
    }

    private Mono<Boolean> insertEventAndRecipient(
            NotificationEvents event,
            UserNotifications recipient
    ) {
        return entityTemplate.insert(NotificationEvents.class)
                .using(event)
                .doOnSuccess(saved -> log.info(
                        "|R2dbcNotificationPersistenceAdapter|event saved|eventId={}", saved.getId()))
                .then(Mono.defer(() -> entityTemplate.insert(UserNotifications.class)
                        .using(recipient)
                        .doOnSuccess(saved -> log.info(
                                "|R2dbcNotificationPersistenceAdapter|recipient linked|recipientId={}|notificationId={}",
                                saved.getUserId(), saved.getId()))))
                .thenReturn(true);
    }

    private Mono<Boolean> duplicateExists(NotificationEvents event) {
        Mono<Boolean> sourceEventDuplicate = hasValue(event.getSourceEventKey())
                ? eventRepository.findBySourceEventKey(event.getSourceEventKey()).hasElement()
                : Mono.just(false);
        return sourceEventDuplicate.flatMap(exists -> exists
                ? Mono.just(true)
                : eventRepository.findByDedupKey(event.getDedupKey()).hasElement());
    }

    private boolean hasDurableDedupKey(String dedupKey) {
        return dedupKey != null
                && (dedupKey.startsWith("UP_STORY:")
                || dedupKey.startsWith("AVATAR_UPDATE_UPLOAD:")
                || dedupKey.startsWith("LIKE_STORY:"));
    }

    private boolean hasValue(String value) {
        return value != null && !value.isBlank();
    }
}
