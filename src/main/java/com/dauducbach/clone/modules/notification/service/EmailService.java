package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.constants.NotificationStatus;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE,  makeFinal = true)

public class EmailService {
    private static final Logger logger = LoggerFactory.getLogger(EmailService.class);
    JavaMailSender javaMailSender;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<String> sendEmail(NotificationForService request) {
        return Mono.fromCallable(() -> {
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");

            helper.setTo(request.getRecipient());
            helper.setSubject(Objects.requireNonNullElse(request.getTitle(), request.getActorId()));
            helper.setText(request.getHtmlContent(), true);

            javaMailSender.send(mimeMessage);
            logger.info("|EmailService|sendEmail|email sent successfully|recipientId={}|recipientEmail={}", request.getRecipient(), request.getRecipient());
            return request.getRecipient();
        })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(sentRecipientEmail -> persistNotification(request, sentRecipientEmail))
                .thenReturn("Emails sent successfully")
                .doOnError(e -> logger.error("|EmailService|sendEmail|error={}", e.getMessage()))
                .onErrorMap(throwable -> {
                    if (throwable instanceof AppException appException) {
                        return appException;
                    }

                    return new AppException(
                            ErrorCode.SEND_EMAIL_FAILED,
                            String.format("Send email failed for recipientId=%s", request.getRecipient()),
                            throwable
                    );
                });
    }

    private Mono<Void> persistNotification(NotificationForService request, String recipientEmail) {
        var entity = NotificationEvents.builder()
                .id(UUID.randomUUID().toString())
                .actorId(request.getActorId())
                .actionType(request.getActionType())
                .entityId(request.getEntityId())
                .entityType(request.getEntityType())
                .createdAt(Instant.now())
                .build();

        logger.info("|EmailService|sendEmail|prepare to insert NotificationEvents|recipientId={}|recipientEmail={}|eventId={}", request.getRecipient(), recipientEmail, entity.getId());

        var userNotification = UserNotifications.builder()
                .id(UUID.randomUUID().toString())
                .userId(request.getRecipient())
                .eventId(entity.getId())
                .notificationStatus(NotificationStatus.UNREAD)
                .readAt(null)
                .createdAt(Instant.now())
                .build();

        logger.info("|EmailService|sendEmail|prepare to insert UserNotifications|recipientId={}|recipientEmail={}|notificationId={}", request.getRecipient(), recipientEmail, userNotification.getId());

        return r2dbcEntityTemplate.insert(NotificationEvents.class)
                .using(entity)
                .doOnSuccess(e -> logger.info("|EmailService|sendEmail|insert NotificationEvents success|recipientId={}|recipientEmail={}|eventId={}", request.getRecipient(), recipientEmail, entity.getId()))
                .then(r2dbcEntityTemplate.insert(UserNotifications.class)
                        .using(userNotification)
                        .doOnSuccess(e -> logger.info("|EmailService|sendEmail|insert UserNotifications success|recipientId={}|recipientEmail={}|notificationId={}", request.getRecipient(), recipientEmail, userNotification.getId())))
                .then();
    }
}
