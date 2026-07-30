package com.dauducbach.clone.modules.notification.service;

import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import com.google.firebase.messaging.WebpushConfig;
import com.google.firebase.messaging.WebpushNotification;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
public class FirebaseNotificationPushGateway implements NotificationPushGateway {

    @Override
    public Mono<String> send(NotificationPushPayload payload) {
        return Mono.fromCallable(() -> FirebaseMessaging.getInstance().send(buildMessage(payload)))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Message buildMessage(NotificationPushPayload payload) {
        WebpushNotification notification = WebpushNotification.builder()
                .setTitle(payload.title())
                .setBody(payload.body())
                .setTag(payload.tag())
                .setRenotify(false)
                .build();
        WebpushConfig webpushConfig = WebpushConfig.builder()
                .putAllData(payload.data())
                .setNotification(notification)
                .build();

        return Message.builder()
                .setToken(payload.token())
                .setNotification(Notification.builder()
                        .setTitle(payload.title())
                        .setBody(payload.body())
                        .build())
                .putAllData(payload.data())
                .setWebpushConfig(webpushConfig)
                .build();
    }
}
