package com.dauducbach.clone.modules.notification.service;

import com.google.firebase.FirebaseApp;
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
        return Mono.fromCallable(() -> {
                    if (FirebaseApp.getApps().isEmpty()) {
                        throw new IllegalStateException(
                                "Firebase push is not initialized. Set FIREBASE_ENABLED=true and configure "
                                        + "FIREBASE_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS.");
                    }
                    return FirebaseMessaging.getInstance().send(buildMessage(payload));
                })
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
