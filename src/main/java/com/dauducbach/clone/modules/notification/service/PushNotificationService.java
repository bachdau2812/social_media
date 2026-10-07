package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.modules.notification.delivery.DeliverNotificationUseCase;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class PushNotificationService {
    private final DeliverNotificationUseCase deliverNotification;

    public Mono<String> sendPushNotification(NotificationForService request) {
        return Mono.defer(() -> deliverNotification.send(request));
    }
}