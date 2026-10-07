package com.dauducbach.clone.modules.notification.infrastructure.persistence;

import com.dauducbach.clone.modules.notification.delivery.NotificationPushTokenQuery;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public class R2dbcNotificationPushTokenQueryAdapter implements NotificationPushTokenQuery {
    private final UserPushNotificationRepository tokenRepository;

    public R2dbcNotificationPushTokenQueryAdapter(UserPushNotificationRepository tokenRepository) {
        this.tokenRepository = tokenRepository;
    }

    @Override
    public Mono<String> findDeviceToken(String userId) {
        return tokenRepository.findByUserId(userId)
                .map(token -> token.getDeviceToken() == null ? "" : token.getDeviceToken())
                .filter(value -> !value.isBlank());
    }
}
