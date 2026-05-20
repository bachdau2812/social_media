package com.dauducbach.clone.modules.notification.repository;

import com.dauducbach.clone.modules.notification.entity.NotificationPushToken;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface UserPushNotificationRepository extends ReactiveCrudRepository<NotificationPushToken, String> {
    Mono<NotificationPushToken> findByUserId(String userId);
}
