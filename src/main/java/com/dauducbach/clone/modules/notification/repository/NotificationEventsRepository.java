package com.dauducbach.clone.modules.notification.repository;

import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface NotificationEventsRepository extends ReactiveCrudRepository<NotificationEvents, String> {
    Mono<NotificationEvents> findByDedupKey(String dedupKey);
}
