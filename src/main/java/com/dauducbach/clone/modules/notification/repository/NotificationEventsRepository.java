package com.dauducbach.clone.modules.notification.repository;

import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface NotificationEventsRepository extends ReactiveCrudRepository<NotificationEvents, String> {
}
