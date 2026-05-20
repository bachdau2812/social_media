package com.dauducbach.clone.modules.notification.repository;

import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserNotificationRepository extends ReactiveCrudRepository<UserNotifications, Long> {
}
