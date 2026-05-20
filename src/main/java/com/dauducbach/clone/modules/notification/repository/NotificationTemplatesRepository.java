package com.dauducbach.clone.modules.notification.repository;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface NotificationTemplatesRepository extends ReactiveCrudRepository<NotificationTemplates, String> {
    Mono<NotificationTemplates> findByActionType(UserActionType actionType);
}
