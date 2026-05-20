package com.dauducbach.clone.modules.notification.repository;

import com.dauducbach.clone.modules.notification.entity.UserNotificationSetting;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserNotificationSettingRepository extends ReactiveCrudRepository<UserNotificationSetting, String> {

}
