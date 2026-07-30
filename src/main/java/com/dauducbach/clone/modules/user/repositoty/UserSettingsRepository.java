package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserSettings;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserSettingsRepository extends ReactiveCrudRepository<UserSettings, String> {
}