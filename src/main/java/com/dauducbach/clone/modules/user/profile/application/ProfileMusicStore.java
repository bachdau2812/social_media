package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.modules.user.entity.UserMusics;
import reactor.core.publisher.Mono;

public interface ProfileMusicStore {
    Mono<UserMusics> insert(UserMusics userMusic);
}
