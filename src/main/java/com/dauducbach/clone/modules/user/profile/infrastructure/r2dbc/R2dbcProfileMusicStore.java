package com.dauducbach.clone.modules.user.profile.infrastructure.r2dbc;

import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.profile.application.ProfileMusicStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class R2dbcProfileMusicStore implements ProfileMusicStore {
    private final R2dbcEntityTemplate entityTemplate;

    @Override
    public Mono<UserMusics> insert(UserMusics userMusic) {
        return entityTemplate.insert(UserMusics.class).using(userMusic);
    }
}
