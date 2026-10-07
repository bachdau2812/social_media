package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Temporary facade retained for existing post publishers while they adopt the common transport contract. */
@Service
@RequiredArgsConstructor
public class PostSseService {
    private final UserSsePublisher publisher;

    public Mono<Void> sendToUser(String userId, String event, String data) {
        return publisher.sendToUser(userId, event, data);
    }
}
