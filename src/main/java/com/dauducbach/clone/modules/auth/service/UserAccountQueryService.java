package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserAccountQueryService {
    private final UserCredentialsRepository userCredentialsRepository;

    public Mono<Boolean> exists(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(false);
        }
        return userCredentialsRepository.existsById(userId.trim()).defaultIfEmpty(false);
    }

    public Mono<SessionIdentity> getSessionIdentity(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        return userCredentialsRepository.findById(userId.trim())
                .map(credentials -> new SessionIdentity(
                        credentials.getUserId(),
                        credentials.getUsername()
                ));
    }

    public record SessionIdentity(String userId, String username) {
    }
}
