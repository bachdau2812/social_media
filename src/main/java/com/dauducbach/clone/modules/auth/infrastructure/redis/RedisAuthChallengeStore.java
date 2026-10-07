package com.dauducbach.clone.modules.auth.infrastructure.redis;

import com.dauducbach.clone.modules.auth.recovery.RecoveryChallengeStore;
import com.dauducbach.clone.modules.auth.registration.RegistrationChallengeStore;
import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisAuthChallengeStore implements RegistrationChallengeStore, RecoveryChallengeStore {
    private static final String PENDING_REGISTRATION_PREFIX = "user_registration:";
    private static final String REGISTRATION_CODE_PREFIX = "registration_verify:";
    private static final String RECOVERY_CODE_PREFIX = "forget_password:";

    private final ReactiveRedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> saveDraft(String email, RegistrationDraft draft, Duration ttl) {
        return redisTemplate.opsForValue().set(PENDING_REGISTRATION_PREFIX + email, draft, ttl).then();
    }

    @Override
    public Mono<RegistrationDraft> findDraft(String email) {
        return redisTemplate.opsForValue().get(PENDING_REGISTRATION_PREFIX + email)
                .map(value -> objectMapper.convertValue(value, RegistrationDraft.class));
    }

    @Override
    public Mono<Void> saveRegistrationCode(String email, String code, Duration ttl) {
        return redisTemplate.opsForValue().set(REGISTRATION_CODE_PREFIX + email, code, ttl).then();
    }

    @Override
    public Mono<String> findRegistrationCode(String email) {
        return redisTemplate.opsForValue().get(REGISTRATION_CODE_PREFIX + email)
                .map(String::valueOf);
    }

    @Override
    public Mono<Void> saveRecoveryCode(String email, String code, Duration ttl) {
        return redisTemplate.opsForValue().set(RECOVERY_CODE_PREFIX + email, code, ttl).then();
    }

    @Override
    public Mono<String> findRecoveryCode(String email) {
        return redisTemplate.opsForValue().get(RECOVERY_CODE_PREFIX + email)
                .map(String::valueOf);
    }
}
