package com.dauducbach.clone.modules.auth.infrastructure.persistence;

import com.dauducbach.clone.modules.auth.entity.RefreshTokens;
import com.dauducbach.clone.modules.auth.repository.RefreshTokensRepository;
import com.dauducbach.clone.modules.auth.sessions.RefreshSession;
import com.dauducbach.clone.modules.auth.sessions.RefreshSessionStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class R2dbcRefreshSessionStore implements RefreshSessionStore {
    private final RefreshTokensRepository repository;
    private final R2dbcEntityTemplate entityTemplate;

    @Override
    public Mono<RefreshSession> findCurrentValid(String tokenHash, String deviceInfo) {
        return repository.getCurrentValidToken(tokenHash, deviceInfo)
                .map(R2dbcRefreshSessionStore::toSession);
    }

    @Override
    public Mono<Integer> revokeCurrent(String tokenHash, String deviceInfo) {
        return repository.checkAndRevokedAnyActiveRefreshTokenOnThisDevice(tokenHash, deviceInfo);
    }

    @Override
    public Mono<Integer> revokeAllForUser(String userId) {
        return repository.revokeAllActiveRefreshTokensByUserId(userId);
    }

    @Override
    public Mono<Void> save(RefreshSession session) {
        return entityTemplate.insert(RefreshTokens.class)
                .using(toEntity(session))
                .then();
    }

    private static RefreshSession toSession(RefreshTokens entity) {
        return new RefreshSession(
                entity.getId(),
                entity.getUserId(),
                entity.getTokenHash(),
                entity.getExpiredTime(),
                entity.getCreatedAt(),
                entity.getDeviceInfo());
    }

    private static RefreshTokens toEntity(RefreshSession session) {
        return RefreshTokens.builder()
                .id(session.id())
                .userId(session.userId())
                .tokenHash(session.tokenHash())
                .expiredTime(session.expiresAt())
                .createdAt(session.createdAt())
                .deviceInfo(session.deviceInfo())
                .revoked(false)
                .build();
    }
}
