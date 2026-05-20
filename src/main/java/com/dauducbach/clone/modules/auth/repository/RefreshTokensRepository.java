package com.dauducbach.clone.modules.auth.repository;

import com.dauducbach.clone.modules.auth.entity.RefreshTokens;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Repository
public interface RefreshTokensRepository extends ReactiveCrudRepository<RefreshTokens, String> {

    @Modifying
    @Transactional
    @Query("UPDATE refresh_tokens " +
            "SET revoked = true " +
            "WHERE token_hash = :tokenHash AND device_info = :deviceInfo AND revoked = false")
    Mono<Integer> checkAndRevokedAnyActiveRefreshTokenOnThisDevice(String tokenHash, String deviceInfo);

    @Query("""
        SELECT * FROM refresh_tokens 
        WHERE token_hash = :tokenHash 
            AND device_info = :deviceInfo 
            AND expired_time > NOW(6) 
            AND revoked = false
    """)
    Mono<RefreshTokens> getCurrentValidToken(String tokenHash, String deviceInfo);

    @Modifying
    @Transactional
    @Query("UPDATE refresh_tokens SET revoked = true WHERE user_id = :userId AND revoked = false")
    Mono<Integer> revokeAllActiveRefreshTokensByUserId(String userId);
}
