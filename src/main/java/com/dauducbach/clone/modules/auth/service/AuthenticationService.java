package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.dto.request.IntrospectRequest;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import com.dauducbach.clone.modules.auth.dto.request.LoginRequest;
import com.dauducbach.clone.modules.auth.dto.request.LogoutRequest;
import com.dauducbach.clone.modules.auth.dto.request.RefreshTokenRequest;
import com.dauducbach.clone.modules.auth.dto.response.IntrospectResponse;
import com.dauducbach.clone.modules.auth.entity.RefreshTokens;
import com.dauducbach.clone.modules.auth.repository.RefreshTokensRepository;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE,  makeFinal = true)

public class AuthenticationService {
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationService.class);

    private static final Integer REVOKE_ACCESS_TIME = 15;

    JwtService jwtService;
    UserCredentialsRepository userCredentialsRepository;
    RefreshTokensRepository refreshTokensRepository;
    PasswordEncoder passwordEncoder;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> redisTemplate;

    public Mono<AuthenticationResponse> login(LoginRequest loginRequest) {
        logger.info("|AuthenticationService|login|request={}", loginRequest);

        return userCredentialsRepository.findByUsername(loginRequest.getUsername())
            .switchIfEmpty(Mono.error(new AppException(ErrorCode.USER_NOT_FOUND)))
            .flatMap(userCredentials -> {
                if (!passwordEncoder.matches(loginRequest.getPassword(), userCredentials.getUserPassword())) {
                    logger.info("|AuthenticationService|login|password incorrect for userId={}", userCredentials.getUserId());
                    return Mono.error(new AppException(ErrorCode.PASSWORD_INCORRECT));
                }

                logger.info("|AuthenticationService|login|valid password for userId={}", userCredentials.getUserId());

                return jwtService.generateToken(userCredentials)
                        .flatMap(accessToken -> {
                            String refreshToken = RefreshTokenGenerator.generateRawToken();

                            var refreshTokenEntity = RefreshTokens.builder()
                                    .id(UUID.randomUUID().toString())
                                    .userId(userCredentials.getUserId())
                                    .tokenHash(RefreshTokenGenerator.sha256(refreshToken))
                                    .expiredTime(Instant.now().plus(5, ChronoUnit.DAYS))
                                    .revoked(false)
                                    .deviceInfo(loginRequest.getDeviceInfo())
                                    .createdAt(Instant.now())
                                    .build();

                            var response = AuthenticationResponse.builder()
                                    .accessToken(accessToken)
                                    .refreshToken(refreshToken)
                                    .deviceInfo(loginRequest.getDeviceInfo())
                                    .build();

                            return r2dbcEntityTemplate.insert(RefreshTokens.class)
                                    .using(refreshTokenEntity)
                                    .doOnSuccess(savedToken -> logger.info("|AuthenticationService|login|refresh token saved for userId={}, refreshTokenId={}", userCredentials.getUserId(), savedToken.getId()))
                                    .onErrorMap(e -> {
                                        logger.error("|AuthenticationService|login|failed to save refresh token for userId={}, error={}", userCredentials.getUserId(), e.getMessage());
                                        return new AppException(ErrorCode.AUTHENTICATION_FAILED);
                                    })
                                    .thenReturn(response);
                        });
            });
    }

    public Mono<AuthenticationResponse> refreshToken(RefreshTokenRequest request) {
        logger.info("|AuthenticationService|refreshToken|request={}", request);

        String tokenHash = RefreshTokenGenerator.sha256(request.getRefreshToken());

        return refreshTokensRepository.getCurrentValidToken(tokenHash, request.getDeviceInfo())
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.REFRESH_TOKEN_INVALID, "Refresh token is invalid or expired. Please login again.")))
                .flatMap(refreshTokenEntity -> {
                    logger.info("|AuthenticationService|refreshToken|valid refresh token found for userId={}, refreshTokenId={}", refreshTokenEntity.getUserId(), refreshTokenEntity.getId());

                    return userCredentialsRepository.findById(refreshTokenEntity.getUserId())
                            .doOnError(e -> logger.error("|AuthenticationService|refreshToken|failed to get user credentials for userId={}, error={}", refreshTokenEntity.getUserId(), e.getMessage()))
                            .switchIfEmpty(Mono.error(new AppException(ErrorCode.USER_NOT_FOUND, "User not found. Please login again.")))
                            .flatMap(userCredentials -> refreshTokensRepository.checkAndRevokedAnyActiveRefreshTokenOnThisDevice(tokenHash, request.getDeviceInfo())
                                    .flatMap(revokedCount -> {
                                        if (revokedCount == 0) {
                                            logger.info("|AuthenticationService|refreshToken|refresh token not found or already revoked for tokenHash={}, deviceInfo={}", tokenHash, request.getDeviceInfo());
                                            return Mono.error(new AppException(ErrorCode.REFRESH_TOKEN_INVALID, "Refresh token is no longer valid. Please login again."));
                                        }

                                        logger.info("|AuthenticationService|refreshToken|refresh token revoked for userId={}, tokenHash={}, deviceInfo={}", userCredentials.getUserId(), tokenHash, request.getDeviceInfo());

                                        return jwtService.generateToken(userCredentials)
                                                .flatMap(accessToken -> {
                                                    logger.info("|AuthenticationService|refreshToken|new access token generated for userId={}", userCredentials.getUserId());

                                                    String newToken = RefreshTokenGenerator.generateRawToken();

                                                    var newRefreshToken = RefreshTokens.builder()
                                                            .id(UUID.randomUUID().toString())
                                                            .userId(userCredentials.getUserId())
                                                            .tokenHash(RefreshTokenGenerator.sha256(newToken))
                                                            .expiredTime(Instant.now().plus(5, ChronoUnit.DAYS))
                                                            .revoked(false)
                                                            .deviceInfo(request.getDeviceInfo())
                                                            .createdAt(Instant.now())
                                                            .build();

                                                    return r2dbcEntityTemplate.insert(RefreshTokens.class)
                                                            .using(newRefreshToken)
                                                            .doOnSuccess(savedToken -> logger.info("|AuthenticationService|refreshToken|new refresh token saved for userId={}, refreshTokenId={}", userCredentials.getUserId(), savedToken.getId()))
                                                            .map(savedToken -> AuthenticationResponse.builder()
                                                                    .accessToken(accessToken)
                                                                    .refreshToken(newToken)
                                                                    .deviceInfo(request.getDeviceInfo())
                                                                    .build());
                                                });
                                    })
                                    .onErrorResume(error -> revokeAndFailRefresh(userCredentials.getUserId(), error))
                            );
                });
    }

    public Mono<String> logout(LogoutRequest logoutRequest) {
        logger.info("|AuthenticationService|logout|request={}", logoutRequest);

        String accessToken = logoutRequest.getAccessToken();
        String tokenHash = RefreshTokenGenerator.sha256(logoutRequest.getRefreshToken());

        /// Revoke access token by pushing to redis with key "logout:{token}" and set expired time same as token's expired time
        return redisTemplate.opsForValue().set("logout:" + accessToken, "REVOKED", REVOKE_ACCESS_TIME)
                .doOnError(e -> logger.error("|AuthenticationService|logout|failed to revoke access token in redis for accessToken={}, error={}", accessToken, e.getMessage()))
                .onErrorMap(success -> {
                    logger.info("|AuthenticationService|logout|access token revoked in) redis for accessToken={}", accessToken);

                    return new AppException(ErrorCode.LOGOUT_FAILED);
                })
                /// Revoke refresh token
                .then(refreshTokensRepository.checkAndRevokedAnyActiveRefreshTokenOnThisDevice(tokenHash, logoutRequest.getDeviceInfo()))
                .thenReturn("Logout successful");
    }

    public Mono<IntrospectResponse> introspect(IntrospectRequest introspectRequest) {
        logger.info("|AuthenticationService|introspect|request={}", introspectRequest);

        return jwtService.verifyToken(introspectRequest.getAccessToken())
                .doOnSuccess(claimsSet -> logger.info("|AuthenticationService|introspect|check successful"))
                .onErrorResume(e -> {
                    logger.error("|AuthenticationService|introspect|check failed for accessToken={}, error={}", introspectRequest.getAccessToken(), e.getMessage());
                    return Mono.just(false);
                })
                .map(valid -> IntrospectResponse.builder()
                        .valid(valid)
                        .build());
    }

    private Mono<AuthenticationResponse> revokeAndFailRefresh(String userId, Throwable error) {
        logger.error("|AuthenticationService|refreshToken|refresh failed for userId={}, error={}", userId, error.getMessage(), error);

        return refreshTokensRepository.revokeAllActiveRefreshTokensByUserId(userId)
                .doOnSuccess(revokedCount -> logger.info("|AuthenticationService|refreshToken|revoked active refresh tokens for userId={}, revokedCount={}", userId, revokedCount))
                .doOnError(revokeError -> logger.error("|AuthenticationService|refreshToken|failed to revoke active refresh tokens for userId={}, error={}", userId, revokeError.getMessage(), revokeError))
                .onErrorResume(revokeError -> Mono.empty())
                .then(Mono.error(new AppException(
                        ErrorCode.REFRESH_TOKEN_FAILED,
                        "Failed to refresh token. Your session has been revoked. Please login again.",
                        error
                )));
    }
}
