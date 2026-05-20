package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import com.dauducbach.clone.modules.auth.entity.RefreshTokens;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.user.DefaultOAuth2User;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.server.WebFilterExchange;
import org.springframework.security.web.server.authentication.ServerAuthenticationSuccessHandler;
import org.springframework.stereotype.Service;
import org.springframework.http.server.reactive.ServerHttpResponse;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE,  makeFinal = true)

public class SocialLoginSuccessHandler implements ServerAuthenticationSuccessHandler {
    private static final Logger logger = LoggerFactory.getLogger(SocialLoginSuccessHandler.class);
    private static final String FRONT_END_LOGIN_PAGE = "http://localhost:5000/login";

    R2dbcEntityTemplate r2dbcEntityTemplate;
    UserCredentialsRepository userCredentialsRepository;
    JwtService jwtService;
    AuthCookieService authCookieService;

    @Override
    public Mono<Void> onAuthenticationSuccess(WebFilterExchange webFilterExchange, Authentication authentication) {
        logger.info("|SocialLoginSuccessHandler|loginSuccess");

        /// Extract provider and providerId from authentication
        var oauthToken = (OAuth2AuthenticationToken) authentication;
        var principal = (DefaultOAuth2User) oauthToken.getPrincipal();

        String provider = oauthToken.getAuthorizedClientRegistrationId();
        String providerId = extractProviderId(principal, provider);

        /// Find user by providerId and generate JWT token
        return userCredentialsRepository.findByProviderId(providerId)
                .doOnSuccess(userCredentials -> logger.info("|SocialLoginSuccessHandler|findUserCredentialsByProviderIdSuccess|providerId: {}|userId: {}", providerId, userCredentials.getUserId()))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.USER_NOT_FOUND)))
                .flatMap(userCredentials -> jwtService.generateToken(userCredentials)
                        .flatMap(accessToken -> {
                            /// Login and generate refresh token for user
                            String refreshToken = RefreshTokenGenerator.generateRawToken();
                            String deviceInfo = "social-login-" + UUID.randomUUID();

                            var refreshTokenEntity = RefreshTokens.builder()
                                    .id(UUID.randomUUID().toString())
                                    .userId(userCredentials.getUserId())
                                    .expiredTime(Instant.now().plus(5, ChronoUnit.DAYS))
                                    .tokenHash(RefreshTokenGenerator.sha256(refreshToken))
                                    .createdAt(Instant.now())
                                    .revoked(false)
                                    .deviceInfo(deviceInfo)
                                    .build();

                            AuthenticationResponse authResponse = AuthenticationResponse.builder()
                                    .accessToken(accessToken)
                                    .refreshToken(refreshToken)
                                    .deviceInfo(deviceInfo)
                                    .build();

                            /// Save refresh token to database
                            return r2dbcEntityTemplate.insert(RefreshTokens.class)
                                    .using(refreshTokenEntity)
                                    .doOnSuccess(refreshTokens -> logger.info("|SocialLoginSuccessHandler|saveRefreshTokensSuccess|userId: {}|refreshTokenId: {}", userCredentials.getUserId(), refreshTokens.getId()))
                                    .onErrorMap(throwable -> {
                                        logger.info("|SocialLoginSuccessHandler|saveRefreshTokensError|userId: {}|error", userCredentials.getUserId());
                                        return new AppException(ErrorCode.AUTHENTICATION_FAILED);
                                    })
                                    .flatMap(refreshTokens -> {
                                        ServerHttpResponse response = webFilterExchange.getExchange().getResponse();
                                        authCookieService.writeAuthCookies(response, authResponse);

                                        response.setStatusCode(HttpStatus.FOUND);
                                        response.getHeaders().setLocation(java.net.URI.create(FRONT_END_LOGIN_PAGE));

                                        return response.setComplete().doOnSuccess(unused ->
                                                logger.info("|SocialLoginSuccessHandler|redirectToFrontEnd|userId: {}|redirectUrl: {}", userCredentials.getUserId(), FRONT_END_LOGIN_PAGE)
                                        );
                                    });
                        }));
    }

    private String extractProviderId(OAuth2User oAuth2User, String provider) {
        return switch (provider) {
            case "google" -> oAuth2User.getAttribute("sub"); // String
            case "facebook" -> (String) Objects.requireNonNull(oAuth2User.getAttribute("id"));
            case "github" -> {
                Integer githubId = oAuth2User.getAttribute("id");
                yield githubId != null ? githubId.toString() : null;
            }
            default -> null;
        };
    }
}
