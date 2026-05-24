package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import lombok.RequiredArgsConstructor;
import lombok.experimental.NonFinal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JwtService {

    private static final Logger logger = LoggerFactory.getLogger(JwtService.class);
    private static final String ISSUER = "vtm.com";
    private static final String SCOPE_CLAIM = "scope";
    private static final String LOGOUT_KEY_PREFIX = "logout:";
    private static final String REVOKED_USER_KEY_PREFIX = "revoked_user:";

    private final ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    @NonFinal
    @Value("${jwt.signerKey}")
    private String signerKey;

    @NonFinal
    @Value("${jwt.valid-duration}")
    private long validDuration;

    public Mono<String> generateToken(UserCredentials userCredentials) {
        return Mono.fromSupplier(() -> {
            try {
                return createToken(userCredentials);
            } catch (JOSEException e) {
                logger.error("Cannot create token: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    public Mono<Boolean> verifyToken(String token) {
        return Mono.defer(() -> {
            try {
                SignedJWT signedJWT = SignedJWT.parse(token);
                JWSVerifier jwsVerifier = new MACVerifier(signerKey.getBytes(StandardCharsets.UTF_8));

                if (!signedJWT.verify(jwsVerifier)) {
                    return Mono.error(new JwtException("Invalid signature"));
                }

                Date expiryTime = signedJWT.getJWTClaimsSet().getExpirationTime();
                if (expiryTime == null || expiryTime.before(new Date())) {
                    return Mono.error(new JwtException("Token expired"));
                }

                return reactiveRedisTemplate.hasKey(LOGOUT_KEY_PREFIX + token)
                        .flatMap(isLoggedOut -> {
                            if (Boolean.TRUE.equals(isLoggedOut)) {
                                return Mono.error(new JwtException("Token logged out"));
                            }

                            try {
                                return reactiveRedisTemplate.hasKey(REVOKED_USER_KEY_PREFIX + signedJWT.getJWTClaimsSet().getSubject())
                                        .flatMap(isUserRevoked -> {
                                            if (Boolean.TRUE.equals(isUserRevoked)) {
                                                return Mono.error(new JwtException("User revoked")).hasElement();
                                            }

                                            return Mono.just(true);
                                        });
                            } catch (ParseException e) {
                                throw new JwtException("Invalid token 1", e);
                            }
                        });
            } catch (ParseException | JOSEException e) {
                logger.error("Cannot verify token: {}", e.getMessage());
                return Mono.error(new JwtException("Invalid token 2"));
            }
        });
    }

    private String createToken(UserCredentials userCredentials) throws JOSEException {
        JWSHeader header = new JWSHeader(JWSAlgorithm.HS512);

        JWTClaimsSet jwtClaimsSet = new JWTClaimsSet.Builder()
                .subject(userCredentials.getUserId())
                .issuer(ISSUER)
                .issueTime(new Date())
                .expirationTime(new Date(Instant.now().plus(validDuration, ChronoUnit.SECONDS).toEpochMilli()))
                .jwtID(UUID.randomUUID().toString())
                .claim(SCOPE_CLAIM, buildScope(userCredentials))
                .build();

        Payload payload = new Payload(jwtClaimsSet.toJSONObject());
        JWSObject jwsObject = new JWSObject(header, payload);

        try {
            jwsObject.sign(new MACSigner(signerKey.getBytes()));
            return jwsObject.serialize();
        } catch (JOSEException e) {
            logger.error("Cannot create token: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String buildScope(UserCredentials userCredentials) {
        if (!StringUtils.hasText(userCredentials.getUserRole())) {
            return "";
        }

        return userCredentials.getUserRole().startsWith("ROLE_")
                ? userCredentials.getUserRole()
                : "ROLE_" + userCredentials.getUserRole();
    }
}

