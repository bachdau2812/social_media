package com.dauducbach.clone.modules.auth.infrastructure.jwt;

import com.dauducbach.clone.modules.auth.sessions.AccessTokenRevocationStore;
import com.dauducbach.clone.modules.auth.sessions.AuthTokenProvider;
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
public class JwtService implements AuthTokenProvider {

    private static final Logger logger = LoggerFactory.getLogger(JwtService.class);
    private static final String ISSUER = "vtm.com";
    private static final String SCOPE_CLAIM = "scope";
    private final AccessTokenRevocationStore tokenRevocationStore;

    @NonFinal
    @Value("${jwt.signerKey}")
    private String signerKey;

    @NonFinal
    @Value("${jwt.valid-duration}")
    private long validDuration;

    public Mono<String> generateToken(UserCredentials userCredentials) {
        return issueAccessToken(userCredentials.getUserId(), userCredentials.getUserRole());
    }

    @Override
    public Mono<String> issueAccessToken(String userId, String role) {
        return Mono.fromSupplier(() -> {
            try {
                return createToken(userId, role);
            } catch (JOSEException e) {
                logger.error("Cannot create token|errorType={}", e.getClass().getSimpleName());
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

                return tokenRevocationStore.isRevoked(token)
                        .flatMap(isLoggedOut -> {
                            if (Boolean.TRUE.equals(isLoggedOut)) {
                                return Mono.error(new JwtException("Token logged out"));
                            }

                            try {
                                return tokenRevocationStore.isUserRevoked(signedJWT.getJWTClaimsSet().getSubject())
                                        .flatMap(isUserRevoked -> Boolean.TRUE.equals(isUserRevoked)
                                                ? Mono.error(new JwtException("User revoked"))
                                                : Mono.just(true));
                            } catch (ParseException e) {
                                throw new JwtException("Invalid token", e);
                            }
                        });
            } catch (ParseException | JOSEException e) {
                logger.warn("Token verification failed|errorType={}", e.getClass().getSimpleName());
                return Mono.error(new JwtException("Invalid token 2"));
            }
        });
    }

    @Override
    public Mono<Boolean> verifyAccessToken(String token) {
        return verifyToken(token);
    }

    private String createToken(String userId, String role) throws JOSEException {
        JWSHeader header = new JWSHeader(JWSAlgorithm.HS512);

        JWTClaimsSet jwtClaimsSet = new JWTClaimsSet.Builder()
                .subject(userId)
                .issuer(ISSUER)
                .issueTime(new Date())
                .expirationTime(new Date(Instant.now().plus(validDuration, ChronoUnit.SECONDS).toEpochMilli()))
                .jwtID(UUID.randomUUID().toString())
                .claim(SCOPE_CLAIM, buildScope(role))
                .build();

        Payload payload = new Payload(jwtClaimsSet.toJSONObject());
        JWSObject jwsObject = new JWSObject(header, payload);

        try {
            jwsObject.sign(new MACSigner(signerKey.getBytes()));
            return jwsObject.serialize();
        } catch (JOSEException e) {
            logger.error("Cannot create token|errorType={}", e.getClass().getSimpleName());
            throw new RuntimeException(e);
        }
    }

    private String buildScope(String role) {
        if (!StringUtils.hasText(role)) {
            return "";
        }

        return role.startsWith("ROLE_")
                ? role
                : "ROLE_" + role;
    }
}

