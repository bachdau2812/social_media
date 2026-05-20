package com.dauducbach.clone.configuration;

import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.auth.service.JwtService;
import com.dauducbach.clone.modules.auth.service.SocialLoginFailService;
import com.dauducbach.clone.modules.auth.service.SocialLoginSuccessHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jwt.SignedJWT;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.config.oauth2.client.CommonOAuth2Provider;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.ServerAuthenticationEntryPoint;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import reactor.core.publisher.Mono;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

@Configuration
@EnableWebFluxSecurity
@EnableReactiveMethodSecurity

public class SecurityConfig {
    private static final String[] PUBLIC_ENDPOINT = {
            "/oauth2/authorization/**",
            "/login/oauth2/code/**",
            "/login/**",
            "/auth/login",
            "/auth/user-credentials/**",
            "/auth/introspect",
            "/auth/refresh-token",
            "/auth/logout"
    };

    private final JwtService jwtService;
    private final SocialLoginSuccessHandler socialLoginSuccessHandler;
    private final SocialLoginFailService socialLoginFailService;

    @Value("${spring.security.oauth2.client.registration.google.client-id}")
    private String googleClientId;
    @Value("${spring.security.oauth2.client.registration.google.client-secret}")
    private String googleClientSecret;
    @Value("${spring.security.oauth2.client.registration.google.redirect-uri}")
    private String googleRedirectUri;

    @Value("${spring.security.oauth2.client.registration.facebook.client-id}")
    private String facebookClientId;
    @Value("${spring.security.oauth2.client.registration.facebook.client-secret}")
    private String facebookClientSecret;
    @Value("${spring.security.oauth2.client.registration.facebook.redirect-uri}")
    private String facebookRedirectUri;

    @Value("${spring.security.oauth2.client.registration.github.client-id}")
    private String githubClientId;
    @Value("${spring.security.oauth2.client.registration.github.client-secret}")
    private String githubClientSecret;
    @Value("${spring.security.oauth2.client.registration.github.redirect-uri}")
    private String githubRedirectUri;

    public SecurityConfig(JwtService jwtService, SocialLoginSuccessHandler socialLoginSuccessHandler, SocialLoginFailService socialLoginFailService) {
        this.jwtService = jwtService;
        this.socialLoginSuccessHandler = socialLoginSuccessHandler;
        this.socialLoginFailService = socialLoginFailService;
    }

    @Bean
    public SecurityWebFilterChain filterChain() {
        ServerHttpSecurity serverHttpSecurity = ServerHttpSecurity.http();

        serverHttpSecurity.csrf(ServerHttpSecurity.CsrfSpec::disable);
        serverHttpSecurity.cors(cors -> cors.configurationSource(corsConfigurationSource()));

        serverHttpSecurity.exceptionHandling(exceptionHandling -> exceptionHandling
                .authenticationEntryPoint(authenticationEntryPoint())
        );

        serverHttpSecurity.authorizeExchange(authorizeExchangeSpec -> authorizeExchangeSpec
                .pathMatchers(PUBLIC_ENDPOINT).permitAll()
                .anyExchange().authenticated()
        );

        serverHttpSecurity.oauth2ResourceServer(oAuth2ResourceServerSpec -> oAuth2ResourceServerSpec
                .jwt(jwtSpec -> jwtSpec
                        .jwtDecoder(reactiveJwtDecoder())
                        .jwtAuthenticationConverter(jwtAuthenticationConverter())
                )
        );

        serverHttpSecurity.oauth2Login(oauth2 -> oauth2
                .clientRegistrationRepository(reactiveClientRegistrationRepository())
                .authenticationSuccessHandler(socialLoginSuccessHandler)
                .authenticationFailureHandler(socialLoginFailService)
        );

        return serverHttpSecurity.build();
    }

    @Bean
    public ReactiveJwtDecoder reactiveJwtDecoder() {
        return token -> jwtService.verifyToken(token)
                .then(Mono.fromSupplier(() -> decodeJwt(token)));
    }

    @Bean
    public ReactiveJwtAuthenticationConverterAdapter jwtAuthenticationConverter() {
        JwtGrantedAuthoritiesConverter jwtGrantedAuthoritiesConverter = new JwtGrantedAuthoritiesConverter();
        jwtGrantedAuthoritiesConverter.setAuthorityPrefix("");

        JwtAuthenticationConverter jwtAuthenticationConverter = new JwtAuthenticationConverter();
        jwtAuthenticationConverter.setJwtGrantedAuthoritiesConverter(jwtGrantedAuthoritiesConverter);

        return new ReactiveJwtAuthenticationConverterAdapter(jwtAuthenticationConverter);
    }

    @Bean
    public ReactiveClientRegistrationRepository reactiveClientRegistrationRepository() {
        return new InMemoryReactiveClientRegistrationRepository(
                CommonOAuth2Provider.GOOGLE.getBuilder("google")
                        .clientId(googleClientId)
                        .clientSecret(googleClientSecret)
                        .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                        .redirectUri(googleRedirectUri)
                        .build(),
                CommonOAuth2Provider.FACEBOOK.getBuilder("facebook")
                        .clientId(facebookClientId)
                        .clientSecret(facebookClientSecret)
                        .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                        .redirectUri(facebookRedirectUri)
                        .build(),
                CommonOAuth2Provider.GITHUB.getBuilder("github")
                        .clientId(githubClientId)
                        .clientSecret(githubClientSecret)
                        .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                        .redirectUri(githubRedirectUri)
                        .build()
        );
    }

    @Bean
    public ServerAuthenticationEntryPoint authenticationEntryPoint() {
        return (exchange, authException) -> {
            boolean expired = isAccessTokenExpired(authException);
            int code = expired ? ErrorCode.ACCESS_TOKEN_EXPIRED.getCode() : ErrorCode.INVALID_TOKEN.getCode();
            String message = expired ? ErrorCode.ACCESS_TOKEN_EXPIRED.getMessage() : ErrorCode.INVALID_TOKEN.getMessage();

            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

            ApiResponse<Object> apiResponse = ApiResponse.builder()
                    .code(code)
                    .message(message)
                    .traceId(resolveTraceId(exchange))
                    .build();

            try {
                byte[] bytes = new ObjectMapper().writeValueAsBytes(apiResponse);
                return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
            } catch (Exception e) {
                return Mono.error(e);
            }
        };
    }



    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(Arrays.asList("http://localhost:5173", "http://localhost:5000"));
        configuration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
        configuration.setAllowedHeaders(Collections.singletonList("*"));
        configuration.setAllowCredentials(true);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    private Jwt decodeJwt(String token) {
        try {
            SignedJWT signedJWT = SignedJWT.parse(token);
            Date issueTime = signedJWT.getJWTClaimsSet().getIssueTime();
            Date expiryTime = signedJWT.getJWTClaimsSet().getExpirationTime();

            if (issueTime == null || expiryTime == null) {
                throw new JwtException("Invalid token");
            }

            return new Jwt(
                    token,
                    issueTime.toInstant(),
                    expiryTime.toInstant(),
                    signedJWT.getHeader().toJSONObject(),
                    signedJWT.getJWTClaimsSet().getClaims()
            );
        } catch (ParseException e) {
            throw new JwtException("Invalid token", e);
        }
    }

    private String resolveTraceId(org.springframework.web.server.ServerWebExchange exchange) {
        Object traceId = exchange.getAttribute(TraceIdFilter.TRACE_ID_ATTR);
        return traceId == null ? null : traceId.toString();
    }

    private boolean isAccessTokenExpired(Throwable authException) {
        if (authException == null || authException.getMessage() == null) {
            return false;
        }

        String message = authException.getMessage().toLowerCase();
        return message.contains("expired");
    }
}
