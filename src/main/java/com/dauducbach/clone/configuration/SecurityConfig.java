package com.dauducbach.clone.configuration;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.auth.service.JwtService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jwt.SignedJWT;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.ServerAuthenticationEntryPoint;
import org.springframework.security.web.server.authentication.ServerAuthenticationSuccessHandler;
import org.springframework.security.web.server.authentication.ServerAuthenticationFailureHandler;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import reactor.core.publisher.Mono;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {
    private static final String[] PUBLIC_ENDPOINT = {
            "/auth/login",
            "/auth/introspect",
            "/auth/refresh-token"
    };

    private final JwtService jwtService;

    public SecurityConfig(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @Bean
    public SecurityWebFilterChain filterChain(ServerAuthenticationSuccessHandler successHandler,
                                              ServerAuthenticationFailureHandler failureHandler) {
        ServerHttpSecurity serverHttpSecurity = ServerHttpSecurity.http();

        serverHttpSecurity.csrf(ServerHttpSecurity.CsrfSpec::disable);
        serverHttpSecurity.cors(ServerHttpSecurity.CorsSpec::disable);

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
                .authenticationSuccessHandler(successHandler)
                .authenticationFailureHandler(failureHandler)
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
    public ServerAuthenticationEntryPoint authenticationEntryPoint() {
        return (exchange, authException) -> {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

            ApiResponse<Object> apiResponse = ApiResponse.builder()
                    .code(1001)
                    .message("Unauthenticated")
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
    public ServerAuthenticationSuccessHandler successHandler() {
        return (webFilterExchange, authentication) -> {
            ApiResponse<Object> apiResponse = ApiResponse.builder()
                    .code(1000)
                    .message("OAuth2 login success")
                    .traceId(resolveTraceId(webFilterExchange.getExchange()))
                    .result(authentication.getName())
                    .build();

            return writeJsonResponse(webFilterExchange.getExchange(), HttpStatus.OK, apiResponse);
        };
    }

    @Bean
    public ServerAuthenticationFailureHandler failureHandler() {
        return (webFilterExchange, exception) -> {
            ApiResponse<Object> apiResponse = ApiResponse.builder()
                    .code(1001)
                    .message(exception.getMessage())
                    .traceId(resolveTraceId(webFilterExchange.getExchange()))
                    .build();

            return writeJsonResponse(webFilterExchange.getExchange(), HttpStatus.UNAUTHORIZED, apiResponse);
        };
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(Collections.singletonList("http://localhost:5173"));
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

    private Mono<Void> writeJsonResponse(org.springframework.web.server.ServerWebExchange exchange,
                                         HttpStatus status,
                                         ApiResponse<Object> body) {
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        try {
            byte[] bytes = new ObjectMapper().writeValueAsBytes(body);
            return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
