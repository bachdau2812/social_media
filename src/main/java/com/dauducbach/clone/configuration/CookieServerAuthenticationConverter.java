package com.dauducbach.clone.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpCookie;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import org.springframework.security.web.server.authentication.ServerAuthenticationConverter;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Configuration
public class CookieServerAuthenticationConverter implements ServerAuthenticationConverter {
    
    private static final String COOKIE_NAME = "accessToken";
    private static final Logger log = LoggerFactory.getLogger(CookieServerAuthenticationConverter.class);

    @Override
    public Mono<Authentication> convert(ServerWebExchange exchange) {
        log.info("Attempting to authenticate using cookie: {}", COOKIE_NAME);
        // Lấy cookie từ request
        HttpCookie cookie = exchange.getRequest().getCookies().getFirst(COOKIE_NAME);
        
        if (cookie != null && !cookie.getValue().isEmpty()) {
            String token = cookie.getValue();
            log.info("Cookie found: {}", token);

            // Trả về token dưới dạng BearerTokenAuthenticationToken để Spring Security xử lý tiếp
            return Mono.just(new BearerTokenAuthenticationToken(token));
        } else {
            log.info("Cookie is empty or null");
        }
        
        // Trả về rỗng nếu không tìm thấy cookie, Spring sẽ chuyển sang báo lỗi 401
        return Mono.empty();
    }
}