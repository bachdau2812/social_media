package com.dauducbach.clone.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {
    @Bean
    public WebClient webClient(@Value("${app.media.limits.image:100MB}") DataSize maxInMemorySize) {
        int maxBytes = Math.toIntExact(maxInMemorySize.toBytes());
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("app.media.limits.image must be greater than 0");
        }

        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(maxBytes))
                .build();

        return WebClient.builder()
                .exchangeStrategies(strategies)
                .build();
    }
}
