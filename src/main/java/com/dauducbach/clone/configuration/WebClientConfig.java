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
    public WebClient webClient(@Value("${post.media.scan.max-in-memory-size:10MB}") DataSize maxInMemorySize) {
        int maxBytes = Math.toIntExact(maxInMemorySize.toBytes());
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("post.media.scan.max-in-memory-size must be greater than 0");
        }

        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(maxBytes))
                .build();

        return WebClient.builder()
                .exchangeStrategies(strategies)
                .build();
    }
}
