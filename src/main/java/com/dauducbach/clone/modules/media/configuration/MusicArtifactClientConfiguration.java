package com.dauducbach.clone.modules.media.configuration;

import io.netty.channel.ChannelOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration
public class MusicArtifactClientConfiguration {
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int JSON_CODEC_MAX_IN_MEMORY_BYTES = 64 * 1024;

    @Bean("musicArtifactWebClient")
    public WebClient musicArtifactWebClient(SpotifyMusicFetchProperties properties) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .responseTimeout(properties.getServiceTimeout());
        ExchangeStrategies exchangeStrategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer.defaultCodecs()
                        .maxInMemorySize(JSON_CODEC_MAX_IN_MEMORY_BYTES))
                .build();

        return WebClient.builder()
                .baseUrl(properties.getServiceBaseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .exchangeStrategies(exchangeStrategies)
                .build();
    }
}
