package com.dauducbach.clone.modules.media.service.music;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SpotifyOEmbedClient {
    private static final String SPOTIFY_TRACK_PREFIX = "https://open.spotify.com/track/";

    private final WebClient webClient;

    public Mono<String> fetchThumbnail(String trackId) {
        String spotifyTrackUrl = SPOTIFY_TRACK_PREFIX + trackId;
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("open.spotify.com")
                        .path("/oembed")
                        .queryParam("url", spotifyTrackUrl)
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> response.path("thumbnail_url").asText(""))
                .map(String::trim)
                .filter(thumbnail -> !thumbnail.isBlank());
    }
}
