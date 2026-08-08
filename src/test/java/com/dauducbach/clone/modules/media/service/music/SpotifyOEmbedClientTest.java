package com.dauducbach.clone.modules.media.service.music;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SpotifyOEmbedClientTest {
    private static final String TRACK_ID = "2plbrEY59IikOBgBGLjaoe";

    @Test
    void callsCanonicalSpotifyOEmbedUrlAndExtractsThumbnail() {
        AtomicReference<URI> requestedUri = new AtomicReference<>();
        WebClient webClient = WebClient.builder()
                .exchangeFunction(request -> {
                    requestedUri.set(request.url());
                    return Mono.just(jsonResponse(HttpStatus.OK,
                            "{\"thumbnail_url\":\"https://i.scdn.co/image/cover\"}"));
                })
                .build();

        StepVerifier.create(new SpotifyOEmbedClient(webClient).fetchThumbnail(TRACK_ID))
                .expectNext("https://i.scdn.co/image/cover")
                .verifyComplete();

        assertThat(requestedUri.get().getScheme()).isEqualTo("https");
        assertThat(requestedUri.get().getHost()).isEqualTo("open.spotify.com");
        assertThat(requestedUri.get().getPath()).isEqualTo("/oembed");
        assertThat(requestedUri.get().getQuery())
                .isEqualTo("url=https://open.spotify.com/track/" + TRACK_ID);
    }

    @Test
    void completesEmptyWhenThumbnailIsMissing() {
        WebClient webClient = WebClient.builder()
                .exchangeFunction(request -> Mono.just(jsonResponse(HttpStatus.OK, "{}")))
                .build();

        StepVerifier.create(new SpotifyOEmbedClient(webClient).fetchThumbnail(TRACK_ID))
                .verifyComplete();
    }

    @Test
    void propagatesTransportErrorsForCoordinatorToHandle() {
        WebClient webClient = WebClient.builder()
                .exchangeFunction(request -> Mono.error(new IllegalStateException("network down")))
                .build();

        StepVerifier.create(new SpotifyOEmbedClient(webClient).fetchThumbnail(TRACK_ID))
                .expectErrorMessage("network down")
                .verify();
    }

    private ClientResponse jsonResponse(HttpStatus status, String body) {
        return ClientResponse.create(status)
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }
}
