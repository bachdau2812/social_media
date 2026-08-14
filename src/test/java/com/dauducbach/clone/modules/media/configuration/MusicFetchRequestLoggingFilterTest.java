package com.dauducbach.clone.modules.media.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class MusicFetchRequestLoggingFilterTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";

    @Test
    void matchingFetchRequestPreservesTheDownstreamResponse() {
        MusicFetchRequestLoggingFilter filter = new MusicFetchRequestLoggingFilter();
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/app/musics/" + TRACK_ID + "/fetch").build());
        AtomicBoolean invoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, current -> {
                    invoked.set(true);
                    current.getResponse().setStatusCode(HttpStatus.ACCEPTED);
                    return current.getResponse().setComplete();
                }))
                .verifyComplete();

        assertThat(invoked).isTrue();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    void onlyPostTrackFetchPathsAreObserved() {
        assertThat(MusicFetchRequestLoggingFilter.musicTrackId(
                MockServerHttpRequest.post("/app/musics/" + TRACK_ID + "/fetch").build()))
                .contains(TRACK_ID);
        assertThat(MusicFetchRequestLoggingFilter.musicTrackId(
                MockServerHttpRequest.get("/app/musics/" + TRACK_ID + "/fetch").build()))
                .isEmpty();
        assertThat(MusicFetchRequestLoggingFilter.musicTrackId(
                MockServerHttpRequest.post("/app/posts/" + TRACK_ID + "/fetch").build()))
                .isEmpty();
    }
}
