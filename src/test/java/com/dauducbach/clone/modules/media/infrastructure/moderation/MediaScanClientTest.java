package com.dauducbach.clone.modules.media.infrastructure.moderation;

import com.dauducbach.clone.modules.media.infrastructure.moderation.MediaScanClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

class MediaScanClientTest {

    @ParameterizedTest
    @ValueSource(strings = {"{\"data\":{}}", "{\"data\":{\"is_nsfw\":null}}", "{\"data\":{\"is_nsfw\":\"false\"}}", "{\"data\":{\"is_nsfw\":0}}", ""})
    void scanMediaFailsWhenScannerResponseDoesNotContainAnExplicitBooleanDecision(String response) {
        MediaScanClient utils = newClient(request -> Mono.just(ClientResponse.create(HttpStatus.OK)
                .body(request.url().toString().equals("http://scan.local/api") ? response : "image-bytes")
                .build()));

        StepVerifier.create(utils.scanMedia("https://cdn.example.com/image.png", "folder/image"))
                .expectError()
                .verify();
    }

    @Test
    void scanMediaRejectsAnEmptyDownloadedFile() {
        MediaScanClient utils = newClient(request -> Mono.just(ClientResponse.create(HttpStatus.OK).build()));
        StepVerifier.create(utils.scanMedia("https://cdn.example.com/image.png", "folder/image"))
                .expectNextMatches(MediaScanClient.ScanResult::nsfw)
                .verifyComplete();
    }

    @Test
    void scanMediaReturnsApprovedWhenScanApiMarksMediaSafe() {
        MediaScanClient utils = newClient(request -> {
            if (request.url().toString().equals("http://scan.local/api")) {
                return Mono.just(ClientResponse.create(HttpStatus.OK)
                        .body("{\"data\":{\"is_nsfw\":false}}")
                        .build());
            }
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                    .body("fake-image-bytes")
                    .build());
        });

        StepVerifier.create(utils.scanMedia("https://cdn.example.com/media/image.jpg", "folder/image"))
                .expectNextMatches(result -> !result.nsfw())
                .verifyComplete();
    }

    @Test
    void scanMediaPropagatesDownloadFailuresInsteadOfTreatingThemAsModerationRejections() {
        MediaScanClient utils = newClient(request -> Mono.error(new IllegalStateException("download failed")));

        StepVerifier.create(utils.scanMedia("https://cdn.example.com/media/image.jpg", "folder/image"))
                .expectErrorMessage("download failed")
                .verify();
    }

    @Test
    void scanMediaPropagatesDownloadTimeoutInsteadOfTreatingItAsModerationRejection() {
        MediaScanClient utils = newClient(request -> Mono.never());
        ReflectionTestUtils.setField(utils, "scanTimeout", Duration.ofMillis(10));

        StepVerifier.create(utils.scanMedia("https://cdn.example.com/media/image.jpg", "folder/image"))
                .expectError(java.util.concurrent.TimeoutException.class)
                .verify();
    }

    @Test
    void scanMediaPropagatesScannerApiFailuresInsteadOfTreatingThemAsModerationRejections() {
        MediaScanClient utils = newClient(request -> {
            if (request.url().toString().equals("http://scan.local/api")) {
                return Mono.error(new IllegalStateException("scanner unavailable"));
            }
            return Mono.just(ClientResponse.create(HttpStatus.OK).body("fake-image-bytes").build());
        });

        StepVerifier.create(utils.scanMedia("https://cdn.example.com/media/image.jpg", "folder/image"))
                .expectErrorMessage("scanner unavailable")
                .verify();
    }

    private MediaScanClient newClient(ExchangeFunction exchangeFunction) {
        MediaScanClient utils = new MediaScanClient(WebClient.builder()
                .exchangeFunction(exchangeFunction)
                .build());
        ReflectionTestUtils.setField(utils, "scanApiUrl", "http://scan.local/api");
        ReflectionTestUtils.setField(utils, "maxScanMemorySize", DataSize.ofMegabytes(10));
        ReflectionTestUtils.setField(utils, "scanTimeout", Duration.ofSeconds(45));
        return utils;
    }
}
