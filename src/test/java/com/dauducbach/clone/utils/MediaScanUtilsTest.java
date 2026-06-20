package com.dauducbach.clone.utils;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class MediaScanUtilsTest {

    @Test
    void scanMediaReturnsApprovedWhenScanApiMarksMediaSafe() {
        MediaScanUtils utils = newUtils(request -> {
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
    void scanMediaReturnsRejectedWhenDownloadFails() {
        MediaScanUtils utils = newUtils(request -> Mono.error(new IllegalStateException("download failed")));

        StepVerifier.create(utils.scanMedia("https://cdn.example.com/media/image.jpg", "folder/image"))
                .expectNextMatches(MediaScanUtils.ScanResult::nsfw)
                .verifyComplete();
    }

    private MediaScanUtils newUtils(ExchangeFunction exchangeFunction) {
        MediaScanUtils utils = new MediaScanUtils(WebClient.builder()
                .exchangeFunction(exchangeFunction)
                .build());
        ReflectionTestUtils.setField(utils, "scanApiUrl", "http://scan.local/api");
        ReflectionTestUtils.setField(utils, "maxScanMemorySize", DataSize.ofMegabytes(10));
        return utils;
    }
}
