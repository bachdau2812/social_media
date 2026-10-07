package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.modules.media.application.MediaScanGateway;
import com.dauducbach.clone.modules.media.publicapi.MediaInspection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MediaInspectionServiceTest {
    @Test
    void delegatesInspectionToTheScanGateway() {
        MediaScanGateway gateway = mock(MediaScanGateway.class);
        when(gateway.scan("https://cdn.test/image.jpg", "asset-1"))
                .thenReturn(Mono.just(new MediaInspection.Result(false)));

        StepVerifier.create(new MediaInspectionService(gateway)
                        .inspect("https://cdn.test/image.jpg", "asset-1"))
                .expectNext(new MediaInspection.Result(false))
                .verifyComplete();

        verify(gateway).scan("https://cdn.test/image.jpg", "asset-1");
    }
}
