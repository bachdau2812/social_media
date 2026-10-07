package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.modules.media.publicapi.MediaInspection;
import com.dauducbach.clone.modules.media.application.MediaScanGateway;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class MediaInspectionService implements MediaInspection {
    private final MediaScanGateway scanGateway;

    @Override
    public Mono<Result> inspect(String mediaUrl, String publicId) {
        return scanGateway.scan(mediaUrl, publicId);
    }
}
