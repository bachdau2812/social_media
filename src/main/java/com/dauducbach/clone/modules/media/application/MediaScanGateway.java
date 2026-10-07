package com.dauducbach.clone.modules.media.application;

import com.dauducbach.clone.modules.media.publicapi.MediaInspection;
import reactor.core.publisher.Mono;

/** Port from media inspection orchestration to the configured moderation provider. */
public interface MediaScanGateway {
    Mono<MediaInspection.Result> scan(String mediaUrl, String publicId);
}
