package com.dauducbach.clone.modules.media.publicapi;

public interface MediaUploadPolicy {
    long imageMaxBytes();

    long videoMaxBytes();

    long audioMaxBytes();
}
