package com.dauducbach.clone.modules.media.publicapi;

public interface MusicUrlDelivery {
    void validateMusicSegment(Long musicStart, Long musicEnd);
    String transformMusicUrl(String musicUrl, Long musicStart, Long musicEnd);
    String transformMusicUrlIfSupported(String musicUrl, Long musicStart, Long musicEnd);
}
