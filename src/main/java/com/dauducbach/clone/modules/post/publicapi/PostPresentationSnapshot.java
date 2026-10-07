package com.dauducbach.clone.modules.post.publicapi;

/** Stable post presentation values shared by cross-module read contracts. */
public final class PostPresentationSnapshot {
    private PostPresentationSnapshot() {
    }

    public record Item(
            String id,
            Integer orderNumber,
            String caption,
            Media media,
            Music music
    ) {
    }

    public record Media(
            String assetId,
            String publicId,
            String mediaFormat,
            String resourceType,
            String url,
            String secureUrl,
            String displayName,
            int width,
            int height
    ) {
    }

    public record Music(
            String id,
            String displayName,
            String artist,
            String artworkUrl,
            String playbackUrl,
            Long segmentStart,
            Long segmentEnd,
            Long duration
    ) {
    }
}
