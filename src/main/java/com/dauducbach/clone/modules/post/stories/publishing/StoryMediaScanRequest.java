package com.dauducbach.clone.modules.post.stories.publishing;

public record StoryMediaScanRequest(
        String storyId,
        String userId,
        String mediaUrl,
        String publicId,
        String mediaType
) { }
