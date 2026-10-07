package com.dauducbach.clone.modules.post.stories.publishing;

import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import reactor.core.publisher.Mono;

public interface StoryPublicationMessaging {
    Mono<Void> requestMediaScan(UserStories story, String publicId);

    Mono<Void> publishApproved(UserStories story, MediaAssetView media, String transformedMusicUrl);

    Mono<Void> publishRejected(String userId, String storyId, String mediaUrl, String message);
}
