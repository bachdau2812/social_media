package com.dauducbach.clone.modules.post.repository.projection;

import java.time.Instant;

public interface FriendFeedActivityProjection {
    String getFeedEntryId();

    String getPostId();

    String getActivityType();

    String getActorId();

    Instant getActivityAt();
}
