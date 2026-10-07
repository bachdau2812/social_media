package com.dauducbach.clone.modules.post.infrastructure.redis;

final class PostNotificationCacheKeys {
    private static final String POST_NOTIFICATION_MUTED_PREFIX = "post:notification:muted:";

    private PostNotificationCacheKeys() {
    }

    static String mutedPostNotification(String postId, String userId) {
        return POST_NOTIFICATION_MUTED_PREFIX + postId + ":" + userId;
    }
}
