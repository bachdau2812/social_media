package com.dauducbach.clone.commons.constant;

public final class PostNotificationCacheKeys {
    private static final String POST_NOTIFICATION_MUTED_PREFIX = "post:notification:muted:";

    private PostNotificationCacheKeys() {
    }

    public static String mutedPostNotification(String postId, String userId) {
        return POST_NOTIFICATION_MUTED_PREFIX + postId + ":" + userId;
    }
}
