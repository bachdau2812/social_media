package com.dauducbach.clone.modules.feed.constant;

public final class FeedCacheKeys {
    public static final String USER_FEED_PREFIX = "feed:";
    public static final String SEEN_POST_PREFIX = "seen_post:";
    public static final String USER_SHORT_TERM_VECTOR_PREFIX = "user_short_term_vector:";
    public static final String USER_LONG_TERM_VECTOR_SNAPSHOT_PREFIX = "user_long_term_vector_snapshot:";

    private FeedCacheKeys() {
    }

    public static String userFeed(String userId) {
        return USER_FEED_PREFIX + userId;
    }

    public static String recommendations(String userId) { return "feed:recommendations:v2:" + userId; }
    public static String fanout(String userId) { return "feed:fanout:v2:" + userId; }
    public static String metadata(String userId) { return "feed:metadata:v2:" + userId; }

    public static String seenPost(String userId) {
        return SEEN_POST_PREFIX + userId;
    }

    public static String userShortTermVector(String userId) {
        return USER_SHORT_TERM_VECTOR_PREFIX + userId;
    }

    public static String userLongTermVectorSnapshot(String userId) {
        return USER_LONG_TERM_VECTOR_SNAPSHOT_PREFIX + userId;
    }
}
