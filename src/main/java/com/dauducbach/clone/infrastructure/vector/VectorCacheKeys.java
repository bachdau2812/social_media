package com.dauducbach.clone.infrastructure.vector;

/** Existing deployment is standalone Redis. These multi-key scripts must not run on Redis Cluster. */
public final class VectorCacheKeys {
    private VectorCacheKeys() {}
    public static String lock(String userId) { return "vector:lock:" + userId; }
    public static String version(String userId) { return "user_vector_version:" + userId; }
    public static String dirty(String userId) { return "feed:dirty:" + userId; }
    public static String operation(String userId) { return "vector:committed_operation:" + userId; }
    public static String shortTerm(String userId) { return "user_short_term_vector:" + userId; }
    public static String shortTermModel(String userId) { return "user_short_term_vector_model:" + userId; }
}
