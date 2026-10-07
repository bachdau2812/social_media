package com.dauducbach.clone.modules.user.profile.application;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/** Optional cache for profile records. Cache failures behave as misses and never fail profile operations. */
public interface ProfileDataCache {
    <T> Mono<T> find(String key, Class<T> recordType);

    <T> Mono<List<T>> findList(String key, Class<T> recordType);

    Mono<Void> put(String key, Object value, Duration ttl);

    Mono<Void> evict(String key);
}
