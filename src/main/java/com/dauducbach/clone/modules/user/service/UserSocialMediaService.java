package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.dto.request.UserSocialMediaRequest;
import com.dauducbach.clone.modules.user.entity.UserSocialMedia;
import com.dauducbach.clone.modules.user.repositoty.UserSocialMediaRepository;
import com.dauducbach.clone.utils.RedisUtil;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserSocialMediaService {
    UserSocialMediaRepository userSocialMediaRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;

    private static final Logger log = LoggerFactory.getLogger(UserSocialMediaService.class);
    private static final String CACHE_PREFIX = "user_social_media:";
    private static final String LIST_CACHE_PREFIX = "user_social_media_list:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Tạo mới UserSocialMedia
    public Mono<UserSocialMedia> createUserSocialMedia(UserSocialMediaRequest request) {
        log.info("|UserSocialMediaService|createUserSocialMedia|userId={}", request.getUserId());

        String id = UUID.randomUUID().toString();
        String cacheKey = CACHE_PREFIX + id;
        String listCacheKey = LIST_CACHE_PREFIX + request.getUserId();

        UserSocialMedia userSocialMedia = UserSocialMedia.builder()
                .id(id)
                .userId(request.getUserId())
                .link(request.getLink())
                .build();

        return r2dbcEntityTemplate.insert(UserSocialMedia.class)
                .using(userSocialMedia)
                .doOnSuccess(savedSocialMedia -> {
                    log.info("|UserSocialMediaService|createUserSocialMedia|created user social media|id={}", savedSocialMedia.getId());
                    String jsonString = RedisUtil.serialize(savedSocialMedia);
                    if (jsonString != null) {
                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                    }
                    reactiveRedisStringTemplate.opsForValue().delete(listCacheKey).subscribe();
                })
                .doOnError(error -> log.error("|UserSocialMediaService|createUserSocialMedia|failed to create|error={}", error.getMessage()));
    }

    /// Lấy UserSocialMedia theo ID
    public Mono<UserSocialMedia> getUserSocialMediaById(String id) {
        log.info("|UserSocialMediaService|getUserSocialMediaById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .flatMap(cachedJsonString -> {
                    UserSocialMedia cached = RedisUtil.deserialize(cachedJsonString, UserSocialMedia.class);
                    if (cached != null) {
                        log.info("|UserSocialMediaService|getUserSocialMediaById|found in cache|id={}", id);
                        return Mono.just(cached);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(
                        userSocialMediaRepository.findById(id)
                                .switchIfEmpty(Mono.error(new RuntimeException("UserSocialMedia not found with id: " + id)))
                                .doOnSuccess(socialMedia -> {
                                    log.info("|UserSocialMediaService|getUserSocialMediaById|found in database|id={}", id);
                                    String jsonString = RedisUtil.serialize(socialMedia);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .doOnError(error -> log.error("|UserSocialMediaService|getUserSocialMediaById|failed to fetch|id={}|error={}", id, error.getMessage()))
                );
    }

    /// Lấy danh sách UserSocialMedia của user
    public Flux<UserSocialMedia> getUserSocialMediaByUserId(String userId) {
        log.info("|UserSocialMediaService|getUserSocialMediaByUserId|userId={}", userId);

        String listCacheKey = LIST_CACHE_PREFIX + userId;

        return reactiveRedisStringTemplate.opsForValue().get(listCacheKey)
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserSocialMediaService|getUserSocialMediaByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(RedisUtil.deserializeList(cachedJsonString, UserSocialMedia.class));
                    }
                    return Flux.empty();
                })
                .switchIfEmpty(
                        userSocialMediaRepository.findByUserId(userId)
                                .collectList()
                                .doOnNext(socialMediaList -> {
                                    log.info("|UserSocialMediaService|getUserSocialMediaByUserId|found {} items in database|userId={}", socialMediaList.size(), userId);
                                    String jsonString = RedisUtil.serialize(socialMediaList);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(listCacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .flatMapMany(Flux::fromIterable)
                                .doOnError(error -> log.error("|UserSocialMediaService|getUserSocialMediaByUserId|failed to fetch|userId={}|error={}", userId, error.getMessage()))
                );
    }

    /// Xóa UserSocialMedia
    public Mono<Void> deleteUserSocialMedia(String id) {
        log.info("|UserSocialMediaService|deleteUserSocialMedia|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return userSocialMediaRepository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("UserSocialMedia not found with id: " + id)))
                .flatMap(socialMedia -> userSocialMediaRepository.deleteById(id)
                        .doOnSuccess(v -> {
                            log.info("|UserSocialMediaService|deleteUserSocialMedia|deleted|id={}", id);
                            reactiveRedisStringTemplate.opsForValue().delete(cacheKey).subscribe();
                            reactiveRedisStringTemplate.opsForValue().delete(LIST_CACHE_PREFIX + socialMedia.getUserId()).subscribe();
                        })
                        .doOnError(error -> log.error("|UserSocialMediaService|deleteUserSocialMedia|failed to delete|id={}|error={}", id, error.getMessage()))
                );
    }
}