package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.dto.request.UserHighSchoolRequest;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.repositoty.UserHighSchoolRepository;
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

public class UserHighSchoolService {
    UserHighSchoolRepository userHighSchoolRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;

    private static final Logger log = LoggerFactory.getLogger(UserHighSchoolService.class);
    private static final String CACHE_PREFIX = "user_high_school:";
    private static final String LIST_CACHE_PREFIX = "user_high_school_list:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Tạo mới UserHighSchool
    public Mono<UserHighSchool> createUserHighSchool(UserHighSchoolRequest request) {
        log.info("|UserHighSchoolService|createUserHighSchool|userId={}", request.getUserId());

        String id = UUID.randomUUID().toString();
        String cacheKey = CACHE_PREFIX + id;
        String listCacheKey = LIST_CACHE_PREFIX + request.getUserId();

        UserHighSchool userHighSchool = UserHighSchool.builder()
                .id(id)
                .userId(request.getUserId())
                .schoolName(request.getSchoolName())
                .fromDate(request.getFrom())
                .toDate(request.getTo())
                .isGraduate(request.getIsGraduate() != null ? request.getIsGraduate() : false)
                .isPublic(request.getIsPublic() != null ? request.getIsPublic() : false)
                .build();

        return r2dbcEntityTemplate.insert(UserHighSchool.class)
                .using(userHighSchool)
                .doOnSuccess(savedHighSchool -> {
                    log.info("|UserHighSchoolService|createUserHighSchool|created|id={}", savedHighSchool.getId());
                    String jsonString = RedisUtil.serialize(savedHighSchool);
                    if (jsonString != null) {
                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                    }
                    reactiveRedisStringTemplate.opsForValue().delete(listCacheKey).subscribe();
                })
                .doOnError(error -> log.error("|UserHighSchoolService|createUserHighSchool|failed to create|error={}", error.getMessage()));
    }

    /// Lấy UserHighSchool theo ID
    public Mono<UserHighSchool> getUserHighSchoolById(String id) {
        log.info("|UserHighSchoolService|getUserHighSchoolById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .flatMap(cachedJsonString -> {
                    UserHighSchool cached = RedisUtil.deserialize(cachedJsonString, UserHighSchool.class);
                    if (cached != null) {
                        log.info("|UserHighSchoolService|getUserHighSchoolById|found in cache|id={}", id);
                        return Mono.just(cached);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(
                        userHighSchoolRepository.findById(id)
                                .switchIfEmpty(Mono.error(new RuntimeException("UserHighSchool not found with id: " + id)))
                                .doOnSuccess(highSchool -> {
                                    log.info("|UserHighSchoolService|getUserHighSchoolById|found in database|id={}", id);
                                    String jsonString = RedisUtil.serialize(highSchool);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .doOnError(error -> log.error("|UserHighSchoolService|getUserHighSchoolById|failed to fetch|id={}|error={}", id, error.getMessage()))
                );
    }

    /// Lấy danh sách UserHighSchool của user (có filter theo isPublic)
    public Flux<UserHighSchool> getUserHighSchoolsByUserId(String userId, Boolean includeNonPublic) {
        log.info("|UserHighSchoolService|getUserHighSchoolsByUserId|userId={}|includeNonPublic={}", userId, includeNonPublic);

        String listCacheKey = LIST_CACHE_PREFIX + userId;

        if (includeNonPublic != null && includeNonPublic) {
            return userHighSchoolRepository.findByUserId(userId)
                    .doOnComplete(() -> log.info("|UserHighSchoolService|getUserHighSchoolsByUserId|fetched all for userId={}", userId))
                    .doOnError(error -> log.error("|UserHighSchoolService|getUserHighSchoolsByUserId|failed to fetch|userId={}|error={}", userId, error.getMessage()));
        }

        return reactiveRedisStringTemplate.opsForValue().get(listCacheKey)
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserHighSchoolService|getUserHighSchoolsByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(RedisUtil.deserializeList(cachedJsonString, UserHighSchool.class))
                                .filter(UserHighSchool::isPublic);
                    }
                    return Flux.empty();
                })
                .switchIfEmpty(
                        userHighSchoolRepository.findByUserId(userId)
                                .filter(UserHighSchool::isPublic)
                                .collectList()
                                .doOnNext(highSchoolList -> {
                                    log.info("|UserHighSchoolService|getUserHighSchoolsByUserId|found {} public items in database|userId={}", highSchoolList.size(), userId);
                                    String jsonString = RedisUtil.serialize(highSchoolList);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(listCacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .flatMapMany(Flux::fromIterable)
                                .doOnError(error -> log.error("|UserHighSchoolService|getUserHighSchoolsByUserId|failed to fetch|userId={}|error={}", userId, error.getMessage()))
                );
    }

    /// Xóa UserHighSchool
    public Mono<Void> deleteUserHighSchool(String id) {
        log.info("|UserHighSchoolService|deleteUserHighSchool|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return userHighSchoolRepository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("UserHighSchool not found with id: " + id)))
                .flatMap(highSchool -> userHighSchoolRepository.deleteById(id)
                        .doOnSuccess(v -> {
                            log.info("|UserHighSchoolService|deleteUserHighSchool|deleted|id={}", id);
                            reactiveRedisStringTemplate.opsForValue().delete(cacheKey).subscribe();
                            reactiveRedisStringTemplate.opsForValue().delete(LIST_CACHE_PREFIX + highSchool.getUserId()).subscribe();
                        })
                        .doOnError(error -> log.error("|UserHighSchoolService|deleteUserHighSchool|failed to delete|id={}|error={}", id, error.getMessage()))
                );
    }
}