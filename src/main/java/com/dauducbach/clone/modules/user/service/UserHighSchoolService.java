package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.user.dto.request.UserHighSchoolRequest;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.repositoty.UserHighSchoolRepository;
import com.dauducbach.clone.utils.RedisUtil;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserHighSchoolService {
    UserHighSchoolRepository userHighSchoolRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    UserAuditService userAuditService;
    UserProfileVectorEventPublisher userProfileVectorEventPublisher;

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
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.USER_HIGH_SCHOOL_SAVE_FAILED,
                        String.format("Save user high school failed for userId=%s", request.getUserId()),
                        throwable
                ))
                .flatMap(savedHighSchool -> saveProfileComponentAudit(savedHighSchool.getUserId(), "USER_HIGH_SCHOOL", savedHighSchool.getId(), "CREATE").thenReturn(savedHighSchool))
                .flatMap(savedHighSchool -> publishProfileVectorRefresh(savedHighSchool.getUserId(), "USER_HIGH_SCHOOL", "CREATE", savedHighSchool.getId())
                        .thenReturn(savedHighSchool))
                .publishOn(Schedulers.boundedElastic())
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
    private Mono<Void> saveProfileComponentAudit(String userId, String component, String resourceId, String operation) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("component", component);
        metadata.addProperty("operation", operation);
        return userAuditService.save(AuditLogs.builder()
                .actorId(userId)
                .action(AuditActionType.UPDATE_USER_DETAILS)
                .resourceType(component)
                .resourceId(resourceId)
                .status("SUCCESS")
                .metadata(metadata.toString())
                .build());
    }

    private Mono<Void> publishProfileVectorRefresh(String userId, String source, String operation, String resourceId) {
        return userProfileVectorEventPublisher.publishRefreshEvent(userId, source, operation, resourceId)
                .onErrorResume(error -> {
                    log.warn("|UserHighSchoolService|publishProfileVectorRefresh|failed|userId={}|source={}|operation={}|error={}",
                            userId, source, operation, error.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<UserHighSchool> getUserHighSchoolById(String id) {
        log.info("|UserHighSchoolService|getUserHighSchoolById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .onErrorResume(error -> {
                    log.warn("|UserHighSchoolService|getUserHighSchoolById|cache read failed, fallback to database|id={}|error={}", id, error.getMessage());
                    return Mono.empty();
                })
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
                                .switchIfEmpty(Mono.error(new AppException(
                                        ErrorCode.USER_HIGH_SCHOOL_NOT_FOUND,
                                        String.format("User high school not found for id=%s", id)
                                )))
                                .onErrorMap(throwable -> throwable instanceof AppException
                                        ? throwable
                                        : new AppException(
                                                ErrorCode.USER_HIGH_SCHOOL_FETCH_FAILED,
                                                String.format("Fetch user high school failed for id=%s", id),
                                                throwable
                                        ))
                                .publishOn(Schedulers.boundedElastic())
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
                .onErrorResume(error -> {
                    log.warn("|UserHighSchoolService|getUserHighSchoolsByUserId|cache read failed, fallback to database|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                })
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
                                .onErrorMap(throwable -> new AppException(
                                        ErrorCode.USER_HIGH_SCHOOL_FETCH_FAILED,
                                        String.format("Fetch user high schools failed for userId=%s", userId),
                                        throwable
                                ))
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
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.USER_HIGH_SCHOOL_NOT_FOUND,
                        String.format("User high school not found for id=%s", id)
                )))
                .flatMap(highSchool -> userHighSchoolRepository.deleteById(id)
                        .doOnSuccess(v -> {
                            log.info("|UserHighSchoolService|deleteUserHighSchool|deleted|id={}", id);
                            reactiveRedisStringTemplate.opsForValue().delete(cacheKey).subscribe();
                            reactiveRedisStringTemplate.opsForValue().delete(LIST_CACHE_PREFIX + highSchool.getUserId()).subscribe();
                        })
                        .doOnError(error -> log.error("|UserHighSchoolService|deleteUserHighSchool|failed to delete|id={}|error={}", id, error.getMessage()))
                        .onErrorMap(throwable -> throwable instanceof AppException
                                ? throwable
                                : new AppException(
                                        ErrorCode.USER_HIGH_SCHOOL_DELETE_FAILED,
                                        String.format("Delete user high school failed for id=%s", id),
                                        throwable
                                ))
                );
    }
}
