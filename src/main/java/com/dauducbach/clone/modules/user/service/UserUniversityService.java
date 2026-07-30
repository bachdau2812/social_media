package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.user.dto.request.UserUniversityRequest;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.repositoty.UserUniversityRepository;
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

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserUniversityService {
    UserUniversityRepository userUniversityRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    UserAuditService userAuditService;
    UserProfileVectorEventPublisher userProfileVectorEventPublisher;

    private static final Logger log = LoggerFactory.getLogger(UserUniversityService.class);
    private static final String CACHE_PREFIX = "user_university:";
    private static final String LIST_CACHE_PREFIX = "user_university_list:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Tạo mới UserUniversity
    public Mono<UserUniversity> createUserUniversity(UserUniversityRequest request) {
        log.info("|UserUniversityService|createUserUniversity|userId={}", request.getUserId());

        String id = UUID.randomUUID().toString();
        String cacheKey = CACHE_PREFIX + id;
        String listCacheKey = LIST_CACHE_PREFIX + request.getUserId();

        UserUniversity userUniversity = UserUniversity.builder()
                .id(id)
                .userId(request.getUserId())
                .schoolName(request.getSchoolName())
                .major(request.getMajor())
                .from(request.getFrom())
                .to(request.getTo())
                .isGraduate(request.getIsGraduate() != null ? request.getIsGraduate() : false)
                .isPublic(request.getIsPublic() != null ? request.getIsPublic() : false)
                .build();

        return r2dbcEntityTemplate.insert(UserUniversity.class)
                .using(userUniversity)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.USER_UNIVERSITY_SAVE_FAILED,
                        String.format("Save user university failed for userId=%s", request.getUserId()),
                        throwable
                ))
                .flatMap(savedUniversity -> saveProfileComponentAudit(savedUniversity.getUserId(), "USER_UNIVERSITY", savedUniversity.getId(), "CREATE").thenReturn(savedUniversity))
                .flatMap(savedUniversity -> publishProfileVectorRefresh(savedUniversity.getUserId(), "USER_UNIVERSITY", "CREATE", savedUniversity.getId())
                        .thenReturn(savedUniversity))
                .doOnSuccess(savedUniversity -> {
                    log.info("|UserUniversityService|createUserUniversity|created|id={}", savedUniversity.getId());
                    String jsonString = RedisUtil.serialize(savedUniversity);
                    if (jsonString != null) {
                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                    }
                    reactiveRedisStringTemplate.opsForValue().delete(listCacheKey).subscribe();
                })
                .doOnError(error -> log.error("|UserUniversityService|createUserUniversity|failed to create|error={}", error.getMessage(), error));
    }

    public Mono<UserUniversity> updateUserUniversity(UserUniversityRequest request) {
        return userUniversityRepository.findById(request.getId())
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.USER_UNIVERSITY_NOT_FOUND,
                        String.format("User university not found for id=%s", request.getId()))))
                .flatMap(existing -> {
                    if (request.getSchoolName() != null) existing.setSchoolName(request.getSchoolName().trim());
                    if (request.getMajor() != null) existing.setMajor(request.getMajor().isBlank() ? null : request.getMajor().trim());
                    if (request.getFrom() != null) existing.setFrom(request.getFrom());
                    if (request.getTo() != null) existing.setTo(request.getTo());
                    if (request.getIsGraduate() != null) existing.setGraduate(request.getIsGraduate());
                    if (request.getIsPublic() != null) existing.setPublic(request.getIsPublic());
                    return userUniversityRepository.save(existing);
                })
                .flatMap(updated -> saveProfileComponentAudit(updated.getUserId(), "USER_UNIVERSITY", updated.getId(), "UPDATE").thenReturn(updated))
                .flatMap(updated -> publishProfileVectorRefresh(updated.getUserId(), "USER_UNIVERSITY", "UPDATE", updated.getId()).thenReturn(updated))
                .doOnSuccess(updated -> {
                    String json = RedisUtil.serialize(updated);
                    if (json != null) reactiveRedisStringTemplate.opsForValue().set(CACHE_PREFIX + updated.getId(), json, CACHE_TTL).subscribe();
                    reactiveRedisStringTemplate.opsForValue().delete(LIST_CACHE_PREFIX + updated.getUserId()).subscribe();
                })
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.USER_UNIVERSITY_SAVE_FAILED,
                        String.format("Update user university failed for id=%s", request.getId()), error));
    }
    /// Lấy UserUniversity theo ID
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
                    log.warn("|UserUniversityService|publishProfileVectorRefresh|failed|userId={}|source={}|operation={}|error={}",
                            userId, source, operation, error.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<UserUniversity> getUserUniversityById(String id) {
        log.info("|UserUniversityService|getUserUniversityById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .onErrorResume(error -> {
                    log.warn("|UserUniversityService|getUserUniversityById|cache read failed, fallback to database|id={}|error={}", id, error.getMessage());
                    return Mono.empty();
                })
                .flatMap(cachedJsonString -> {
                    UserUniversity cached = RedisUtil.deserialize(cachedJsonString, UserUniversity.class);
                    if (cached != null) {
                        log.info("|UserUniversityService|getUserUniversityById|found in cache|id={}", id);
                        return Mono.just(cached);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(
                        userUniversityRepository.findById(id)
                                .switchIfEmpty(Mono.error(new AppException(
                                        ErrorCode.USER_UNIVERSITY_NOT_FOUND,
                                        String.format("User university not found for id=%s", id)
                                )))
                                .onErrorMap(throwable -> throwable instanceof AppException
                                        ? throwable
                                        : new AppException(
                                                ErrorCode.USER_UNIVERSITY_FETCH_FAILED,
                                                String.format("Fetch user university failed for id=%s", id),
                                                throwable
                                        ))
                                .doOnSuccess(university -> {
                                    log.info("|UserUniversityService|getUserUniversityById|found in database|id={}", id);
                                    String jsonString = RedisUtil.serialize(university);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .doOnError(error -> log.error("|UserUniversityService|getUserUniversityById|failed to fetch|id={}|error={}", id, error.getMessage()))
                );
    }

    /// Lấy danh sách UserUniversity của user (có filter theo isPublic)
    public Flux<UserUniversity> getUserUniversitiesByUserId(String userId, Boolean includeNonPublic) {
        log.info("|UserUniversityService|getUserUniversitiesByUserId|userId={}|includeNonPublic={}", userId, includeNonPublic);

        String listCacheKey = LIST_CACHE_PREFIX + userId;

        if (includeNonPublic != null && includeNonPublic) {
            return userUniversityRepository.findByUserId(userId)
                    .doOnComplete(() -> log.info("|UserUniversityService|getUserUniversitiesByUserId|fetched all for userId={}", userId))
                    .doOnError(error -> log.error("|UserUniversityService|getUserUniversitiesByUserId|failed to fetch|userId={}|error={}", userId, error.getMessage()));
        }

        return reactiveRedisStringTemplate.opsForValue().get(listCacheKey)
                .onErrorResume(error -> {
                    log.warn("|UserUniversityService|getUserUniversitiesByUserId|cache read failed, fallback to database|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                })
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserUniversityService|getUserUniversitiesByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(RedisUtil.deserializeList(cachedJsonString, UserUniversity.class))
                                .filter(UserUniversity::isPublic);
                    }
                    return Flux.empty();
                })
                .switchIfEmpty(
                        userUniversityRepository.findByUserId(userId)
                                .filter(UserUniversity::isPublic)
                                .collectList()
                                .onErrorMap(throwable -> new AppException(
                                        ErrorCode.USER_UNIVERSITY_FETCH_FAILED,
                                        String.format("Fetch user universities failed for userId=%s", userId),
                                        throwable
                                ))
                                .doOnNext(universityList -> {
                                    log.info("|UserUniversityService|getUserUniversitiesByUserId|found {} public items in database|userId={}", universityList.size(), userId);
                                    String jsonString = RedisUtil.serialize(universityList);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(listCacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .flatMapMany(Flux::fromIterable)
                                .doOnError(error -> log.error("|UserUniversityService|getUserUniversitiesByUserId|failed to fetch|userId={}|error={}", userId, error.getMessage()))
                );
    }

    /// Xóa UserUniversity
    public Mono<Void> deleteUserUniversity(String id) {
        log.info("|UserUniversityService|deleteUserUniversity|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return userUniversityRepository.findById(id)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.USER_UNIVERSITY_NOT_FOUND,
                        String.format("User university not found for id=%s", id)
                )))
                .flatMap(university -> userUniversityRepository.deleteById(id)
                        .doOnSuccess(v -> {
                            log.info("|UserUniversityService|deleteUserUniversity|deleted|id={}", id);
                            reactiveRedisStringTemplate.opsForValue().delete(cacheKey).subscribe();
                            reactiveRedisStringTemplate.opsForValue().delete(LIST_CACHE_PREFIX + university.getUserId()).subscribe();
                        })
                        .doOnError(error -> log.error("|UserUniversityService|deleteUserUniversity|failed to delete|id={}|error={}", id, error.getMessage()))
                        .onErrorMap(throwable -> throwable instanceof AppException
                                ? throwable
                                : new AppException(
                                        ErrorCode.USER_UNIVERSITY_DELETE_FAILED,
                                        String.format("Delete user university failed for id=%s", id),
                                        throwable
                                ))
                );
    }
}
