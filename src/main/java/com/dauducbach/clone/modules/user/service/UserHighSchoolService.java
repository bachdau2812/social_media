package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.user.dto.request.UserHighSchoolRequest;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.profile.application.ProfileDataCache;
import com.dauducbach.clone.modules.user.repository.UserHighSchoolRepository;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
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
    ProfileDataCache profileDataCache;
    AuditRecorder auditRecorder;
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
                .publishOn(Schedulers.boundedElastic())
                .flatMap(savedHighSchool -> profileDataCache.put(cacheKey, savedHighSchool, CACHE_TTL)
                        .then(profileDataCache.evict(listCacheKey))
                        .thenReturn(savedHighSchool))
                .doOnSuccess(savedHighSchool -> log.info("|UserHighSchoolService|createUserHighSchool|created|id={}", savedHighSchool.getId()))
                .flatMap(savedHighSchool -> saveProfileComponentAudit(savedHighSchool.getUserId(), "USER_HIGH_SCHOOL", savedHighSchool.getId(), "CREATE").thenReturn(savedHighSchool))
                .flatMap(savedHighSchool -> publishProfileVectorRefresh(savedHighSchool.getUserId(), "USER_HIGH_SCHOOL", "CREATE", savedHighSchool.getId())
                        .thenReturn(savedHighSchool))
                .doOnError(error -> log.error("|UserHighSchoolService|createUserHighSchool|failed to create|error={}", error.getMessage()));
    }

    public Mono<UserHighSchool> updateUserHighSchool(UserHighSchoolRequest request) {
        return userHighSchoolRepository.findById(request.getId())
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.USER_HIGH_SCHOOL_NOT_FOUND,
                        String.format("User high school not found for id=%s", request.getId()))))
                .flatMap(existing -> {
                    if (request.getSchoolName() != null) existing.setSchoolName(request.getSchoolName().trim());
                    if (request.getFrom() != null) existing.setFromDate(request.getFrom());
                    if (request.getTo() != null) existing.setToDate(request.getTo());
                    if (request.getIsGraduate() != null) existing.setGraduate(request.getIsGraduate());
                    if (request.getIsPublic() != null) existing.setPublic(request.getIsPublic());
                    return userHighSchoolRepository.save(existing);
                })
                .flatMap(updated -> profileDataCache.put(CACHE_PREFIX + updated.getId(), updated, CACHE_TTL)
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + updated.getUserId()))
                        .thenReturn(updated))
                .flatMap(updated -> saveProfileComponentAudit(updated.getUserId(), "USER_HIGH_SCHOOL", updated.getId(), "UPDATE").thenReturn(updated))
                .flatMap(updated -> publishProfileVectorRefresh(updated.getUserId(), "USER_HIGH_SCHOOL", "UPDATE", updated.getId()).thenReturn(updated))
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.USER_HIGH_SCHOOL_SAVE_FAILED,
                        String.format("Update user high school failed for id=%s", request.getId()), error));
    }
    /// Lấy UserHighSchool theo ID
    private Mono<Void> saveProfileComponentAudit(String userId, String component, String resourceId, String operation) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("component", component);
        metadata.addProperty("operation", operation);
        return auditRecorder.record(new AuditEntry(userId, AuditActionType.UPDATE_USER_DETAILS,
                component, resourceId, "SUCCESS", metadata.toString(), null));
    }

    private Mono<Void> publishProfileVectorRefresh(String userId, String source, String operation, String resourceId) {
        return userProfileVectorEventPublisher.publishRefreshEvent(userId, source, operation, resourceId);
    }

    private <T> Mono<T> cacheRecord(String key, T record) {
        return profileDataCache.put(key, record, CACHE_TTL).thenReturn(record);
    }

    public Mono<UserHighSchool> getUserHighSchoolById(String id) {
        log.info("|UserHighSchoolService|getUserHighSchoolById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return profileDataCache.find(cacheKey, UserHighSchool.class)
                .doOnNext(cached -> log.info("|UserHighSchoolService|getUserHighSchoolById|found in cache|id={}", id))
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
                                .flatMap(highSchool -> cacheRecord(cacheKey, highSchool)
                                        .doOnNext(ignored -> log.info("|UserHighSchoolService|getUserHighSchoolById|found in database|id={}", id)))
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

        return profileDataCache.findList(listCacheKey, UserHighSchool.class)
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserHighSchoolService|getUserHighSchoolsByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(cachedJsonString)
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
                                })
                                .flatMapMany(highSchoolList -> profileDataCache.put(listCacheKey, highSchoolList, CACHE_TTL)
                                        .thenMany(Flux.fromIterable(highSchoolList)))
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
                        })
                        .then(profileDataCache.evict(cacheKey))
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + highSchool.getUserId()))
                        .then(Mono.defer(() -> publishProfileVectorRefresh(highSchool.getUserId(), "USER_HIGH_SCHOOL", "DELETE", id)))
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
