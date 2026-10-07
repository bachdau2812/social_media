package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.user.dto.request.UserUniversityRequest;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.profile.application.ProfileDataCache;
import com.dauducbach.clone.modules.user.repository.UserUniversityRepository;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
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
    ProfileDataCache profileDataCache;
    AuditRecorder auditRecorder;
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
                .flatMap(savedUniversity -> profileDataCache.put(cacheKey, savedUniversity, CACHE_TTL)
                        .then(profileDataCache.evict(listCacheKey))
                        .thenReturn(savedUniversity))
                .doOnSuccess(savedUniversity -> log.info("|UserUniversityService|createUserUniversity|created|id={}", savedUniversity.getId()))
                .flatMap(savedUniversity -> saveProfileComponentAudit(savedUniversity.getUserId(), "USER_UNIVERSITY", savedUniversity.getId(), "CREATE").thenReturn(savedUniversity))
                .flatMap(savedUniversity -> publishProfileVectorRefresh(savedUniversity.getUserId(), "USER_UNIVERSITY", "CREATE", savedUniversity.getId())
                        .thenReturn(savedUniversity))
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
                .flatMap(updated -> profileDataCache.put(CACHE_PREFIX + updated.getId(), updated, CACHE_TTL)
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + updated.getUserId()))
                        .thenReturn(updated))
                .flatMap(updated -> saveProfileComponentAudit(updated.getUserId(), "USER_UNIVERSITY", updated.getId(), "UPDATE").thenReturn(updated))
                .flatMap(updated -> publishProfileVectorRefresh(updated.getUserId(), "USER_UNIVERSITY", "UPDATE", updated.getId()).thenReturn(updated))
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.USER_UNIVERSITY_SAVE_FAILED,
                        String.format("Update user university failed for id=%s", request.getId()), error));
    }
    /// Lấy UserUniversity theo ID
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

    public Mono<UserUniversity> getUserUniversityById(String id) {
        log.info("|UserUniversityService|getUserUniversityById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return profileDataCache.find(cacheKey, UserUniversity.class)
                .doOnNext(cached -> log.info("|UserUniversityService|getUserUniversityById|found in cache|id={}", id))
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
                                .flatMap(university -> cacheRecord(cacheKey, university)
                                        .doOnNext(ignored -> log.info("|UserUniversityService|getUserUniversityById|found in database|id={}", id)))
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

        return profileDataCache.findList(listCacheKey, UserUniversity.class)
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserUniversityService|getUserUniversitiesByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(cachedJsonString)
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
                                })
                                .flatMapMany(universityList -> profileDataCache.put(listCacheKey, universityList, CACHE_TTL)
                                        .thenMany(Flux.fromIterable(universityList)))
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
                        })
                        .then(profileDataCache.evict(cacheKey))
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + university.getUserId()))
                        .then(Mono.defer(() -> publishProfileVectorRefresh(university.getUserId(), "USER_UNIVERSITY", "DELETE", id)))
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
