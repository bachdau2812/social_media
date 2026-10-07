package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.user.dto.request.UserJobRequest;
import com.dauducbach.clone.modules.user.dto.request.UserJobUpdateRequest;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.profile.application.ProfileDataCache;
import com.dauducbach.clone.modules.user.repository.UserJobRepository;
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

public class UserJobService {
    UserJobRepository userJobRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ProfileDataCache profileDataCache;
    AuditRecorder auditRecorder;
    UserProfileVectorEventPublisher userProfileVectorEventPublisher;

    private static final Logger log = LoggerFactory.getLogger(UserJobService.class);
    private static final String CACHE_PREFIX = "user_job:";
    private static final String LIST_CACHE_PREFIX = "user_job_list:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Tạo mới UserJob
    public Mono<UserJob> createUserJob(UserJobRequest request) {
        log.info("|UserJobService|createUserJob|userId={}", request.getUserId());

        String id = UUID.randomUUID().toString();
        String cacheKey = CACHE_PREFIX + id;
        String listCacheKey = LIST_CACHE_PREFIX + request.getUserId();

        UserJob userJob = UserJob.builder()
                .id(id)
                .userId(request.getUserId())
                .companyName(request.getCompanyName())
                .position(request.getPosition())
                .fromDate(request.getFrom())
                .toDate(request.getTo())
                .isPublic(request.getIsPublic() != null ? request.getIsPublic() : false)
                .build();

        return r2dbcEntityTemplate.insert(UserJob.class)
                .using(userJob)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.USER_JOB_SAVE_FAILED,
                        String.format("Save user job failed for userId=%s", request.getUserId()),
                        throwable
                ))
                .flatMap(savedJob -> profileDataCache.put(cacheKey, savedJob, CACHE_TTL)
                        .then(profileDataCache.evict(listCacheKey))
                        .thenReturn(savedJob))
                .doOnSuccess(savedJob -> log.info("|UserJobService|createUserJob|created user job|id={}", savedJob.getId()))
                .flatMap(savedJob -> saveProfileComponentAudit(savedJob.getUserId(), "USER_JOB", savedJob.getId(), "CREATE").thenReturn(savedJob))
                .flatMap(savedJob -> publishProfileVectorRefresh(savedJob.getUserId(), "USER_JOB", "CREATE", savedJob.getId()).thenReturn(savedJob))
                .doOnError(error -> log.error("|UserJobService|createUserJob|failed to create job|error={}", error.getMessage()));
    }

    /// Cập nhật UserJob
    public Mono<UserJob> updateUserJob(UserJobUpdateRequest request) {
        log.info("|UserJobService|updateUserJob|id={}", request.getId());

        String cacheKey = CACHE_PREFIX + request.getId();

        return userJobRepository.findById(request.getId())
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.USER_JOB_NOT_FOUND,
                        String.format("User job not found for id=%s", request.getId())
                )))
                .flatMap(existingJob -> {
                    // Update non-null fields
                    if (request.getCompanyName() != null && !request.getCompanyName().isBlank()) {
                        existingJob.setCompanyName(request.getCompanyName());
                    }
                    if (request.getPosition() != null && !request.getPosition().isBlank()) {
                        existingJob.setPosition(request.getPosition());
                    }
                    if (request.getFrom() != null) {
                        existingJob.setFromDate(request.getFrom());
                    }
                    if (request.getTo() != null) {
                        existingJob.setToDate(request.getTo());
                    }
                    if (request.getIsPublic() != null) {
                        existingJob.setPublic(request.getIsPublic());
                    }

                    return userJobRepository.save(existingJob);
                })
                .flatMap(updatedJob -> profileDataCache.put(cacheKey, updatedJob, CACHE_TTL)
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + updatedJob.getUserId()))
                        .thenReturn(updatedJob))
                .doOnSuccess(updatedJob -> log.info("|UserJobService|updateUserJob|updated user job|id={}", updatedJob.getId()))
                .flatMap(updatedJob -> saveProfileComponentAudit(updatedJob.getUserId(), "USER_JOB", updatedJob.getId(), "UPDATE").thenReturn(updatedJob))
                .flatMap(updatedJob -> publishProfileVectorRefresh(updatedJob.getUserId(), "USER_JOB", "UPDATE", updatedJob.getId()).thenReturn(updatedJob))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.USER_JOB_UPDATE_FAILED,
                                String.format("Update user job failed for id=%s", request.getId()),
                                throwable
                        ))
                .doOnError(error -> log.error("|UserJobService|updateUserJob|failed to update job|error={}", error.getMessage()));
    }

    /// Lấy UserJob theo ID
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

    public Mono<UserJob> getUserJobById(String id) {
        log.info("|UserJobService|getUserJobById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return profileDataCache.find(cacheKey, UserJob.class)
                .doOnNext(cachedJob -> log.info("|UserJobService|getUserJobById|found in cache|id={}", id))
                .switchIfEmpty(
                        userJobRepository.findById(id)
                                .switchIfEmpty(Mono.error(new AppException(
                                        ErrorCode.USER_JOB_NOT_FOUND,
                                        String.format("User job not found for id=%s", id)
                                )))
                                .onErrorMap(throwable -> throwable instanceof AppException
                                        ? throwable
                                        : new AppException(
                                                ErrorCode.USER_JOB_FETCH_FAILED,
                                                String.format("Fetch user job failed for id=%s", id),
                                                throwable
                                        ))
                                .flatMap(job -> cacheRecord(cacheKey, job)
                                        .doOnNext(ignored -> log.info("|UserJobService|getUserJobById|found in database|id={}", id)))
                                .doOnError(error -> log.error("|UserJobService|getUserJobById|failed to fetch job|id={}|error={}", id, error.getMessage()))
                );
    }

    /// Lấy danh sách UserJob của user (có filter theo isPublic)
    public Flux<UserJob> getUserJobsByUserId(String userId, Boolean includeNonPublic) {
        log.info("|UserJobService|getUserJobsByUserId|userId={}|includeNonPublic={}", userId, includeNonPublic);

        String listCacheKey = LIST_CACHE_PREFIX + userId;

        // Nếu request bao gồm cả non-public data, thì không cache (bảo mật)
        if (includeNonPublic != null && includeNonPublic) {
            return userJobRepository.findByUserId(userId)
                    .doOnComplete(() -> log.info("|UserJobService|getUserJobsByUserId|fetched all jobs for userId={}", userId))
                    .doOnError(error -> log.error("|UserJobService|getUserJobsByUserId|failed to fetch jobs|userId={}|error={}", userId, error.getMessage()));
        }

        // Chỉ lấy public data, có thể cache
        return profileDataCache.findList(listCacheKey, UserJob.class)
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserJobService|getUserJobsByUserId|found list in cache|userId={}", userId);
                        // Trả về public jobs từ cache
                        return Flux.fromIterable(cachedJsonString)
                                .filter(UserJob::isPublic);
                    }
                    return Flux.empty();
                })
                .switchIfEmpty(
                        userJobRepository.findByUserId(userId)
                                .filter(UserJob::isPublic)
                                .collectList()
                                .onErrorMap(throwable -> new AppException(
                                        ErrorCode.USER_JOB_FETCH_FAILED,
                                        String.format("Fetch user jobs failed for userId=%s", userId),
                                        throwable
                                ))
                                .publishOn(Schedulers.boundedElastic())
                                .doOnNext(jobs -> {
                                    log.info("|UserJobService|getUserJobsByUserId|found {} public jobs in database|userId={}", jobs.size(), userId);
                                })
                                .flatMapMany(jobs -> profileDataCache.put(listCacheKey, jobs, CACHE_TTL)
                                        .thenMany(Flux.fromIterable(jobs)))
                                .doOnError(error -> log.error("|UserJobService|getUserJobsByUserId|failed to fetch jobs|userId={}|error={}", userId, error.getMessage()))
                );
    }

    /// Xóa UserJob
    public Mono<Void> deleteUserJob(String id) {
        log.info("|UserJobService|deleteUserJob|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return userJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.USER_JOB_NOT_FOUND,
                        String.format("User job not found for id=%s", id)
                )))
                .flatMap(job -> userJobRepository.deleteById(id)
                        .doOnSuccess(v -> {
                            log.info("|UserJobService|deleteUserJob|deleted job|id={}", id);
                            // Delete cache
                            // Invalidate user's job list cache
                        })
                        .then(profileDataCache.evict(cacheKey))
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + job.getUserId()))
                        .then(Mono.defer(() -> publishProfileVectorRefresh(job.getUserId(), "USER_JOB", "DELETE", id)))
                        .doOnError(error -> log.error("|UserJobService|deleteUserJob|failed to delete job|id={}|error={}", id, error.getMessage()))
                )
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.USER_JOB_DELETE_FAILED,
                                String.format("Delete user job failed for id=%s", id),
                                throwable
                        ));
    }
}
