package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.user.dto.request.UserDetailsUpdateRequest;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.dauducbach.clone.utils.RedisUtil;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserDetailsService {
    UserDetailsRepository userDetailsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    UserAuditService userAuditService;
    UserProfileVectorEventPublisher userProfileVectorEventPublisher;

    private static final Logger log = LoggerFactory.getLogger(UserDetailsService.class);
    private static final String USER_DETAILS_CACHE_PREFIX = "user_details_info:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Listen event and create profile for new user
    @KafkaListener(topics = "profile_creation_event", groupId = "user-service")
    public void createUserDetails (@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        var userDetails = UserDetails.builder()
                .userId(KafkaUtils.extractString(payloadJson, "userId"))
                .username(KafkaUtils.extractString(payloadJson, "username"))
                .dob(KafkaUtils.extractLocalDate(payloadJson, "dob"))
                .hometown(KafkaUtils.extractString(payloadJson, "hometown"))
                .livingIn(KafkaUtils.extractString(payloadJson, "livingIn"))
                .sex(KafkaUtils.extractString(payloadJson, "sex"))
                .build();
        userDetails.setHobbyList(extractHobbyList(payloadJson));
        log.info("|UserDetailsService|createUserDetails|received|userId={}|username={}",
                userDetails.getUserId(), userDetails.getUsername());

        insertUserDetails(userDetails)
                .subscribe(
                        saved -> log.info("|UserDetailsService|createUserDetails|created user details|userId={}", saved.getUserId()),
                        error -> log.error("|UserDetailsService|createUserDetails|failed to create user details|userId={}|error={}", userDetails.getUserId(), error.getMessage())
                );
    }

    /// Insert UserDetails with caching
    public Mono<UserDetails> insertUserDetails(UserDetails userDetails) {
        log.info("|UserDetailsService|insertUserDetails|saving userDetails to database|userId={}", userDetails.getUserId());

        String cacheKey = USER_DETAILS_CACHE_PREFIX + userDetails.getUserId();

        return r2dbcEntityTemplate.insert(UserDetails.class)
                .using(userDetails)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.USER_DETAILS_SAVE_FAILED,
                        String.format("Save user details failed for userId=%s", userDetails.getUserId()),
                        throwable
                ))
                .flatMap(savedUserDetails -> publishProfileVectorRefreshForCreate(savedUserDetails)
                        .thenReturn(savedUserDetails))
                .doOnSuccess(savedUserDetails -> {
                    log.info("|UserDetailsService|insertUserDetails|saved userDetails to database|userId={}", savedUserDetails.getUserId());
                    // Cache the saved user details as JSON string
                    String jsonString = RedisUtil.serialize(savedUserDetails);
                    if (jsonString != null) {
                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL)
                                .doOnSuccess(v -> log.info("|UserDetailsService|insertUserDetails|cached userDetails|userId={}", savedUserDetails.getUserId()))
                                .doOnError(error -> log.error("|UserDetailsService|insertUserDetails|failed to cache userDetails|userId={}|error={}", savedUserDetails.getUserId(), error.getMessage()))
                                .subscribe();
                    }
                })
                .doOnError(error -> log.error("|UserDetailsService|insertUserDetails|failed to save userDetails to database|error={}", error.getMessage()));
    }

    /// Update UserDetails with partial field update
    public Mono<UserDetails> updateUserDetails(UserDetailsUpdateRequest request) {
        log.info("|UserDetailsService|updateUserDetails|updating userDetails|userId={}", request.getUserId());

        String cacheKey = USER_DETAILS_CACHE_PREFIX + request.getUserId();

        // Check if user exists
        return userDetailsRepository.existsById(request.getUserId())
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new AppException(
                                ErrorCode.USER_DETAILS_NOT_FOUND,
                                String.format("User details not found for userId=%s", request.getUserId())
                        ));
                    }

                    // Get existing user details
                    return userDetailsRepository.findById(request.getUserId());
                })
                .flatMap(existingUserDetails -> {
                    log.info("|UserDetailsService|updateUserDetails|found|userId={}", request.getUserId());
                    // Update only non-null and non-empty fields
                    if (request.getUsername() != null && !request.getUsername().isBlank()) {
                        existingUserDetails.setUsername(request.getUsername());
                    }
                    if (request.getDob() != null) {
                        existingUserDetails.setDob(request.getDob());
                    }
                    if (request.getHomeTown() != null && !request.getHomeTown().isBlank()) {
                        existingUserDetails.setHometown(request.getHomeTown());
                    }
                    if (request.getLivingIn() != null && !request.getLivingIn().isBlank()) {
                        existingUserDetails.setLivingIn(request.getLivingIn());
                    }
                    if (request.getSex() != null && !request.getSex().isBlank()) {
                        existingUserDetails.setSex(request.getSex());
                    }
                    if (request.getHobbieList() != null && !request.getHobbieList().isEmpty()) {
                        existingUserDetails.setHobbyList(request.getHobbieList());
                    }

                    return userDetailsRepository.save(existingUserDetails);
                })
                .flatMap(updated -> saveUpdateUserDetailsAudit(request, updated.getUserId()).thenReturn(updated))
                .flatMap(updated -> publishProfileVectorRefresh(updated.getUserId(), "USER_DETAILS", "UPDATE", updated.getUserId())
                        .thenReturn(updated))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.USER_DETAILS_UPDATE_FAILED,
                                String.format("Update user details failed for userId=%s", request.getUserId()),
                                throwable
                        ))
                .publishOn(Schedulers.boundedElastic())
                .doOnSuccess(updatedUserDetails -> {
                    log.info("|UserDetailsService|updateUserDetails|updated userDetails successfully|userId={}", updatedUserDetails.getUserId());
                    // Update cache as JSON string
                    String jsonString = RedisUtil.serialize(updatedUserDetails);
                    if (jsonString != null) {
                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL)
                                .doOnSuccess(v -> log.info("|UserDetailsService|updateUserDetails|updated cache|userId={}", updatedUserDetails.getUserId()))
                                .doOnError(error -> log.error("|UserDetailsService|updateUserDetails|failed to update cache|userId={}|error={}", updatedUserDetails.getUserId(), error.getMessage()))
                                .subscribe();
                    }
                })
                .doOnError(error -> log.error("|UserDetailsService|updateUserDetails|failed to update userDetails|error={}", error.getMessage()));
    }

    private Mono<Void> saveUpdateUserDetailsAudit(UserDetailsUpdateRequest request, String userId) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("usernameChanged", request.getUsername() != null && !request.getUsername().isBlank());
        metadata.addProperty("dobChanged", request.getDob() != null);
        metadata.addProperty("hometownChanged", request.getHomeTown() != null && !request.getHomeTown().isBlank());
        metadata.addProperty("livingInChanged", request.getLivingIn() != null && !request.getLivingIn().isBlank());
        metadata.addProperty("sexChanged", request.getSex() != null && !request.getSex().isBlank());
        metadata.addProperty("hobbyChanged", request.getHobbieList() != null && !request.getHobbieList().isEmpty());

        return userAuditService.save(AuditLogs.builder()
                .actorId(userId)
                .action(AuditActionType.UPDATE_USER_DETAILS)
                .resourceType("USER_DETAILS")
                .resourceId(userId)
                .status("SUCCESS")
                .metadata(metadata.toString())
                .build());
    }

    private java.util.List<String> extractHobbyList(JsonObject payloadJson) {
        java.util.List<String> hobbyList = KafkaUtils.extractStringList(payloadJson, "hobbyList");
        if (!hobbyList.isEmpty()) {
            return hobbyList;
        }
        return KafkaUtils.extractStringList(payloadJson, "hobbieList");
    }

    private Mono<Void> publishProfileVectorRefreshForCreate(UserDetails userDetails) {
        return userProfileVectorEventPublisher.publishRefreshEventForCreatedUser(
                        userDetails.getUserId(),
                        "USER_DETAILS",
                        "CREATE",
                        userDetails.getUserId(),
                        userDetails)
                .onErrorResume(error -> {
                    log.warn("|UserDetailsService|publishProfileVectorRefreshForCreate|failed|userId={}|error={}",
                            userDetails.getUserId(), error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> publishProfileVectorRefresh(String userId, String source, String operation, String resourceId) {
        return userProfileVectorEventPublisher.publishRefreshEvent(userId, source, operation, resourceId)
                .onErrorResume(error -> {
                    log.warn("|UserDetailsService|publishProfileVectorRefresh|failed|userId={}|source={}|operation={}|error={}",
                            userId, source, operation, error.getMessage());
                    return Mono.empty();
                });
    }

    /// Get UserDetails by userId with caching
    public Mono<UserDetails> getUserDetailsById(String userId) {
        log.info("|UserDetailsService|getUserDetailsById|fetching userDetails|userId={}", userId);

        String cacheKey = USER_DETAILS_CACHE_PREFIX + userId;

        // Try to get from cache firstA
        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .onErrorResume(error -> {
                    log.warn("|UserDetailsService|getUserDetailsById|cache read failed, fallback to database|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                })
                .flatMap(cachedJsonString -> {
                    // Cache hit - deserialize JSON string to UserDetails
                    UserDetails cachedUserDetails = RedisUtil.deserialize(cachedJsonString, UserDetails.class);
                    if (cachedUserDetails != null) {
                        log.info("|UserDetailsService|getUserDetailsById|found userDetails in cache|userId={}", userId);
                        return Mono.just(cachedUserDetails);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(
                        // Cache miss - fetch from database
                        userDetailsRepository.findById(userId)
                                .switchIfEmpty(Mono.error(new AppException(
                                        ErrorCode.USER_DETAILS_NOT_FOUND,
                                        String.format("User details not found for userId=%s", userId)
                                )))
                                .onErrorMap(throwable -> throwable instanceof AppException
                                        ? throwable
                                        : new AppException(
                                                ErrorCode.USER_DETAILS_FETCH_FAILED,
                                                String.format("Fetch user details failed for userId=%s", userId),
                                                throwable
                                        ))
                                .doOnSuccess(userDetails -> {
                                    log.info("|UserDetailsService|getUserDetailsById|found userDetails in database|userId={}", userId);
                                    // Cache the result as JSON string
                                    String jsonString = RedisUtil.serialize(userDetails);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL)
                                                .doOnSuccess(v -> log.info("|UserDetailsService|getUserDetailsById|cached userDetails|userId={}", userId))
                                                .doOnError(error -> log.error("|UserDetailsService|getUserDetailsById|failed to cache userDetails|userId={}|error={}", userId, error.getMessage()))
                                                .subscribe();
                                    }
                                })
                                .doOnError(error -> log.error("|UserDetailsService|getUserDetailsById|failed to fetch userDetails|userId={}|error={}", userId, error.getMessage()))
                );
    }

    /// Delete UserDetails by userId with cache eviction
    public Mono<Void> deleteUserDetails(String userId) {
        log.info("|UserDetailsService|deleteUserDetails|deleting userDetails|userId={}", userId);

        String cacheKey = USER_DETAILS_CACHE_PREFIX + userId;

        return userDetailsRepository.findById(userId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.USER_DETAILS_NOT_FOUND,
                        String.format("User details not found for userId=%s", userId)
                )))
                .flatMap(existing -> reactiveRedisStringTemplate.opsForValue().delete(cacheKey)
                        .onErrorResume(error -> {
                            log.warn("|UserDetailsService|deleteUserDetails|cache delete failed, continue|userId={}|error={}", userId, error.getMessage());
                            return Mono.empty();
                        })
                        .then(userDetailsRepository.deleteById(userId))
                        .then()
                        .doOnSuccess(v -> log.info("|UserDetailsService|deleteUserDetails|deleted userDetails from database|userId={}", userId))
                        .onErrorMap(throwable -> throwable instanceof AppException
                                ? throwable
                                : new AppException(
                                        ErrorCode.USER_DETAILS_DELETE_FAILED,
                                        String.format("Delete user details failed for userId=%s", userId),
                                        throwable
                                ))
                );
    }
}
