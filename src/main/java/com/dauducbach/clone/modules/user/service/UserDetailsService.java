package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.user.dto.request.UserDetailsUpdateRequest;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import com.dauducbach.clone.modules.user.publicapi.UserDeletionCleanup;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.dauducbach.clone.commons.serialization.JsonPayloadReader;
import com.dauducbach.clone.modules.user.profile.application.ProfileCache;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserDetailsService {
    UserDetailsRepository userDetailsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ProfileCache profileCache;
    AuditRecorder auditRecorder;
    UserProfileVectorEventPublisher userProfileVectorEventPublisher;
    UserDeletionCleanup vectorCleanup;

    private static final Logger log = LoggerFactory.getLogger(UserDetailsService.class);

    /// Listen event and create profile for new user
    @KafkaListener(topics = "profile_creation_event", groupId = "user-service")
    public CompletableFuture<Void> createUserDetails (@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);

        var userDetails = UserDetails.builder()
                .userId(JsonPayloadReader.extractString(payloadJson, "userId"))
                .username(JsonPayloadReader.extractString(payloadJson, "username"))
                .fullName(JsonPayloadReader.extractString(payloadJson, "fullName"))
                .dob(JsonPayloadReader.extractLocalDate(payloadJson, "dob"))
                .hometown(JsonPayloadReader.extractString(payloadJson, "hometown"))
                .livingIn(JsonPayloadReader.extractString(payloadJson, "livingIn"))
                .sex(JsonPayloadReader.extractString(payloadJson, "sex"))
                .build();
        userDetails.setHobbyList(extractHobbyList(payloadJson));
        log.info("|UserDetailsService|createUserDetails|received|userId={}|username={}",
                userDetails.getUserId(), userDetails.getUsername());

        return userDetailsRepository.findById(userDetails.getUserId())
                .flatMap(current -> publishProfileVectorRefreshForCreate(current).thenReturn(current))
                .switchIfEmpty(Mono.defer(() -> insertUserDetails(userDetails)))
                .doOnSuccess(saved -> log.info("|UserDetailsService|createUserDetails|created user details|userId={}", saved.getUserId()))
                .doOnError(error -> log.error("|UserDetailsService|createUserDetails|failed to create user details|userId={}|error={}",
                        userDetails.getUserId(), error.getMessage()))
                .then()
                .toFuture();
    }

    /// Insert UserDetails with caching
    public Mono<UserDetails> insertUserDetails(UserDetails userDetails) {
        log.info("|UserDetailsService|insertUserDetails|saving userDetails to database|userId={}", userDetails.getUserId());

        return r2dbcEntityTemplate.insert(UserDetails.class)
                .using(userDetails)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.USER_DETAILS_SAVE_FAILED,
                        String.format("Save user details failed for userId=%s", userDetails.getUserId()),
                        throwable
                ))
                .doOnSuccess(saved -> log.info("|UserDetailsService|insertUserDetails|saved userDetails to database|userId={}", saved.getUserId()))
                .flatMap(saved -> putProfileCacheBestEffort(saved).thenReturn(saved))
                .flatMap(savedUserDetails -> publishProfileVectorRefreshForCreate(savedUserDetails)
                        .thenReturn(savedUserDetails))
                .doOnError(error -> log.error("|UserDetailsService|insertUserDetails|failed to save userDetails to database|error={}", error.getMessage()));
    }

    /// Update UserDetails with partial field update
    public Mono<UserDetails> updateUserDetails(UserDetailsUpdateRequest request) {
        log.info("|UserDetailsService|updateUserDetails|updating userDetails|userId={}", request.getUserId());

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
                    if (request.getFullName() != null && !request.getFullName().isBlank()) {
                        existingUserDetails.setFullName(request.getFullName());
                    }
                    if (request.getUsername() != null && !request.getUsername().isBlank()) {
                        existingUserDetails.setUsername(request.getUsername());
                    }
                    if (request.getDob() != null) {
                        existingUserDetails.setDob(request.getDob());
                    }
                    if (request.getHomeTown() != null) {
                        existingUserDetails.setHometown(request.getHomeTown().isBlank() ? null : request.getHomeTown().trim());
                    }
                    if (request.getLivingIn() != null) {
                        existingUserDetails.setLivingIn(request.getLivingIn().isBlank() ? null : request.getLivingIn().trim());
                    }
                    if (request.getSex() != null && !request.getSex().isBlank()) {
                        existingUserDetails.setSex(request.getSex());
                    }
                    if (request.getHobbieList() != null) {
                        existingUserDetails.setHobbyList(request.getHobbieList());
                    }

                    return userDetailsRepository.save(existingUserDetails);
                })
                .doOnSuccess(updated -> log.info("|UserDetailsService|updateUserDetails|updated userDetails successfully|userId={}", updated.getUserId()))
                .flatMap(updated -> putProfileCacheBestEffort(updated).thenReturn(updated))
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
                .doOnError(error -> log.error("|UserDetailsService|updateUserDetails|failed to update userDetails|error={}", error.getMessage()));
    }

    private Mono<Void> saveUpdateUserDetailsAudit(UserDetailsUpdateRequest request, String userId) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("fullNameChanged", request.getFullName() != null && !request.getFullName().isBlank());
        metadata.addProperty("usernameChanged", request.getUsername() != null && !request.getUsername().isBlank());
        metadata.addProperty("dobChanged", request.getDob() != null);
        metadata.addProperty("hometownChanged", request.getHomeTown() != null && !request.getHomeTown().isBlank());
        metadata.addProperty("livingInChanged", request.getLivingIn() != null && !request.getLivingIn().isBlank());
        metadata.addProperty("sexChanged", request.getSex() != null && !request.getSex().isBlank());
        metadata.addProperty("hobbyChanged", request.getHobbieList() != null && !request.getHobbieList().isEmpty());

        return auditRecorder.record(new AuditEntry(userId, AuditActionType.UPDATE_USER_DETAILS,
                "USER_DETAILS", userId, "SUCCESS", metadata.toString(), null));
    }

    private java.util.List<String> extractHobbyList(JsonObject payloadJson) {
        java.util.List<String> hobbyList = JsonPayloadReader.extractStringList(payloadJson, "hobbyList");
        if (!hobbyList.isEmpty()) {
            return hobbyList;
        }
        return JsonPayloadReader.extractStringList(payloadJson, "hobbieList");
    }

    private Mono<Void> publishProfileVectorRefreshForCreate(UserDetails userDetails) {
        return userProfileVectorEventPublisher.publishRefreshEventForCreatedUser(
                        userDetails.getUserId(),
                        "USER_DETAILS",
                        "CREATE",
                        userDetails.getUserId(),
                        userDetails);
    }

    private Mono<Void> publishProfileVectorRefresh(String userId, String source, String operation, String resourceId) {
        return userProfileVectorEventPublisher.publishRefreshEvent(userId, source, operation, resourceId);
    }

    /// Get UserDetails by userId with caching
    public Mono<UserDetails> getUserDetailsById(String userId) {
        log.info("|UserDetailsService|getUserDetailsById|fetching userDetails|userId={}", userId);

        // Try to get from cache firstA
        return profileCache.find(userId)
                .onErrorResume(error -> {
                    log.warn("|UserDetailsService|getUserDetailsById|cache read failed, fallback to database|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                })
                .doOnNext(cached -> log.info("|UserDetailsService|getUserDetailsById|found userDetails in cache|userId={}", userId))
                .switchIfEmpty(Mono.defer(() ->
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
                                .flatMap(userDetails -> putProfileCacheBestEffort(userDetails).thenReturn(userDetails))
                                .doOnNext(userDetails -> log.info("|UserDetailsService|getUserDetailsById|found userDetails in database|userId={}", userId))
                ))
                .doOnError(error -> log.error("|UserDetailsService|getUserDetailsById|failed to fetch userDetails|userId={}|error={}", userId, error.getMessage()));
    }

    public Mono<Boolean> userExists(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(false);
        }
        return userDetailsRepository.existsById(userId.trim()).defaultIfEmpty(false);
    }

    public Flux<UserDetails> getUserDetailsByIds(Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Flux.empty();
        }
        return userDetailsRepository.findAllById(userIds);
    }

    /// Delete UserDetails by userId with cache eviction
    public Mono<Void> deleteUserDetails(String userId) {
        log.info("|UserDetailsService|deleteUserDetails|deleting userDetails|userId={}", userId);

        return vectorCleanup.deleteUser(userId,
                        () -> userDetailsRepository.deleteById(userId),
                        () -> evictProfileCacheBestEffort(userId))
                .then()
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.USER_DETAILS_DELETE_FAILED,
                        String.format("Delete user details failed for userId=%s", userId), error));
    }

    private Mono<Void> putProfileCacheBestEffort(UserDetails userDetails) {
        return profileCache.put(userDetails)
                .doOnSuccess(unused -> log.info("|UserDetailsService|profile cache updated|userId={}", userDetails.getUserId()))
                .onErrorResume(error -> {
                    log.warn("|UserDetailsService|profile cache write failed, continue|userId={}|error={}",
                            userDetails.getUserId(), error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> evictProfileCacheBestEffort(String userId) {
        return profileCache.evict(userId)
                .onErrorResume(error -> {
                    log.warn("|UserDetailsService|profile cache delete failed, continue|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.empty();
                });
    }
}
