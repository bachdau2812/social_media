package com.dauducbach.clone.modules.user.service;

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

import java.time.Duration;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserDetailsService {
    UserDetailsRepository userDetailsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;

    private static final Logger log = LoggerFactory.getLogger(UserDetailsService.class);
    private static final String USER_DETAILS_CACHE_PREFIX = "user_details_info:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Listen event and create profile for new user
    @KafkaListener(topics = "profile_creation_event", groupId = "user-service")
    public void createUserDetails (@Payload String payload) {
        log.info("|UserDetailsService|createUserDetails|received payload={}", payload);

        JsonObject payloadJson = GsonUtils.fromString(payload);

        var userDetails = UserDetails.builder()
                .userId(KafkaUtils.extractString(payloadJson, "userId"))
                .username(KafkaUtils.extractString(payloadJson, "username"))
                .dob(KafkaUtils.extractLocalDate(payloadJson, "dob"))
                .hometown(KafkaUtils.extractString(payloadJson, "hometown"))
                .livingIn(KafkaUtils.extractString(payloadJson, "livingIn"))
                .sex(KafkaUtils.extractString(payloadJson, "sex"))
                .build();
        userDetails.setHobbyList(KafkaUtils.extractStringList(payloadJson, "hobbieList"));

        insertUserDetails(userDetails).subscribe();
    }

    /// Insert UserDetails with caching
    public Mono<UserDetails> insertUserDetails(UserDetails userDetails) {
        log.info("|UserDetailsService|insertUserDetails|saving userDetails to database|userId={}", userDetails.getUserId());

        String cacheKey = USER_DETAILS_CACHE_PREFIX + userDetails.getUserId();

        return r2dbcEntityTemplate.insert(UserDetails.class)
                .using(userDetails)
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
                        return Mono.error(new RuntimeException("User not found with id: " + request.getUserId()));
                    }

                    // Get existing user details
                    return userDetailsRepository.findById(request.getUserId());
                })
                .flatMap(existingUserDetails -> {
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

    /// Get UserDetails by userId with caching
    public Mono<UserDetails> getUserDetailsById(String userId) {
        log.info("|UserDetailsService|getUserDetailsById|fetching userDetails|userId={}", userId);

        String cacheKey = USER_DETAILS_CACHE_PREFIX + userId;

        // Try to get from cache first
        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
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
                                .switchIfEmpty(Mono.error(new RuntimeException("User not found with id: " + userId)))
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

        // Delete from cache
        return reactiveRedisStringTemplate.opsForValue().delete(cacheKey)
                .doOnSuccess(v -> log.info("|UserDetailsService|deleteUserDetails|deleted cache|userId={}", userId))
                .doOnError(error -> log.error("|UserDetailsService|deleteUserDetails|failed to delete cache|userId={}|error={}", userId, error.getMessage()))
                .then(
                        // Delete from database
                        userDetailsRepository.deleteById(userId)
                                .doOnSuccess(v -> log.info("|UserDetailsService|deleteUserDetails|deleted userDetails from database|userId={}", userId))
                                .doOnError(error -> log.error("|UserDetailsService|deleteUserDetails|failed to delete userDetails from database|userId={}|error={}", userId, error.getMessage()))
                );
    }
}
