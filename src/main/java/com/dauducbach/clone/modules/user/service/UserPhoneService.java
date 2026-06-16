package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.request.UserPhoneRequest;
import com.dauducbach.clone.modules.user.entity.UserPhone;
import com.dauducbach.clone.modules.user.repositoty.UserPhoneRepository;
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

public class UserPhoneService {
    UserPhoneRepository userPhoneRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;

    private static final Logger log = LoggerFactory.getLogger(UserPhoneService.class);
    private static final String CACHE_PREFIX = "user_phone:";
    private static final String LIST_CACHE_PREFIX = "user_phone_list:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /// Tạo mới UserPhone
    public Mono<UserPhone> createUserPhone(UserPhoneRequest request) {
        log.info("|UserPhoneService|createUserPhone|userId={}", request.getUserId());

        String id = UUID.randomUUID().toString();
        String cacheKey = CACHE_PREFIX + id;
        String listCacheKey = LIST_CACHE_PREFIX + request.getUserId();

        UserPhone userPhone = UserPhone.builder()
                .id(id)
                .userId(request.getUserId())
                .phoneNum(request.getPhoneNum())
                .isVerified(false) // Default false
                .build();

        return r2dbcEntityTemplate.insert(UserPhone.class)
                .using(userPhone)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.USER_PHONE_SAVE_FAILED,
                        String.format("Save user phone failed for userId=%s", request.getUserId()),
                        throwable
                ))
                .doOnSuccess(savedPhone -> {
                    log.info("|UserPhoneService|createUserPhone|created user phone|id={}", savedPhone.getId());
                    String jsonString = RedisUtil.serialize(savedPhone);
                    if (jsonString != null) {
                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                    }
                    reactiveRedisStringTemplate.opsForValue().delete(listCacheKey).subscribe();
                })
                .doOnError(error -> log.error("|UserPhoneService|createUserPhone|failed to create|error={}", error.getMessage()));
    }

    /// Lấy UserPhone theo ID
    public Mono<UserPhone> getUserPhoneById(String id) {
        log.info("|UserPhoneService|getUserPhoneById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .onErrorResume(error -> {
                    log.warn("|UserPhoneService|getUserPhoneById|cache read failed, fallback to database|id={}|error={}", id, error.getMessage());
                    return Mono.empty();
                })
                .flatMap(cachedJsonString -> {
                    UserPhone cached = RedisUtil.deserialize(cachedJsonString, UserPhone.class);
                    if (cached != null) {
                        log.info("|UserPhoneService|getUserPhoneById|found in cache|id={}", id);
                        return Mono.just(cached);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(
                        userPhoneRepository.findById(id)
                                .switchIfEmpty(Mono.error(new AppException(
                                        ErrorCode.USER_PHONE_NOT_FOUND,
                                        String.format("User phone not found for id=%s", id)
                                )))
                                .onErrorMap(throwable -> throwable instanceof AppException
                                        ? throwable
                                        : new AppException(
                                                ErrorCode.USER_PHONE_FETCH_FAILED,
                                                String.format("Fetch user phone failed for id=%s", id),
                                                throwable
                                        ))
                                .doOnSuccess(phone -> {
                                    log.info("|UserPhoneService|getUserPhoneById|found in database|id={}", id);
                                    String jsonString = RedisUtil.serialize(phone);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(cacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .doOnError(error -> log.error("|UserPhoneService|getUserPhoneById|failed to fetch|id={}|error={}", id, error.getMessage()))
                );
    }

    /// Lấy danh sách UserPhone của user
    public Flux<UserPhone> getUserPhonesByUserId(String userId) {
        log.info("|UserPhoneService|getUserPhonesByUserId|userId={}", userId);

        String listCacheKey = LIST_CACHE_PREFIX + userId;

        return reactiveRedisStringTemplate.opsForValue().get(listCacheKey)
                .onErrorResume(error -> {
                    log.warn("|UserPhoneService|getUserPhonesByUserId|cache read failed, fallback to database|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                })
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserPhoneService|getUserPhonesByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(RedisUtil.deserializeList(cachedJsonString, UserPhone.class));
                    }
                    return Flux.empty();
                })
                .switchIfEmpty(
                        userPhoneRepository.findByUserId(userId)
                                .collectList()
                                .onErrorMap(throwable -> new AppException(
                                        ErrorCode.USER_PHONE_FETCH_FAILED,
                                        String.format("Fetch user phones failed for userId=%s", userId),
                                        throwable
                                ))
                                .doOnNext(phoneList -> {
                                    log.info("|UserPhoneService|getUserPhonesByUserId|found {} items in database|userId={}", phoneList.size(), userId);
                                    String jsonString = RedisUtil.serialize(phoneList);
                                    if (jsonString != null) {
                                        reactiveRedisStringTemplate.opsForValue().set(listCacheKey, jsonString, CACHE_TTL).subscribe();
                                    }
                                })
                                .flatMapMany(Flux::fromIterable)
                                .doOnError(error -> log.error("|UserPhoneService|getUserPhonesByUserId|failed to fetch|userId={}|error={}", userId, error.getMessage()))
                );
    }

    /// Xóa UserPhone
    public Mono<Void> deleteUserPhone(String id) {
        log.info("|UserPhoneService|deleteUserPhone|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return userPhoneRepository.findById(id)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.USER_PHONE_NOT_FOUND,
                        String.format("User phone not found for id=%s", id)
                )))
                .flatMap(phone -> userPhoneRepository.deleteById(id)
                        .doOnSuccess(v -> {
                            log.info("|UserPhoneService|deleteUserPhone|deleted|id={}", id);
                            reactiveRedisStringTemplate.opsForValue().delete(cacheKey).subscribe();
                            reactiveRedisStringTemplate.opsForValue().delete(LIST_CACHE_PREFIX + phone.getUserId()).subscribe();
                        })
                        .doOnError(error -> log.error("|UserPhoneService|deleteUserPhone|failed to delete|id={}|error={}", id, error.getMessage()))
                        .onErrorMap(throwable -> throwable instanceof AppException
                                ? throwable
                                : new AppException(
                                        ErrorCode.USER_PHONE_DELETE_FAILED,
                                        String.format("Delete user phone failed for id=%s", id),
                                        throwable
                                ))
                );
    }
}