package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.request.UserPhoneRequest;
import com.dauducbach.clone.modules.user.entity.UserPhone;
import com.dauducbach.clone.modules.user.profile.application.ProfileDataCache;
import com.dauducbach.clone.modules.user.repository.UserPhoneRepository;
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

public class UserPhoneService {
    UserPhoneRepository userPhoneRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ProfileDataCache profileDataCache;

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
                .flatMap(savedPhone -> profileDataCache.put(cacheKey, savedPhone, CACHE_TTL)
                        .then(profileDataCache.evict(listCacheKey))
                        .thenReturn(savedPhone))
                .doOnSuccess(savedPhone -> log.info("|UserPhoneService|createUserPhone|created user phone|id={}", savedPhone.getId()))
                .doOnError(error -> log.error("|UserPhoneService|createUserPhone|failed to create|error={}", error.getMessage()));
    }

    /// Lấy UserPhone theo ID
    public Mono<UserPhone> getUserPhoneById(String id) {
        log.info("|UserPhoneService|getUserPhoneById|id={}", id);

        String cacheKey = CACHE_PREFIX + id;

        return profileDataCache.find(cacheKey, UserPhone.class)
                .doOnNext(cached -> log.info("|UserPhoneService|getUserPhoneById|found in cache|id={}", id))
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
                                .flatMap(phone -> profileDataCache.put(cacheKey, phone, CACHE_TTL).thenReturn(phone)
                                        .doOnNext(ignored -> log.info("|UserPhoneService|getUserPhoneById|found in database|id={}", id)))
                                .doOnError(error -> log.error("|UserPhoneService|getUserPhoneById|failed to fetch|id={}|error={}", id, error.getMessage()))
                );
    }

    /// Lấy danh sách UserPhone của user
    public Flux<UserPhone> getUserPhonesByUserId(String userId) {
        log.info("|UserPhoneService|getUserPhonesByUserId|userId={}", userId);

        String listCacheKey = LIST_CACHE_PREFIX + userId;

        return profileDataCache.findList(listCacheKey, UserPhone.class)
                .flatMapMany(cachedJsonString -> {
                    if (cachedJsonString != null) {
                        log.info("|UserPhoneService|getUserPhonesByUserId|found list in cache|userId={}", userId);
                        return Flux.fromIterable(cachedJsonString);
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
                                })
                                .flatMapMany(phoneList -> profileDataCache.put(listCacheKey, phoneList, CACHE_TTL)
                                        .thenMany(Flux.fromIterable(phoneList)))
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
                        })
                        .then(profileDataCache.evict(cacheKey))
                        .then(profileDataCache.evict(LIST_CACHE_PREFIX + phone.getUserId()))
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
