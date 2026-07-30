package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.UserDetailVector;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserVectorQueryService {
    private static final Logger log = LoggerFactory.getLogger(UserVectorQueryService.class);

    ReactiveElasticsearchOperations elasticsearchOperations;

    public Mono<List<Double>> getLongTermVector(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(List.of());
        }

        return elasticsearchOperations.get(userId, UserDetailVector.class)
                .map(UserDetailVector::getUserLongTermVector)
                .filter(vector -> vector != null && !vector.isEmpty())
                .defaultIfEmpty(List.of())
                .onErrorResume(error -> {
                    log.warn("|UserVectorQueryService|getLongTermVector|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    public Mono<List<Double>> getUserVector(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(List.of());
        }

        return elasticsearchOperations.get(userId, UserDetailVector.class)
                .map(UserDetailVector::getUserVector)
                .filter(vector -> vector != null && !vector.isEmpty())
                .defaultIfEmpty(List.of())
                .onErrorResume(error -> {
                    log.warn("|UserVectorQueryService|getUserVector|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    public Mono<List<Double>> getLongTermOrUserVector(String userId) {
        return getLongTermVector(userId)
                .flatMap(vector -> vector.isEmpty() ? getUserVector(userId) : Mono.just(vector));
    }

    public Mono<Void> saveLongTermVector(String userId, List<Double> vector) {
        if (userId == null || userId.isBlank() || vector == null || vector.isEmpty()) {
            return Mono.empty();
        }

        return elasticsearchOperations.get(userId, UserDetailVector.class)
                .defaultIfEmpty(UserDetailVector.builder().userId(userId).build())
                .doOnNext(userDetailVector -> userDetailVector.setUserLongTermVector(vector))
                .flatMap(elasticsearchOperations::save)
                .doOnSuccess(saved -> log.info("|UserVectorQueryService|saveLongTermVector|saved|userId={}", userId))
                .doOnError(error -> log.error("|UserVectorQueryService|saveLongTermVector|failed|userId={}|error={}",
                        userId, error.getMessage()))
                .onErrorResume(error -> Mono.empty())
                .then();
    }
}