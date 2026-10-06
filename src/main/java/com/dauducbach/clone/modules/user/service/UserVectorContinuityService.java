package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.user.entity.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserVectorUpdateOperationRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Repository-only boundary: call after pending journal recovery, under the existing user lease. */
@Service
@RequiredArgsConstructor
public class UserVectorContinuityService {
    private static final Logger log = LoggerFactory.getLogger(UserVectorContinuityService.class);
    private final UserVectorUpdateOperationRepository journal;
    private final UserDetailsRepository users;
    private final UserVectorQueryService query;
    private final UserVectorCoordinator coordinator;
    private final VectorRedisState redis;

    public Mono<Void> requireContinuity(VectorLease lease) {
        return coordinator.requireOwner(lease).then(users.existsById(lease.userId()))
                .flatMap(exists -> exists ? journal.findLatestCompletedVector(lease.userId())
                        .flatMap(operation -> verify(lease, operation)) : Mono.empty())
                .doOnError(VectorRepairRequiredException.class, error -> log.error(
                        "|UserVectorContinuityService|quarantined|userId={}|reason={}", lease.userId(), error.getMessage()));
    }

    private Mono<Void> verify(VectorLease lease, UserVectorUpdateOperation operation) {
        return Mono.zip(redis.operation(lease.userId()).defaultIfEmpty(""), redis.version(lease.userId()),
                        query.getSnapshot(lease.userId()).switchIfEmpty(Mono.error(quarantine("Acknowledged ES document missing"))))
                .flatMap(state -> {
                    var document = state.getT3();
                    String currentOperation = "PROFILE".equals(operation.getOperationKind())
                            ? document.getUserProfileOperationId() : document.getUserLongTermOperationId();
                    if (Boolean.TRUE.equals(document.getDeleted()) || !operation.getOperationId().equals(currentOperation))
                        return Mono.error(quarantine("Acknowledged ES operation missing or changed"));
                    if (!operation.getOperationId().equals(state.getT1()))
                        return Mono.error(quarantine("Redis committed journal marker missing or rolled back"));
                    if (document.getVectorVersion() == null || state.getT2() < document.getVectorVersion())
                        return Mono.error(quarantine("Redis vector version below acknowledged ES vector version"));
                    return coordinator.requireOwner(lease);
                });
    }

    private VectorRepairRequiredException quarantine(String reason) {
        return new VectorRepairRequiredException(reason + "; quarantine user and restore consistent durable state; never replay COMPLETED rows");
    }
}
