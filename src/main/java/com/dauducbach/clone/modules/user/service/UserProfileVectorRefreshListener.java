package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.constant.UserProfileVectorTopics;
import com.dauducbach.clone.infrastructure.vector.UserVectorCoordinator;
import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserHighSchoolRepository;
import com.dauducbach.clone.modules.user.repositoty.UserJobRepository;
import com.dauducbach.clone.modules.user.repositoty.UserUniversityRepository;
import com.dauducbach.clone.utils.GetVectorEmbedding;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import reactor.util.retry.Retry;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserProfileVectorRefreshListener {
    private static final Logger log = LoggerFactory.getLogger(UserProfileVectorRefreshListener.class);

    UserDetailsRepository userDetailsRepository;
    UserJobRepository userJobRepository;
    UserHighSchoolRepository userHighSchoolRepository;
    UserUniversityRepository userUniversityRepository;
    GetVectorEmbedding getVectorEmbedding;
    UserVectorCoordinator coordinator;
    UserVectorOperationService operations;

    @KafkaListener(topics = UserProfileVectorTopics.PROFILE_VECTOR_REFRESH, groupId = "user-service")
    public CompletableFuture<Void> handleProfileVectorRefreshEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        String source = KafkaUtils.extractString(json, "source");
        String eventId = KafkaUtils.extractString(json, "eventId");
        // Legacy queued events retain a deterministic identity on redelivery too.
        String identity = eventId.isBlank() ? fingerprint(payload) : eventId;
        Mono<Void> refreshFlow = refreshUserVector(userId, identity);

        return refreshFlow
                .doOnSuccess(unused -> log.info("|UserProfileVectorRefreshListener|handleProfileVectorRefreshEvent|success|userId={}|source={}",
                        userId, source))
                .doOnError(error -> log.error("|UserProfileVectorRefreshListener|handleProfileVectorRefreshEvent|failed|userId={}|source={}|error={}",
                        userId, source, error.getMessage()))
                .toFuture();
    }

    public Mono<Void> refreshUserVector(String userId) {
        return refreshUserVector(userId, UUID.randomUUID().toString());
    }

    private Mono<Void> refreshUserVector(String userId, String eventId) {
        if (userId == null || userId.isBlank()) return Mono.empty();
        String cleanUserId = userId.trim();
        // Each retry rebuilds outside the lease, including retry after a changed source/OCC.
        return Mono.defer(() -> buildProfileText(cleanUserId)
                .flatMap(text -> getVectorEmbedding.getEmbedding(text)
                        .switchIfEmpty(Mono.error(new IllegalStateException("Embedding returned no vector")))
                        .map(VectorMath::normalize)
                        .flatMap(vector -> coordinator.withUserLock(cleanUserId, lease ->
                                buildProfileText(cleanUserId).flatMap(current -> {
                                    if (!current.equals(text)) return Mono.error(new ProfileSourceChangedException());
                                    String key = "profile:" + fingerprint(cleanUserId) + ":" + fingerprint(eventId) + ":" + fingerprint(current);
                                    return operations.applyProfile(lease, key, vector).then();
                                }))))).retryWhen(Retry.max(3)
                        .filter(error -> error instanceof ProfileSourceChangedException
                                || error instanceof UserVectorStore.RetryableVectorConflictException)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
    }

    /** Compatibility entry point: CREATE snapshots are hints; SQL remains authoritative. */
    public Mono<Void> refreshCreatedUserVector(JsonObject profileJson) {
        if (profileJson == null) return Mono.empty();
        String userId = KafkaUtils.extractString(profileJson, "userId");
        return refreshUserVector(userId, "profile-create:" + userId);
    }

    Mono<String> buildProfileText(String userId) {
        // Absence must remain empty; a synthetic profile could resurrect a deleted user.
        return userDetailsRepository.findById(userId).flatMap(details -> Mono.zip(
                userJobRepository.findByUserId(userId).collectList(),
                userHighSchoolRepository.findByUserId(userId).collectList(),
                userUniversityRepository.findByUserId(userId).collectList())
                .map(tuple -> buildProfileText(details, tuple.getT1(), tuple.getT2(), tuple.getT3())));
    }

    private String fingerprint(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }

    private static class ProfileSourceChangedException extends RuntimeException {}

    private String buildProfileText(UserDetails details,
                                    List<UserJob> jobs,
                                    List<UserHighSchool> highSchools,
                                    List<UserUniversity> universities) {
        StringBuilder builder = new StringBuilder();

        append(builder, "full_name", details.getFullName());
        append(builder, "username", details.getUsername());
        append(builder, "hometown", details.getHometown());
        append(builder, "living_in", details.getLivingIn());
        append(builder, "sex", details.getSex());
        append(builder, "date_of_birth", formatDate(details.getDob()));
        append(builder, "hobbies", String.join(", ", details.getHobbyList()));

        jobs.stream().map(job -> joinParts(
                job.getPosition(),
                job.getCompanyName(),
                formatDate(job.getFromDate()),
                formatDate(job.getToDate())
        )).sorted().forEach(value -> append(builder, "job", value));

        highSchools.stream().map(highSchool -> joinParts(
                highSchool.getSchoolName(),
                highSchool.isGraduate() ? "graduated" : "not graduated",
                formatDate(highSchool.getFromDate()),
                formatDate(highSchool.getToDate())
        )).sorted().forEach(value -> append(builder, "high_school", value));

        universities.stream().map(university -> joinParts(
                university.getSchoolName(),
                university.getMajor(),
                university.isGraduate() ? "graduated" : "not graduated",
                formatDate(university.getFrom()),
                formatDate(university.getTo())
        )).sorted().forEach(value -> append(builder, "university", value));

        return builder.toString().trim();
    }

    private void append(StringBuilder builder, String label, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        if (!builder.isEmpty()) {
            builder.append(". ");
        }
        builder.append(label).append(": ").append(value.trim());
    }

    private String joinParts(String... parts) {
        return java.util.Arrays.stream(parts)
                .filter(part -> part != null && !part.isBlank())
                .map(String::trim)
                .reduce((left, right) -> left + ", " + right)
                .orElse("");
    }

    private String formatDate(LocalDate date) {
        return date == null ? "" : date.toString();
    }

}
