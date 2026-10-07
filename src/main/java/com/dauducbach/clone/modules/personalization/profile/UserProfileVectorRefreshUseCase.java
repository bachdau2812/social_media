package com.dauducbach.clone.modules.personalization.profile;

import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.HighSchoolSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.JobSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.ProfileContentSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UniversitySnapshot;
import com.dauducbach.clone.modules.embedding.publicapi.TextEmbeddingProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/** Builds the derived profile preference from user-owned snapshots and applies it under the vector lease. */
@Service
@RequiredArgsConstructor
public class UserProfileVectorRefreshUseCase {
    private final UserProfileQuery profiles;
    private final TextEmbeddingProvider embeddings;
    private final UserVectorCoordinator coordinator;
    private final PreferenceRecoveryCoordinator recovery;
    private final UserVectorOperationService operations;

    public Mono<Void> refreshUserVector(String userId) {
        return refreshUserVector(userId, UUID.randomUUID().toString());
    }

    public Mono<Void> refreshUserVector(String userId, String eventId) {
        if (userId == null || userId.isBlank()) return Mono.empty();
        String cleanUserId = userId.trim();
        return Mono.defer(() -> buildProfileText(cleanUserId)
                .flatMap(text -> embeddings.getEmbedding(text)
                        .switchIfEmpty(Mono.error(new IllegalStateException("Embedding returned no vector")))
                        .map(VectorMath::normalize)
                        .flatMap(vector -> coordinator.withUserLock(cleanUserId, lease ->
                                buildProfileText(cleanUserId).flatMap(current -> {
                                    if (!current.equals(text)) return Mono.error(new ProfileSourceChangedException());
                                    String key = "profile:" + fingerprint(cleanUserId) + ":" + fingerprint(eventId) + ":" + fingerprint(current);
                                    return recovery.reconcilePending(lease)
                                            .then(operations.applyProfile(lease, key, vector)).then();
                                })))))
                .retryWhen(Retry.max(3)
                        .filter(error -> error instanceof ProfileSourceChangedException
                                || error instanceof UserVectorStore.RetryableVectorConflictException)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
    }

    Mono<String> buildProfileText(String userId) {
        return profiles.getProfileContentSnapshot(userId).map(this::buildProfileText);
    }

    private String buildProfileText(ProfileContentSnapshot snapshot) {
        var details = snapshot.user();
        StringBuilder builder = new StringBuilder();
        append(builder, "full_name", details.fullName());
        append(builder, "username", details.username());
        append(builder, "hometown", details.hometown());
        append(builder, "living_in", details.livingIn());
        append(builder, "sex", details.sex());
        append(builder, "date_of_birth", formatDate(details.dob()));
        append(builder, "hobbies", String.join(", ", details.hobbyList()));

        snapshot.jobs().stream().map(this::jobText).sorted().forEach(value -> append(builder, "job", value));
        snapshot.highSchools().stream().map(this::highSchoolText).sorted().forEach(value -> append(builder, "high_school", value));
        snapshot.universities().stream().map(this::universityText).sorted().forEach(value -> append(builder, "university", value));
        return builder.toString().trim();
    }

    private String jobText(JobSnapshot job) {
        return joinParts(job.position(), job.companyName(), formatDate(job.fromDate()), formatDate(job.toDate()));
    }

    private String highSchoolText(HighSchoolSnapshot school) {
        return joinParts(school.schoolName(), school.graduate() ? "graduated" : "not graduated",
                formatDate(school.fromDate()), formatDate(school.toDate()));
    }

    private String universityText(UniversitySnapshot university) {
        return joinParts(university.schoolName(), university.major(), university.graduate() ? "graduated" : "not graduated",
                formatDate(university.from()), formatDate(university.to()));
    }

    private void append(StringBuilder builder, String label, String value) {
        if (value == null || value.isBlank()) return;
        if (!builder.isEmpty()) builder.append(". ");
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

    private String fingerprint(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    private static class ProfileSourceChangedException extends RuntimeException {
    }
}
