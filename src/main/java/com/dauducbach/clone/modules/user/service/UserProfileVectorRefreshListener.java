package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.constant.UserProfileVectorTopics;
import com.dauducbach.clone.modules.user.entity.UserDetailVector;
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
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserProfileVectorRefreshListener {
    private static final Logger log = LoggerFactory.getLogger(UserProfileVectorRefreshListener.class);

    UserDetailsRepository userDetailsRepository;
    UserJobRepository userJobRepository;
    UserHighSchoolRepository userHighSchoolRepository;
    UserUniversityRepository userUniversityRepository;
    ReactiveElasticsearchOperations elasticsearchOperations;
    GetVectorEmbedding getVectorEmbedding;

    @KafkaListener(topics = UserProfileVectorTopics.PROFILE_VECTOR_REFRESH, groupId = "user-service")
    public CompletableFuture<Void> handleProfileVectorRefreshEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        String source = KafkaUtils.extractString(json, "source");
        String operation = KafkaUtils.extractString(json, "operation");

        Mono<Void> refreshFlow = "CREATE".equalsIgnoreCase(operation) && json.has("profile") && json.get("profile").isJsonObject()
                ? refreshCreatedUserVector(json.getAsJsonObject("profile"))
                : refreshUserVector(userId);

        return refreshFlow
                .doOnSuccess(unused -> log.info("|UserProfileVectorRefreshListener|handleProfileVectorRefreshEvent|success|userId={}|source={}",
                        userId, source))
                .doOnError(error -> log.error("|UserProfileVectorRefreshListener|handleProfileVectorRefreshEvent|failed|userId={}|source={}|error={}",
                        userId, source, error.getMessage()))
                .toFuture();
    }

    public Mono<Void> refreshUserVector(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }

        String cleanUserId = userId.trim();
        return buildProfileText(cleanUserId)
                .filter(text -> !text.isBlank())
                .flatMap(getVectorEmbedding::getEmbedding)
                .filter(vector -> vector != null && !vector.isEmpty())
                .flatMap(vector -> saveUserVector(cleanUserId, vector))
                .doOnSuccess(unused -> log.info("|UserProfileVectorRefreshListener|refreshUserVector|completed|userId={}", cleanUserId))
                .onErrorResume(error -> {
                    log.error("|UserProfileVectorRefreshListener|refreshUserVector|failed|userId={}|error={}",
                            cleanUserId, error.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<Void> refreshCreatedUserVector(JsonObject profileJson) {
        if (profileJson == null) {
            return Mono.empty();
        }

        String userId = KafkaUtils.extractString(profileJson, "userId");
        if (userId.isBlank()) {
            return Mono.empty();
        }

        UserDetails details = UserDetails.builder()
                .userId(userId)
                .username(KafkaUtils.extractString(profileJson, "username"))
                .fullName(KafkaUtils.extractString(profileJson, "fullName"))
                .hometown(KafkaUtils.extractString(profileJson, "hometown"))
                .livingIn(KafkaUtils.extractString(profileJson, "livingIn"))
                .sex(KafkaUtils.extractString(profileJson, "sex"))
                .dob(parseOptionalLocalDate(KafkaUtils.extractString(profileJson, "dob")))
                .build();
        details.setHobbyList(extractHobbyList(profileJson));

        String profileText = buildProfileText(details, List.of(), List.of(), List.of());
        return Mono.just(profileText)
                .filter(text -> !text.isBlank())
                .flatMap(getVectorEmbedding::getEmbedding)
                .filter(vector -> vector != null && !vector.isEmpty())
                .flatMap(vector -> saveUserVector(userId, vector))
                .doOnSuccess(unused -> log.info("|UserProfileVectorRefreshListener|refreshCreatedUserVector|completed|userId={}", userId))
                .onErrorResume(error -> {
                    log.error("|UserProfileVectorRefreshListener|refreshCreatedUserVector|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.empty();
                });
    }

    Mono<String> buildProfileText(String userId) {
        Mono<UserDetails> detailsMono = userDetailsRepository.findById(userId).defaultIfEmpty(UserDetails.builder().userId(userId).build());
        Mono<List<UserJob>> jobsMono = userJobRepository.findByUserId(userId).collectList();
        Mono<List<UserHighSchool>> highSchoolsMono = userHighSchoolRepository.findByUserId(userId).collectList();
        Mono<List<UserUniversity>> universitiesMono = userUniversityRepository.findByUserId(userId).collectList();

        return Mono.zip(detailsMono, jobsMono, highSchoolsMono, universitiesMono)
                .map(tuple -> buildProfileText(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4()));
    }

    private List<String> extractHobbyList(JsonObject json) {
        List<String> hobbyList = KafkaUtils.extractStringList(json, "hobbyList");
        if (!hobbyList.isEmpty()) {
            return hobbyList;
        }
        return KafkaUtils.extractStringList(json, "hobbieList");
    }

    private Mono<Void> saveUserVector(String userId, List<Double> vector) {
        return elasticsearchOperations.get(userId, UserDetailVector.class)
                .defaultIfEmpty(UserDetailVector.builder().userId(userId).build())
                .doOnNext(userDetailVector -> userDetailVector.setUserVector(vector))
                .flatMap(elasticsearchOperations::save)
                .then();
    }

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

        jobs.forEach(job -> append(builder, "job", joinParts(
                job.getPosition(),
                job.getCompanyName(),
                formatDate(job.getFromDate()),
                formatDate(job.getToDate())
        )));

        highSchools.forEach(highSchool -> append(builder, "high_school", joinParts(
                highSchool.getSchoolName(),
                highSchool.isGraduate() ? "graduated" : "not graduated",
                formatDate(highSchool.getFromDate()),
                formatDate(highSchool.getToDate())
        )));

        universities.forEach(university -> append(builder, "university", joinParts(
                university.getSchoolName(),
                university.getMajor(),
                university.isGraduate() ? "graduated" : "not graduated",
                formatDate(university.getFrom()),
                formatDate(university.getTo())
        )));

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

    private LocalDate parseOptionalLocalDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(value);
        } catch (Exception error) {
            log.warn("|UserProfileVectorRefreshListener|parseOptionalLocalDate|invalid date={}", value);
            return null;
        }
    }
}
