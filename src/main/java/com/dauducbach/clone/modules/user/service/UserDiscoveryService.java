package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.infrastructure.SemanticVectorSearchService;
import com.dauducbach.clone.modules.user.dto.response.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class UserDiscoveryService {
    private static final Logger log = LoggerFactory.getLogger(UserDiscoveryService.class);
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 50;
    private static final int MAX_SIMILAR_CANDIDATES = 200;
    private static final int SUGGESTION_CACHE_SIZE = 30;
    private static final String SUGGESTION_CACHE_PREFIX = "user:suggestions:v1:";
    private static final Duration SUGGESTION_CACHE_TTL = Duration.ofHours(36);
    private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

    private final UserSearchService userSearchService;
    private final UserDiscoveryHydrator hydrator;
    private final UserVectorQueryService userVectorQueryService;
    private final SemanticVectorSearchService semanticVectorSearchService;
    private final UserDetailsRepository userDetailsRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public Mono<PageResponse<UserDiscoveryResponse>> search(String viewerId,
                                                            String query,
                                                            String filter,
                                                            int page,
                                                            int size) {
        return userSearchService.searchUsers(query, filter, page, size)
                .flatMap(result -> hydratePage(viewerId, result));
    }

    public Mono<PageResponse<UserDiscoveryResponse>> findSimilar(String viewerId,
                                                                 String targetUserId,
                                                                 int page,
                                                                 int size) {
        String target = requireText(targetUserId, "targetUserId");
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeSize(size);
        long requestedCount = ((long) pageNumber + 1L) * pageSize;
        int requested = (int) Math.min(requestedCount, MAX_SIMILAR_CANDIDATES);
        Set<String> excludedIds = new LinkedHashSet<>();
        excludedIds.add(target);
        if (hasText(viewerId)) excludedIds.add(viewerId.trim());

        return userVectorQueryService.getLongTermOrUserVector(target)
                .flatMap(vector -> vector.isEmpty()
                        ? Mono.just(PageResponse.of(List.<UserDiscoveryResponse>of(), pageNumber, 0, pageSize))
                        : semanticVectorSearchService.searchUserIdsByVector(vector, requested, excludedIds)
                        .flatMap(ids -> hydrateSimilarPage(viewerId, ids, pageNumber, pageSize)));
    }

    public Mono<PageResponse<UserDiscoveryResponse>> findSuggested(String viewerId, int page, int size) {
        String viewer = requireText(viewerId, "viewerId");
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeSize(size);
        return readSuggestedIds(viewer)
                .flatMap(ids -> ids.isEmpty() ? refreshSuggestedIds(viewer) : Mono.just(ids))
                .flatMap(ids -> hydrateSimilarPage(viewer, ids, pageNumber, pageSize));
    }

    public Mono<PageResponse<UserDiscoveryResponse>> refreshSuggested(String viewerId, int page, int size) {
        String viewer = requireText(viewerId, "viewerId");
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeSize(size);
        return refreshSuggestedIds(viewer)
                .flatMap(ids -> hydrateSimilarPage(viewer, ids, pageNumber, pageSize));
    }

    @Scheduled(cron = "0 30 5 * * *", zone = "Asia/Ho_Chi_Minh")
    public void refreshDailySuggestions() {
        userDetailsRepository.findAllUserIds()
                .concatMap(this::refreshSuggestedIds)
                .then()
                .doOnSuccess(unused -> log.info("|UserDiscoveryService|refreshDailySuggestions|completed"))
                .doOnError(error -> log.error("|UserDiscoveryService|refreshDailySuggestions|failed|error={}", error.getMessage()))
                .subscribe();
    }

    private Mono<List<String>> refreshSuggestedIds(String viewerId) {
        return Mono.zip(
                        userDetailsRepository.findById(viewerId).defaultIfEmpty(new UserDetails()),
                        findSimilar(viewerId, viewerId, 0, MAX_PAGE_SIZE)
                )
                .flatMap(tuple -> {
                    UserDetails viewer = tuple.getT1();
                    List<UserDiscoveryResponse> candidates = tuple.getT2().content().stream()
                            .filter(candidate -> !candidate.viewerFollowsUser() && !candidate.friend())
                            .toList();
                    return Flux.fromIterable(candidates)
                            .index()
                            .concatMap(indexed -> userDetailsRepository.findById(indexed.getT2().userId())
                                    .defaultIfEmpty(new UserDetails())
                                    .map(details -> new ScoredCandidate(
                                            indexed.getT2().userId(),
                                            profileScore(viewer, details) + Math.max(0, MAX_PAGE_SIZE - indexed.getT1())
                                    )))
                            .sort(Comparator.comparingLong(ScoredCandidate::score).reversed())
                            .map(ScoredCandidate::userId)
                            .take(SUGGESTION_CACHE_SIZE)
                            .collectList();
                })
                .flatMap(ids -> redisTemplate.opsForValue()
                        .set(SUGGESTION_CACHE_PREFIX + viewerId, GsonUtils.getGson().toJson(ids), SUGGESTION_CACHE_TTL)
                        .onErrorReturn(false)
                        .thenReturn(ids));
    }

    private Mono<List<String>> readSuggestedIds(String viewerId) {
        return redisTemplate.opsForValue().get(SUGGESTION_CACHE_PREFIX + viewerId)
                .map(json -> {
                    List<String> ids = GsonUtils.getGson().fromJson(json, STRING_LIST_TYPE);
                    return ids == null ? List.<String>of() : ids;
                })
                .onErrorReturn(List.of())
                .defaultIfEmpty(List.of());
    }

    private long profileScore(UserDetails viewer, UserDetails candidate) {
        long score = 0;
        if (sameText(viewer.getLivingIn(), candidate.getLivingIn())) score += 20;
        if (sameText(viewer.getHometown(), candidate.getHometown())) score += 12;
        Set<String> viewerHobbies = viewer.getHobbyList().stream()
                .filter(this::hasText)
                .map(value -> value.trim().toLowerCase())
                .collect(java.util.stream.Collectors.toSet());
        score += candidate.getHobbyList().stream()
                .filter(this::hasText)
                .map(value -> value.trim().toLowerCase())
                .filter(viewerHobbies::contains)
                .distinct()
                .count() * 6;
        return score;
    }

    private boolean sameText(String left, String right) {
        return hasText(left) && hasText(right) && left.trim().equalsIgnoreCase(right.trim());
    }

    private Mono<PageResponse<UserDiscoveryResponse>> hydratePage(
            String viewerId,
            PageResponse<String> page
    ) {
        return Flux.fromIterable(page.content())
                .concatMap(userId -> hydrator.hydrate(viewerId, userId))
                .collectList()
                .map(content -> new PageResponse<>(
                        content,
                        page.pageNumber(),
                        page.totalElements(),
                        page.totalPages()
                ));
    }

    private Mono<PageResponse<UserDiscoveryResponse>> hydrateSimilarPage(
            String viewerId,
            List<String> ids,
            int page,
            int size
    ) {
        long fromOffset = (long) page * size;
        int from = fromOffset >= ids.size() ? ids.size() : (int) fromOffset;
        int to = Math.min(from + size, ids.size());
        return Flux.fromIterable(ids.subList(from, to))
                .concatMap(userId -> hydrator.hydrate(viewerId, userId))
                .collectList()
                .map(content -> PageResponse.of(content, page, ids.size(), size));
    }

    private int normalizeSize(int size) {
        return size <= 0 ? DEFAULT_PAGE_SIZE : Math.min(size, MAX_PAGE_SIZE);
    }

    private String requireText(String value, String field) {
        if (!hasText(value)) throw new IllegalArgumentException(field + " is required");
        return value.trim();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record ScoredCandidate(String userId, long score) { }
}
