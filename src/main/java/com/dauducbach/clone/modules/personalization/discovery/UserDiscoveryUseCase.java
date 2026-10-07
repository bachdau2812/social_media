package com.dauducbach.clone.modules.personalization.discovery;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceQuery;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryProfile;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryQuery;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.publicapi.UserSearchQuery;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class UserDiscoveryUseCase {
    private static final Logger log = LoggerFactory.getLogger(UserDiscoveryUseCase.class);
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 50;
    private static final int MAX_SIMILAR_CANDIDATES = 200;
    private static final int SUGGESTION_CACHE_SIZE = 30;

    private final UserSearchQuery userSearch;
    private final UserDiscoveryQuery userProfiles;
    private final PreferenceQuery preferences;
    private final SimilarUserSearch similarUsers;
    private final SuggestionIdCache suggestionCache;

    public Mono<PageResponse<UserDiscoveryResponse>> search(String viewerId, String query, String filter, int page, int size) {
        return userSearch.searchUsers(query, filter, page, size)
                .flatMap(result -> hydratePage(viewerId, result));
    }

    public Mono<PageResponse<UserDiscoveryResponse>> findSimilar(String viewerId, String targetUserId, int page, int size) {
        String target = requireText(targetUserId, "targetUserId");
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeSize(size);
        long requestedCount = ((long) pageNumber + 1L) * pageSize;
        int requested = (int) Math.min(requestedCount, MAX_SIMILAR_CANDIDATES);
        Set<String> excludedIds = new LinkedHashSet<>();
        excludedIds.add(target);
        if (hasText(viewerId)) excludedIds.add(viewerId.trim());

        return preferences.load(target)
                .map(snapshot -> snapshot.longTerm().isEmpty() ? snapshot.profile() : snapshot.longTerm())
                .defaultIfEmpty(List.of())
                .doOnNext(vector -> {
                    if (vector.isEmpty()) log.debug("|UserDiscoveryUseCase|missing-vector|userId={}|field=long-term/profile", target);
                })
                .flatMap(vector -> vector.isEmpty()
                        ? Mono.just(PageResponse.of(List.<UserDiscoveryResponse>of(), pageNumber, 0, pageSize))
                        : similarUsers.findSimilarUserIds(vector, requested, excludedIds)
                            .flatMap(ids -> hydrateSimilarPage(viewerId, ids, pageNumber, pageSize)));
    }

    public Mono<PageResponse<UserDiscoveryResponse>> findSuggested(String viewerId, int page, int size) {
        String viewer = requireText(viewerId, "viewerId");
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeSize(size);
        return suggestionCache.get(viewer)
                .flatMap(ids -> ids.isEmpty() ? refreshSuggestedIds(viewer) : Mono.just(ids))
                .flatMap(ids -> hydrateSimilarPage(viewer, ids, pageNumber, pageSize));
    }

    public Mono<PageResponse<UserDiscoveryResponse>> refreshSuggested(String viewerId, int page, int size) {
        String viewer = requireText(viewerId, "viewerId");
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeSize(size);
        return refreshSuggestedIds(viewer).flatMap(ids -> hydrateSimilarPage(viewer, ids, pageNumber, pageSize));
    }

    @Scheduled(cron = "0 30 5 * * *", zone = "Asia/Ho_Chi_Minh")
    public Mono<Void> refreshDailySuggestions() {
        return Mono.defer(() -> userProfiles.findAllUserIds()
                .concatMap(this::refreshSuggestedIds)
                .then()
                .doOnSuccess(unused -> log.info("|UserDiscoveryUseCase|refreshDailySuggestions|completed"))
                .doOnError(error -> log.error("|UserDiscoveryUseCase|refreshDailySuggestions|failed|error={}", error.getMessage())));
    }

    private Mono<List<String>> refreshSuggestedIds(String viewerId) {
        return Mono.zip(
                        userProfiles.findProfile(viewerId).defaultIfEmpty(emptyProfile(viewerId)),
                        findSimilar(viewerId, viewerId, 0, MAX_PAGE_SIZE))
                .flatMap(tuple -> {
                    UserDiscoveryProfile viewer = tuple.getT1();
                    List<UserDiscoveryResponse> candidates = tuple.getT2().content().stream()
                            .filter(candidate -> !candidate.viewerFollowsUser() && !candidate.friend())
                            .toList();
                    return Flux.fromIterable(candidates)
                            .index()
                            .concatMap(indexed -> userProfiles.findProfile(indexed.getT2().userId())
                                    .defaultIfEmpty(emptyProfile(indexed.getT2().userId()))
                                    .map(profile -> new ScoredCandidate(profile.userId(),
                                            profileScore(viewer, profile) + Math.max(0, MAX_PAGE_SIZE - indexed.getT1()))))
                            .sort(Comparator.comparingLong(ScoredCandidate::score).reversed())
                            .map(ScoredCandidate::userId)
                            .take(SUGGESTION_CACHE_SIZE)
                            .collectList();
                })
                .flatMap(ids -> suggestionCache.put(viewerId, ids).thenReturn(ids));
    }

    private long profileScore(UserDiscoveryProfile viewer, UserDiscoveryProfile candidate) {
        long score = 0;
        if (sameText(viewer.livingIn(), candidate.livingIn())) score += 20;
        if (sameText(viewer.hometown(), candidate.hometown())) score += 12;
        Set<String> viewerHobbies = viewer.hobbies().stream()
                .filter(this::hasText)
                .map(value -> value.trim().toLowerCase())
                .collect(java.util.stream.Collectors.toSet());
        score += candidate.hobbies().stream()
                .filter(this::hasText)
                .map(value -> value.trim().toLowerCase())
                .filter(viewerHobbies::contains)
                .distinct()
                .count() * 6;
        return score;
    }

    private Mono<PageResponse<UserDiscoveryResponse>> hydratePage(String viewerId, PageResponse<String> page) {
        return Flux.fromIterable(page.content())
                .concatMap(userId -> userProfiles.hydrate(viewerId, userId))
                .collectList()
                .map(content -> new PageResponse<>(content, page.pageNumber(), page.totalElements(), page.totalPages()));
    }

    private Mono<PageResponse<UserDiscoveryResponse>> hydrateSimilarPage(String viewerId, List<String> ids, int page, int size) {
        long fromOffset = (long) page * size;
        int from = fromOffset >= ids.size() ? ids.size() : (int) fromOffset;
        int to = Math.min(from + size, ids.size());
        return Flux.fromIterable(ids.subList(from, to))
                .concatMap(userId -> userProfiles.hydrate(viewerId, userId))
                .collectList()
                .map(content -> PageResponse.of(content, page, ids.size(), size));
    }

    private UserDiscoveryProfile emptyProfile(String userId) {
        return new UserDiscoveryProfile(userId, "", "", List.of());
    }

    private int normalizeSize(int size) {
        return size <= 0 ? DEFAULT_PAGE_SIZE : Math.min(size, MAX_PAGE_SIZE);
    }

    private String requireText(String value, String field) {
        if (!hasText(value)) throw new IllegalArgumentException(field + " is required");
        return value.trim();
    }

    private boolean sameText(String left, String right) {
        return hasText(left) && hasText(right) && left.trim().equalsIgnoreCase(right.trim());
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record ScoredCandidate(String userId, long score) { }
}
