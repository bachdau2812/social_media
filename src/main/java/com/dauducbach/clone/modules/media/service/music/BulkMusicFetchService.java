package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.MusicFetchType;
import com.dauducbach.clone.modules.media.dto.music.request.BulkMusicFetchRequest;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchItemResponse;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchResponse;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class BulkMusicFetchService {
    private static final Logger log = LoggerFactory.getLogger(BulkMusicFetchService.class);
    private static final Pattern SPOTIFY_TRACK_ID = Pattern.compile("^[A-Za-z0-9]{22}$");
    private static final String SAFE_FAILURE_MESSAGE = "Music fetch failed";

    private final MusicsRepository musicsRepository;
    private final SpotifyMusicFetchService spotifyMusicFetchService;

    public Mono<BulkMusicFetchResponse> triggerFetch(BulkMusicFetchRequest request) {
        return Mono.defer(() -> {
            NormalizedRequest normalized = normalize(request);
            log.info(
                    "|BulkMusicFetchService|triggerFetch|start|type={}|fetchListSize={}|limit={}",
                    normalized.type(),
                    normalized.fetchList().size(),
                    normalized.limit());
            return selectTrackIds(normalized)
                    .distinct()
                    .collectList()
                    .flatMapMany(Flux::fromIterable)
                    .concatMap(this::enqueue)
                    .collectList()
                    .map(items -> BulkMusicFetchResponse.from(normalized.type(), items))
                    .doOnSuccess(response -> log.info(
                            "|BulkMusicFetchService|triggerFetch|completed|type={}|selected={}|started={}|processing={}|alreadyFetched={}|failed={}",
                            response.type(),
                            response.selectedCount(),
                            response.startedCount(),
                            response.processingCount(),
                            response.alreadyFetchedCount(),
                            response.failedCount()));
        });
    }

    private Flux<String> selectTrackIds(NormalizedRequest request) {
        PageRequest pageable = PageRequest.of(0, request.limit());
        return switch (request.type()) {
            case TOP -> musicsRepository.findTopUnfetched(pageable)
                    .filter(music -> music.getId() != null && !music.getId().isBlank())
                    .map(music -> music.getId().trim());
            case ARTIST -> Flux.fromIterable(request.fetchList())
                    .concatMap(artist -> musicsRepository.findUnfetchedByArtist(artist, pageable))
                    .filter(music -> music.getId() != null && !music.getId().isBlank())
                    .map(music -> music.getId().trim());
            case SONG -> Flux.fromIterable(request.fetchList());
        };
    }

    private Mono<BulkMusicFetchItemResponse> enqueue(String trackId) {
        return Mono.defer(() -> spotifyMusicFetchService.requestFetchSilently(trackId))
                .map(result -> new BulkMusicFetchItemResponse(
                        result.trackId(),
                        mapStatus(result.status()),
                        null))
                .onErrorResume(error -> {
                    log.error(
                            "|BulkMusicFetchService|enqueue|failed|trackId={}|errorType={}|error={}",
                            trackId,
                            error.getClass().getSimpleName(),
                            error.getMessage());
                    return Mono.just(new BulkMusicFetchItemResponse(
                            trackId,
                            BulkMusicFetchItemResponse.Status.FAILED,
                            SAFE_FAILURE_MESSAGE));
                });
    }

    private BulkMusicFetchItemResponse.Status mapStatus(MusicFetchAcceptedResponse.Status status) {
        return switch (status) {
            case STARTED -> BulkMusicFetchItemResponse.Status.STARTED;
            case PROCESSING -> BulkMusicFetchItemResponse.Status.PROCESSING;
            case ALREADY_FETCHED -> BulkMusicFetchItemResponse.Status.ALREADY_FETCHED;
        };
    }

    private NormalizedRequest normalize(BulkMusicFetchRequest request) {
        if (request == null || request.type() == null) {
            throw invalid("type is required");
        }

        List<String> fetchList = normalizeList(request.fetchList(), request.type() == MusicFetchType.ARTIST);
        if ((request.type() == MusicFetchType.ARTIST || request.type() == MusicFetchType.SONG)
                && fetchList.isEmpty()) {
            throw invalid("fetchList is required for " + request.type());
        }
        if (request.type() == MusicFetchType.SONG
                && fetchList.stream().anyMatch(trackId -> !SPOTIFY_TRACK_ID.matcher(trackId).matches())) {
            throw invalid("SONG fetchList must contain Spotify track IDs with 22 base-62 characters");
        }

        int limit = request.type() == MusicFetchType.SONG
                ? BulkMusicFetchRequest.DEFAULT_LIMIT
                : request.limit() == null ? BulkMusicFetchRequest.DEFAULT_LIMIT : request.limit();
        if (request.type() != MusicFetchType.SONG
                && (limit < 1 || limit > BulkMusicFetchRequest.MAX_LIMIT)) {
            throw invalid("limit must be between 1 and " + BulkMusicFetchRequest.MAX_LIMIT);
        }
        return new NormalizedRequest(request.type(), fetchList, limit);
    }

    private List<String> normalizeList(List<String> values, boolean caseInsensitiveDuplicates) {
        Map<String, String> normalized = new LinkedHashMap<>();
        for (String value : values == null ? List.<String>of() : values) {
            if (value == null || value.isBlank()) continue;
            String clean = value.trim();
            String key = caseInsensitiveDuplicates ? clean.toLowerCase(Locale.ROOT) : clean;
            normalized.putIfAbsent(key, clean);
        }
        return new ArrayList<>(normalized.values());
    }

    private AppException invalid(String message) {
        return new AppException(ErrorCode.MUSIC_REQUEST_INVALID, message);
    }

    private record NormalizedRequest(
            MusicFetchType type,
            List<String> fetchList,
            int limit
    ) { }
}
