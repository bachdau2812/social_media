package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.user.dto.request.JamendoMusicImportRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicCreateRequest;
import com.dauducbach.clone.modules.user.dto.response.JamendoMusicImportResponse;
import com.dauducbach.clone.modules.user.entity.Musics;
import com.dauducbach.clone.modules.user.repositoty.MusicsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.net.URLDecoder;
import java.text.Normalizer;
import java.time.LocalDate;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class MusicService {
    private static final Logger log = LoggerFactory.getLogger(MusicService.class);
    private static final String JAMENDO_HOST = "api.jamendo.com";
    private static final String MUSIC_LIST_CACHE_KEY = "user:musics:list";
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 100;

    MusicsRepository musicsRepository;
    MediaService mediaService;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> redisTemplate;
    WebClient webClient;
    MediaCompatibilityFacade mediaFacade;

    public Mono<JamendoMusicImportResponse> importFromJamendo(JamendoMusicImportRequest request, String categoryParam) {
        return Mono.defer(() -> {
                    URI uri = validateJamendoUri(request == null ? null : request.url());
                    String category = resolveCategory(request.category(), categoryParam, uri);
                    log.info("|MusicService|importFromJamendo|start|host={}|category={}", uri.getHost(), category);

                    return webClient.get()
                            .uri(uri)
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(this::extractResults)
                            .doOnSuccess(results -> log.info("|MusicService|importFromJamendo|fetched|host={}|itemCount={}",
                                    uri.getHost(), results.size()))
                            .flatMap(results -> Flux.fromIterable(results)
                                    .concatMap(item -> importJamendoTrack(item, category))
                                    .collectList()
                                    .flatMap(resultsPerTrack -> evictMusicListCache()
                                            .thenReturn(toImportResponse(results.size(), resultsPerTrack))));
                })
                .doOnError(error -> {
                    if (error instanceof AppException) {
                        log.warn("|MusicService|importFromJamendo|business failed|error={}", error.getMessage());
                    } else {
                        log.error("|MusicService|importFromJamendo|failed|error={}", error.getMessage());
                    }
                })
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_IMPORT_FAILED, "Import Jamendo musics failed", error));
    }

    public Mono<Musics> createMusic(MusicCreateRequest request) {
        validateCreateRequest(request);

        log.info("|MusicService|createMusic|displayNameLength={}|category={}",
                request.displayName().trim().length(), normalizeOptional(request.category()));
        Musics music = Musics.builder()
                .id(UUID.randomUUID().toString())
                .displayName(request.displayName().trim())
                .slugName(toSlug(request.displayName()))
                .descriptions(normalizeOptional(request.descriptions()))
                .displayImages(normalizeOptional(request.displayImages()))
                .singleName(normalizeOptional(request.singleName()))
                .songUrl(request.songUrl().trim())
                .duration(request.duration())
                .category(normalizeOptional(request.category()))
                .releaseDate(request.releaseDate())
                .build();

        return r2dbcEntityTemplate.insert(Musics.class).using(music)
                .flatMap(saved -> evictMusicListCache().thenReturn(saved))
                .doOnSuccess(saved -> log.info("|MusicService|createMusic|saved|musicId={}|slugName={}", saved.getId(), saved.getSlugName()))
                .doOnError(error -> log.error("|MusicService|createMusic|failed|slugName={}|error={}",
                        music.getSlugName(), error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_SAVE_FAILED, "Create music failed", error));
    }

    public Mono<PageResponse<Musics>> getMusics(int page, int size, String keyword, String category) {
        int pageNumber = Math.max(page, 0);
        int pageSize = Math.clamp(size, 1, MAX_PAGE_SIZE);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        String normalizedKeyword = normalizeOptional(keyword);
        String normalizedCategory = normalizeOptional(category);

        Mono<Long> countMono;
        Flux<Musics> musicFlux;
        if (normalizedKeyword != null && normalizedCategory != null) {
            countMono = musicsRepository.countSearchByCategory(normalizedKeyword, normalizedCategory);
            musicFlux = musicsRepository.searchByCategory(normalizedKeyword, normalizedCategory, pageable);
        } else if (normalizedKeyword != null) {
            countMono = musicsRepository.countSearch(normalizedKeyword);
            musicFlux = musicsRepository.search(normalizedKeyword, pageable);
        } else if (normalizedCategory != null) {
            countMono = musicsRepository.countByCategory(normalizedCategory);
            musicFlux = musicsRepository.findByCategory(normalizedCategory, pageable);
        } else {
            countMono = musicsRepository.count();
            musicFlux = musicsRepository.findPage(pageable);
        }

        log.info("|MusicService|getMusics|page={}|size={}|hasKeyword={}|category={}",
                pageNumber, pageSize, normalizedKeyword != null, normalizedCategory);
        return countMono.flatMap(total -> musicFlux.collectList()
                        .doOnSuccess(items -> log.info("|MusicService|getMusics|dbResult|page={}|count={}|total={}",
                                pageNumber, items.size(), total))
                        .map(items -> PageResponse.of(items, pageNumber, total, pageSize)))
                .doOnError(error -> log.error("|MusicService|getMusics|failed|page={}|size={}|error={}",
                        pageNumber, pageSize, error.getMessage()))
                .onErrorMap(error -> new AppException(ErrorCode.MUSIC_FETCH_FAILED, "Fetch musics failed", error));
    }

    public Mono<Musics> getMusicById(String musicId) {
        if (musicId == null || musicId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "musicId is required"));
        }

        log.info("|MusicService|getMusicById|musicId={}", musicId);
        return musicsRepository.findById(musicId.trim())
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.MUSIC_NOT_FOUND,
                        String.format("Music not found for musicId=%s", musicId)
                )))
                .doOnSuccess(music -> log.info("|MusicService|getMusicById|success|musicId={}|slugName={}",
                        music.getId(), music.getSlugName()))
                .doOnError(error -> log.error("|MusicService|getMusicById|failed|musicId={}|error={}",
                        musicId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_FETCH_FAILED, "Fetch music detail failed", error));
    }

    private Mono<ImportTrackResult> importJamendoTrack(JsonObject item, String category) {
        String musicId = KafkaUtils.extractString(item, "id");
        if (musicId.isBlank()) {
            return Mono.just(ImportTrackResult.SKIPPED);
        }

        return musicsRepository.existsById(musicId)
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        log.info("|MusicService|importJamendoTrack|skipped existing|musicId={}", musicId);
                        return Mono.just(ImportTrackResult.SKIPPED);
                    }

                    String audioUrl = resolveAudioDownloadUrl(item);
                    if (audioUrl.isBlank()) {
                        log.warn("|MusicService|importJamendoTrack|skipped missing audio|musicId={}", musicId);
                        return Mono.just(ImportTrackResult.SKIPPED);
                    }

                    log.info("|MusicService|importJamendoTrack|processing|musicId={}|category={}", musicId, category);
                    return downloadAudio(audioUrl)
                            .flatMap(bytes -> uploadAudioToCloudinary(bytes, musicId, KafkaUtils.extractString(item, "name")))
                            .flatMap(uploadResult -> saveImportedMusic(item, category, uploadResult)
                                    .flatMap(saved -> mediaService.saveImportedMusicMedia(saved.getId(), saved.getDisplayName(), uploadResult)
                                            .thenReturn(ImportTrackResult.SAVED)))
                            .onErrorResume(error -> {
                                log.error("|MusicService|importJamendoTrack|failed|musicId={}|error={}", musicId, error.getMessage());
                                return Mono.just(ImportTrackResult.FAILED);
                            });
                })
                .onErrorResume(error -> {
                    log.error("|MusicService|importJamendoTrack|failed|musicId={}|error={}", musicId, error.getMessage());
                    return Mono.just(ImportTrackResult.FAILED);
                });
    }

    private Mono<Musics> saveImportedMusic(JsonObject item, String category, MediaAudioUploadResult uploadResult) {
        String musicId = KafkaUtils.extractString(item, "id");
        String displayName = firstNonBlank(KafkaUtils.extractString(item, "name"), musicId);
        String secureUrl = firstNonBlank(uploadResult.secureUrl(), uploadResult.url());
        if (secureUrl == null || secureUrl.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MUSIC_IMPORT_FAILED, "Cloudinary upload result missing secure_url"));
        }

        Musics music = Musics.builder()
                .id(musicId)
                .displayName(displayName)
                .slugName(toSlug(displayName))
                .descriptions(normalizeOptional(KafkaUtils.extractString(item, "lyrics")))
                .displayImages(resolveDisplayImage(item))
                .singleName(normalizeOptional(KafkaUtils.extractString(item, "artist_name")))
                .songUrl(secureUrl)
                .duration(KafkaUtils.extractLong(item, "duration"))
                .category(normalizeOptional(category))
                .releaseDate(parseReleaseDate(KafkaUtils.extractString(item, "releasedate")))
                .build();

        return r2dbcEntityTemplate.insert(Musics.class).using(music)
                .doOnSuccess(saved -> log.info("|MusicService|saveImportedMusic|saved|musicId={}", saved.getId()));
    }

    private Mono<byte[]> downloadAudio(String audioUrl) {
        log.info("|MusicService|downloadAudio|start|urlHost={}", URI.create(audioUrl).getHost());
        return webClient.get()
                .uri(audioUrl)
                .retrieve()
                .bodyToMono(byte[].class)
                .doOnSuccess(bytes -> log.info("|MusicService|downloadAudio|success|bytes={}", bytes.length))
                .doOnError(error -> log.error("|MusicService|downloadAudio|failed|error={}", error.getMessage()));
    }

    private Mono<MediaAudioUploadResult> uploadAudioToCloudinary(byte[] bytes, String musicId, String displayName) {
        log.info("|MusicService|uploadAudioToCloudinary|start|musicId={}|bytes={}", musicId, bytes.length);
        return mediaFacade.uploadMusic(
                        bytes,
                        toSlug(firstNonBlank(displayName, musicId)))
                .doOnSuccess(result -> log.info(
                        "|MusicService|uploadAudioToCloudinary|success|musicId={}|publicId={}",
                        musicId, result.publicId()))
                .doOnError(error -> log.error(
                        "|MusicService|uploadAudioToCloudinary|failed|musicId={}|error={}",
                        musicId, error.getMessage()));
    }

    private URI validateJamendoUri(String rawUrl) {
        if (rawUrl == null || rawUrl.isBlank()) {
            throw new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "Jamendo url is required");
        }

        URI uri;
        try {
            uri = URI.create(rawUrl.trim());
        } catch (IllegalArgumentException ex) {
            throw new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "Jamendo url is invalid", ex);
        }

        String scheme = uri.getScheme();
        String host = uri.getHost();
        if (scheme == null || host == null
                || uri.getUserInfo() != null
                || (!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme))
                || !JAMENDO_HOST.equalsIgnoreCase(host)) {
            throw new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "Only Jamendo API url is allowed");
        }
        return uri;
    }

    private List<JsonObject> extractResults(String rawResponse) {
        JsonObject json = GsonUtils.fromString(rawResponse);
        if (!json.has("results") || !json.get("results").isJsonArray()) {
            return List.of();
        }

        JsonArray results = json.getAsJsonArray("results");
        List<JsonObject> items = new ArrayList<>();
        for (JsonElement element : results) {
            if (element.isJsonObject()) {
                items.add(element.getAsJsonObject());
            }
        }
        return items;
    }

    private JamendoMusicImportResponse toImportResponse(int totalReceived, List<ImportTrackResult> results) {
        int saved = count(results, ImportTrackResult.SAVED);
        int skipped = count(results, ImportTrackResult.SKIPPED);
        int failed = count(results, ImportTrackResult.FAILED);
        log.info("|MusicService|importFromJamendo|completed|totalReceived={}|savedCount={}|skippedCount={}|failedCount={}",
                totalReceived, saved, skipped, failed);
        return new JamendoMusicImportResponse(totalReceived, saved, skipped, failed, "Import Jamendo musics completed");
    }

    private int count(List<ImportTrackResult> results, ImportTrackResult expected) {
        return (int) results.stream().filter(result -> result == expected).count();
    }

    private String resolveAudioDownloadUrl(JsonObject item) {
        boolean downloadAllowed = item.has("audiodownload_allowed")
                && !item.get("audiodownload_allowed").isJsonNull()
                && item.get("audiodownload_allowed").getAsBoolean();
        String audioDownload = KafkaUtils.extractString(item, "audiodownload");
        if (downloadAllowed && !audioDownload.isBlank()) {
            return audioDownload;
        }
        return KafkaUtils.extractString(item, "audio");
    }

    private String resolveDisplayImage(JsonObject item) {
        String image = KafkaUtils.extractString(item, "image");
        return image.isBlank() ? normalizeOptional(KafkaUtils.extractString(item, "album_image")) : image;
    }

    private String resolveCategory(String requestCategory, String categoryParam, URI uri) {
        String category = normalizeOptional(categoryParam);
        if (category != null) {
            return category;
        }
        category = normalizeOptional(requestCategory);
        if (category != null) {
            return category;
        }
        return firstQueryValue(uri, "category", "tags");
    }

    private String firstQueryValue(URI uri, String... names) {
        String query = uri.getRawQuery();
        if (query == null || query.isBlank()) {
            return null;
        }

        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            for (String name : names) {
                if (name.equalsIgnoreCase(parts[0])) {
                    return parts[1].isBlank() ? null : URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                }
            }
        }
        return null;
    }

    private LocalDate parseReleaseDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(value.trim());
        } catch (Exception ex) {
            return null;
        }
    }

    private void validateCreateRequest(MusicCreateRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "Request is required");
        }
        if (request.displayName() == null || request.displayName().isBlank()) {
            throw new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "displayName is required");
        }
        if (request.songUrl() == null || request.songUrl().isBlank()) {
            throw new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "songUrl is required");
        }
    }

    private Mono<Void> evictMusicListCache() {
        return redisTemplate.delete(MUSIC_LIST_CACHE_KEY)
                .onErrorResume(error -> {
                    log.error("|MusicService|evictMusicListCache|failed|error={}", error.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private String firstNonBlank(String first, String fallback) {
        return first == null || first.isBlank() ? fallback : first.trim();
    }


    private String toSlug(String value) {
        String source = firstNonBlank(value, UUID.randomUUID().toString());
        String normalized = Normalizer.normalize(source, Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("(^-|-$)", "");
        return normalized.isBlank() ? UUID.randomUUID().toString() : normalized;
    }

    private enum ImportTrackResult {
        SAVED,
        SKIPPED,
        FAILED
    }
}
