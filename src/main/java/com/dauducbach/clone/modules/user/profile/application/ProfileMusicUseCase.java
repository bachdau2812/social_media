package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
public class ProfileMusicUseCase {
    private static final Logger log = LoggerFactory.getLogger(ProfileMusicUseCase.class);

    private final UserIdentityQuery userIdentityQuery;
    private final MusicCatalog musicCatalog;
    private final ProfileMusicStore profileMusicStore;
    private final MediaAssets mediaAssets;
    private final AuditRecorder auditRecorder;

    public ProfileMusicUseCase(
            UserIdentityQuery userIdentityQuery,
            MusicCatalog musicCatalog,
            ProfileMusicStore profileMusicStore,
            MediaAssets mediaAssets,
            AuditRecorder auditRecorder) {
        this.userIdentityQuery = userIdentityQuery;
        this.musicCatalog = musicCatalog;
        this.profileMusicStore = profileMusicStore;
        this.mediaAssets = mediaAssets;
        this.auditRecorder = auditRecorder;
    }

    public Mono<UserMusics> selectProfileMusic(MusicSelectRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String displayName = normalizeRequired(request.musicDisplayName(), "musicDisplayName");
        String slugName = normalizeRequired(request.musicSlugName(), "musicSlugName");

        return userIdentityQuery.exists(userId)
                .flatMap(exists -> Boolean.TRUE.equals(exists)
                        ? Mono.empty()
                        : Mono.error(new AppException(
                                ErrorCode.USER_DETAILS_NOT_FOUND,
                                String.format("User details not found for userId=%s", userId))))
                .then(musicCatalog.findBySlugNameAndDisplayName(slugName, displayName)
                        .switchIfEmpty(Mono.defer(() -> musicCatalog.findBySlugName(slugName)))
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.MUSIC_NOT_FOUND,
                                String.format("Music not found for slugName=%s displayName=%s", slugName, displayName)))))
                .flatMap(music -> saveUserMusic(userId, music)
                        .flatMap(userMusic -> mediaAssets.registerFeatureMusic(
                                        userId,
                                        music.id(),
                                        music.displayName(),
                                        music.slugName(),
                                        music.songUrl(),
                                        music.displayImages())
                                .then(saveAudit(userId, userMusic, music))
                                .thenReturn(userMusic)))
                .doOnSuccess(userMusic -> log.info("|ProfileMusicUseCase|select|saved|userId={}|musicId={}|userMusicId={}",
                        userId, userMusic.getMusicId(), userMusic.getId()))
                .doOnError(error -> log.error("|ProfileMusicUseCase|select|failed|userId={}|error={}",
                        userId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_SAVE_FAILED, "Select profile music failed", error));
    }

    private Mono<UserMusics> saveUserMusic(String userId, MusicTrackView music) {
        UserMusics userMusic = UserMusics.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .musicId(music.id())
                .createdAt(Instant.now())
                .build();
        return profileMusicStore.insert(userMusic);
    }

    private Mono<Void> saveAudit(String userId, UserMusics userMusic, MusicTrackView music) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("musicId", music.id());
        metadata.addProperty("slugName", music.slugName());
        metadata.addProperty("displayName", music.displayName());
        return auditRecorder.record(new AuditEntry(
                userId,
                AuditActionType.SELECT_PROFILE_MUSIC,
                "FEATURE_MUSIC",
                userMusic.getId(),
                "SUCCESS",
                metadata.toString(),
                null));
    }

    private String normalizeRequired(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, fieldName + " is required");
        }
        return value.trim();
    }
}
