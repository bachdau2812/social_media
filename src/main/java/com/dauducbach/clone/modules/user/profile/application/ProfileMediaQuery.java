package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProfileMediaQuery {
    private final MediaCatalog mediaCatalog;
    private final MediaAssets mediaAssets;

    public ProfileMediaQuery(MediaCatalog mediaCatalog, MediaAssets mediaAssets) {
        this.mediaCatalog = mediaCatalog;
        this.mediaAssets = mediaAssets;
    }

    public Mono<PageResponse<MediaAssetView>> getUploadedMedia(
            String userId,
            OwnerType requestedOwnerType,
            int page,
            int size) {
        OwnerType ownerType = normalizeProfileOwnerType(requestedOwnerType);
        return mediaCatalog.findProfileMedia(userId, ownerType, page, size);
    }

    public Mono<MediaAssetView> getCurrentAvatar(String userId, MediaDisplayType mediaType) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.AVATAR : mediaType;
        return mediaCatalog.findCurrentAvatar(userId)
                .map(media -> media.withDeliveryUrls(
                        mediaAssets.transformDeliveryUrl(media.url(), displayType),
                        mediaAssets.transformDeliveryUrl(media.secureUrl(), displayType)));
    }

    public Mono<PageResponse<MediaAssetView>> getProfileMusicHistory(String userId, int page, int size) {
        return mediaCatalog.findProfileMedia(userId, OwnerType.FEATURE_MUSIC, page, size);
    }

    private OwnerType normalizeProfileOwnerType(OwnerType ownerType) {
        if (ownerType == null) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, "mode is required");
        }
        if (ownerType == OwnerType.AVATAR
                || ownerType == OwnerType.POST
                || ownerType == OwnerType.STORY
                || ownerType == OwnerType.FEATURE_MUSIC) {
            return ownerType;
        }
        throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, "mode must be AVATAR, POST, STORY or FEATURE_MUSIC");
    }
}
