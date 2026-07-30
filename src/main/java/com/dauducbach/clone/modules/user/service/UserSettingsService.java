package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.UserSettings;
import com.dauducbach.clone.modules.user.repositoty.UserSettingsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserSettingsService {
    UserSettingsRepository repository;
    R2dbcEntityTemplate entityTemplate;

    public Mono<UserSettings> getSettings(String userId) {
        return repository.findById(userId)
                .switchIfEmpty(Mono.defer(() -> entityTemplate.insert(UserSettings.class).using(UserSettings.defaults(userId))));
    }

    public Mono<UserSettings> updateSettings(String userId, UserSettings patch) {
        return getSettings(userId)
                .flatMap(existing -> {
                    existing.setAccountVisibility(value(patch.getAccountVisibility(), existing.getAccountVisibility()));
                    existing.setStoryVisibility(value(patch.getStoryVisibility(), existing.getStoryVisibility()));
                    existing.setCommentPermission(value(patch.getCommentPermission(), existing.getCommentPermission()));
                    existing.setMentionPermission(value(patch.getMentionPermission(), existing.getMentionPermission()));
                    existing.setTagApprovalRequired(patch.isTagApprovalRequired());
                    existing.setActivityStatusVisible(patch.isActivityStatusVisible());
                    existing.setReadReceiptsEnabled(patch.isReadReceiptsEnabled());
                    existing.setPushEnabled(patch.isPushEnabled());
                    existing.setEmailEnabled(patch.isEmailEnabled());
                    existing.setLikesEnabled(patch.isLikesEnabled());
                    existing.setCommentsEnabled(patch.isCommentsEnabled());
                    existing.setFollowsEnabled(patch.isFollowsEnabled());
                    existing.setMentionsEnabled(patch.isMentionsEnabled());
                    existing.setStoriesEnabled(patch.isStoriesEnabled());
                    existing.setMessagesEnabled(patch.isMessagesEnabled());
                    existing.setSecurityEnabled(patch.isSecurityEnabled());
                    existing.setSensitiveContentLevel(value(patch.getSensitiveContentLevel(), existing.getSensitiveContentLevel()));
                    existing.setAutoplayVideo(value(patch.getAutoplayVideo(), existing.getAutoplayVideo()));
                    existing.setTheme(value(patch.getTheme(), existing.getTheme()));
                    existing.setReducedMotion(patch.isReducedMotion());
                    existing.setTextScale(patch.getTextScale() <= 0 ? existing.getTextScale() : patch.getTextScale());
                    existing.setHighContrast(patch.isHighContrast());
                    existing.setAlwaysShowCaptions(patch.isAlwaysShowCaptions());
                    existing.setUpdatedAt(Instant.now());
                    return repository.save(existing);
                });
    }

    private String value(String candidate, String fallback) {
        return candidate == null || candidate.isBlank() ? fallback : candidate.trim().toUpperCase();
    }
}
