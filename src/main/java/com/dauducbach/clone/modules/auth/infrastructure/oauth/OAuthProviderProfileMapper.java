package com.dauducbach.clone.modules.auth.infrastructure.oauth;

import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class OAuthProviderProfileMapper {
    public ProviderProfile map(OAuth2User user, String provider) {
        String email = user.getAttribute("email");
        String displayName = firstNonBlank(
                user.getAttribute("name"),
                user.getAttribute("login"),
                email,
                "Social user");
        return new ProviderProfile(
                provider,
                email,
                displayName,
                providerId(user, provider),
                avatarUrl(user, provider));
    }

    private String providerId(OAuth2User user, String provider) {
        return switch (provider) {
            case "google" -> user.getAttribute("sub");
            case "facebook" -> user.getAttribute("id");
            case "github" -> {
                Object id = user.getAttribute("id");
                yield id == null ? null : id.toString();
            }
            default -> null;
        };
    }

    private String avatarUrl(OAuth2User user, String provider) {
        return switch (provider) {
            case "google" -> user.getAttribute("picture");
            case "facebook" -> facebookAvatarUrl(user);
            case "github" -> user.getAttribute("avatar_url");
            default -> null;
        };
    }

    @SuppressWarnings("unchecked")
    private String facebookAvatarUrl(OAuth2User user) {
        Map<String, Object> picture = user.getAttribute("picture");
        if (picture == null) return null;
        Map<String, Object> data = (Map<String, Object>) picture.get("data");
        return data == null ? null : (String) data.get("url");
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value.trim();
        }
        return "";
    }

    public record ProviderProfile(String provider, String email, String displayName, String providerId, String avatarUrl) {
    }
}
