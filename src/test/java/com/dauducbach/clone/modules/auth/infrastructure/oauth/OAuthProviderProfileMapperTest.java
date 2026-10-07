package com.dauducbach.clone.modules.auth.infrastructure.oauth;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.core.user.OAuth2User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OAuthProviderProfileMapperTest {
    @Test
    void mapsGoogleClaimsIntoTheProviderNeutralProfile() {
        OAuth2User user = mock(OAuth2User.class);
        when(user.getAttribute("email")).thenReturn("alice@example.com");
        when(user.getAttribute("name")).thenReturn("Alice");
        when(user.getAttribute("sub")).thenReturn("google-subject");
        when(user.getAttribute("picture")).thenReturn("https://example.com/avatar.png");

        var profile = new OAuthProviderProfileMapper().map(user, "google");

        assertThat(profile.email()).isEqualTo("alice@example.com");
        assertThat(profile.displayName()).isEqualTo("Alice");
        assertThat(profile.providerId()).isEqualTo("google-subject");
        assertThat(profile.avatarUrl()).isEqualTo("https://example.com/avatar.png");
    }

    @Test
    void mapsFacebookNestedAvatarWhenPresent() {
        OAuth2User user = mock(OAuth2User.class);
        when(user.getAttribute("id")).thenReturn("facebook-id");
        when(user.getAttribute("picture")).thenReturn(java.util.Map.of(
                "data", java.util.Map.of("url", "https://example.com/facebook.png")));

        var profile = new OAuthProviderProfileMapper().map(user, "facebook");

        assertThat(profile.providerId()).isEqualTo("facebook-id");
        assertThat(profile.avatarUrl()).isEqualTo("https://example.com/facebook.png");
    }
}
