package com.dauducbach.clone.modules.auth.service;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OAuthRedirectUrlBuilderTest {

    @Test
    void buildsCallbackUrlWithoutIdentityOrTokens() {
        URI redirect = OAuthRedirectUrlBuilder.build(
                "http://localhost:5173/oauth/callback"
        );

        String url = redirect.toASCIIString();
        assertEquals("http://localhost:5173/oauth/callback", url);
        assertFalse(url.contains("?"));
        assertFalse(url.toLowerCase().contains("token"));
    }
}
