package com.dauducbach.clone.modules.auth.infrastructure.oauth;

import java.net.URI;

public final class OAuthRedirectUrlBuilder {
    private OAuthRedirectUrlBuilder() {
    }

    public static URI build(String baseUrl) {
        return URI.create(baseUrl);
    }
}
