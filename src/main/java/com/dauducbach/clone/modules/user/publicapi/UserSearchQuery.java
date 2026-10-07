package com.dauducbach.clone.modules.user.publicapi;

import com.dauducbach.clone.commons.response.PageResponse;
import reactor.core.publisher.Mono;

public interface UserSearchQuery {
    Mono<PageResponse<String>> searchUsers(String query, String filter, int page, int limit);
}
