package com.dauducbach.clone.modules.post.stories.library;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.repository.story.projection.StoryViewerRow;
import reactor.core.publisher.Mono;

/** Read-only search across story viewers and public user names. */
public interface StoryViewerSearch {
    Mono<PageResponse<StoryViewerRow>> search(String storyId, String query, int page, int size, int offset);
}