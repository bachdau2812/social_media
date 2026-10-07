package com.dauducbach.clone.modules.post.application;

import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import reactor.core.publisher.Mono;

import java.util.List;

public interface PostPublicationMessaging {
    Mono<Void> requestMediaScan(String postId, String userId, List<PostMediaScanItem> items);

    Mono<Void> publishApproved(PostDetails post, String successMessage);

    Mono<Void> publishUpdated(PostDetails post);
}
