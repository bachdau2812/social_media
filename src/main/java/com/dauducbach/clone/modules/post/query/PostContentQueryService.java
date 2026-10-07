package com.dauducbach.clone.modules.post.query;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.application.PostDetailsCache;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class PostContentQueryService {
    private static final Logger log = LoggerFactory.getLogger(PostContentQueryService.class);

    private final PostDetailsRepository postDetailsRepository;
    private final PostDetailsCache postDetailsCache;

    public Mono<PostDetails> findById(String postId) {
        log.info("|PostContentQueryService|findById|postId={}", postId);
        return postDetailsCache.get(postId)
                .doOnNext(cached -> log.info("|PostContentQueryService|findById|cacheHit|postId={}", postId))
                .switchIfEmpty(Mono.defer(() -> postDetailsRepository.findById(postId)
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.POST_NOT_FOUND,
                                String.format("Post not found for postId=%s", postId)
                        )))
                        .onErrorMap(throwable -> throwable instanceof AppException
                                ? throwable
                                : new AppException(
                                        ErrorCode.POST_FETCH_FAILED,
                                        String.format("Fetch post failed for postId=%s", postId),
                                        throwable
                        ))
                        .flatMap(post -> {
                            log.info("|PostContentQueryService|findById|databaseHit|postId={}|userId={}", postId, post.getUserId());
                            return postDetailsCache.put(post)
                                    .onErrorResume(error -> {
                                        log.warn("|PostContentQueryService|findById|cacheWriteFailed|postId={}|error={}",
                                                postId, error.getMessage());
                                        return Mono.just(false);
                                    })
                                    .thenReturn(post);
                        })));
    }

    public Flux<PostDetails> findByAuthorId(String userId, int page, int size) {
        int limit = size <= 0 ? 10 : Math.min(size, 50);
        int offset = Math.max(page, 0) * limit;

        log.info("|PostContentQueryService|findByAuthorId|userId={}|page={}|size={}|limit={}|offset={}",
                userId, page, size, limit, offset);
        return postDetailsRepository.findByUserId(userId, limit, offset)
                .doOnComplete(() -> log.info("|PostContentQueryService|findByAuthorId|completed|userId={}|limit={}|offset={}",
                        userId, limit, offset))
                .doOnError(error -> log.error("|PostContentQueryService|findByAuthorId|failed|userId={}|error={}",
                        userId, error.getMessage()))
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.POST_LIST_FETCH_FAILED,
                        String.format("Fetch posts failed for userId=%s", userId),
                        throwable
                ));
    }
}
