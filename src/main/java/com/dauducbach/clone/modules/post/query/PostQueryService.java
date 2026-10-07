package com.dauducbach.clone.modules.post.query;

import com.dauducbach.clone.modules.post.publicapi.PostQuery;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PostQueryService implements PostQuery {
    private final PostContentQueryService postContentQueryService;
    private final PostDetailsRepository postDetailsRepository;
    private final PostItemRepository postItemRepository;

    @Override
    public Mono<PostSnapshot> findSnapshot(String postId) {
        return postContentQueryService.findById(postId)
                .map(post -> new PostSnapshot(post.getPostId(), post.getUserId(), post.getContent()));
    }

    @Override
    public Flux<FeedPostSnapshot> findApprovedFeedSnapshots(Collection<String> postIds) {
        List<String> ids = postIds == null ? List.of() : postIds.stream()
                .filter(id -> id != null && !id.isBlank()).map(String::trim).distinct().toList();
        if (ids.isEmpty()) return Flux.empty();
        return postDetailsRepository.findApprovedFeedEligibleByIdIn(ids).collectList()
                .flatMap(posts -> {
                    if (posts.isEmpty()) return Mono.just(List.<FeedPostSnapshot>of());
                    List<String> availableIds = posts.stream().map(PostDetails::getPostId).toList();
                    return postItemRepository.findByPostIdInOrderByPostIdAscOrderNumberAsc(availableIds)
                            .collectList()
                            .map(items -> snapshots(posts, items));
                })
                .flatMapMany(Flux::fromIterable);
    }

    private List<FeedPostSnapshot> snapshots(List<PostDetails> posts, List<PostItem> items) {
        Map<String, List<FeedItemSnapshot>> itemsByPost = items.stream().collect(Collectors.groupingBy(
                PostItem::getPostId,
                Collectors.mapping(item -> new FeedItemSnapshot(item.getId(), item.getOrderNumber(), item.getCaption(),
                        item.getMediaId(), item.getMusicId(), item.getMusicStart(), item.getMusicEnd()),
                        Collectors.toList())));
        return posts.stream().map(post -> new FeedPostSnapshot(
                post.getPostId(), post.getUserId(), post.getContent(), post.getHashtag(), post.getHashtagList(),
                post.getMediaRatio(), post.getValidateStatus(), post.getMusicId(), post.getMusicStart(),
                post.getMusicEnd(), post.getCreatedAt(), post.getUpdatedAt(),
                itemsByPost.getOrDefault(post.getPostId(), List.of()))).toList();
    }
}
