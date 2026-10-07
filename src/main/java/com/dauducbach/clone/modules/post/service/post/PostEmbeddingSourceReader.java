package com.dauducbach.clone.modules.post.service.post;
import com.dauducbach.clone.modules.post.entity.*;
import com.dauducbach.clone.modules.post.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.List;
@Service
@RequiredArgsConstructor
public class PostEmbeddingSourceReader {
    private final PostDetailsRepository posts;
    private final PostItemRepository items;
    private final PostEmbeddingTextBuilder text;
    public Mono<Source> load(String postId) {
        return Mono.defer(() -> posts.findById(postId).flatMap(post -> items.findByPostIdOrderByOrderNumberAsc(postId)
                .collectList().map(currentItems -> new Source(post, currentItems, text.build(post, currentItems),
                        text.fingerprint(post, currentItems), text.revision(post, currentItems)))));
    }
    public record Source(PostDetails post, List<PostItem> items, String text, String fingerprint, String revision) {}
}
