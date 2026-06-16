package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.utils.GetVectorEmbedding;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.modules.post.repositoty.PostVectorRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class PostVectorService {
    private static final Logger log = LoggerFactory.getLogger(PostVectorService.class);

    GetVectorEmbedding getVectorEmbedding;
    PostVectorRepository postVectorRepository;

    public Mono<Void> processPostEmbedding(String postId, String content) {
        log.info("|PostVectorService|processPostEmbedding|start|postId={}", postId);
        return getVectorEmbedding.getEmbedding(content)
                .flatMap(vector -> postVectorRepository.save(PostVector.builder()
                        .postId(postId)
                        .contentVector(vector)
                        .build()))
                .then()
                .doOnSuccess(unused -> log.info("|PostVectorService|processPostEmbedding|success|postId={}", postId))
                .doOnError(error -> log.error("|PostVectorService|processPostEmbedding|failed|postId={}|error={}", postId, error.getMessage()))
                .onErrorResume(error -> Mono.empty());
    }
}

