package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.application.PostDetailsCache;
import com.dauducbach.clone.modules.post.application.PostNotificationMuteStore;
import com.dauducbach.clone.modules.post.application.PostPublicationMessaging;
import com.dauducbach.clone.modules.post.repository.*;
import com.dauducbach.clone.modules.post.dto.request.*;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.*;
import reactor.core.publisher.*;
import reactor.kafka.sender.*;
import reactor.test.StepVerifier;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class PostVectorLifecycleTest {
    final PostDetailsRepository posts = mock(PostDetailsRepository.class);
    final PostItemRepository items = mock(PostItemRepository.class);
    final R2dbcEntityTemplate sql = mock(R2dbcEntityTemplate.class, RETURNS_DEEP_STUBS);
    final PostDetailsCache cache = mock(PostDetailsCache.class);
    final PostNotificationMuteStore muteStore = mock(PostNotificationMuteStore.class);
    final PostPublicationMessaging publicationMessaging = mock(PostPublicationMessaging.class);
    final ReactiveRedisTemplate<String,String> redis = mock(ReactiveRedisTemplate.class);
    final ReactiveValueOperations<String,String> values = mock(ReactiveValueOperations.class);
    final KafkaSender<String,String> kafka = mock(KafkaSender.class);
    final PostSseService sse = mock(PostSseService.class);
    final PostVectorService vectors = mock(PostVectorService.class);
    final PostMediaModerationOrchestrator moderation = mock(PostMediaModerationOrchestrator.class);
    final List<String> trace = new ArrayList<>();
    final PostService service = new PostService(posts, items, sql, cache, muteStore, publicationMessaging, moderation, vectors);
    PostVectorLifecycleTest() {
        when(cache.get(anyString())).thenReturn(Mono.empty());
        when(cache.put(any())).thenReturn(Mono.just(true));
        when(cache.evict(anyString())).thenReturn(Mono.empty());
        when(cache.evictAll(anyList())).thenReturn(Mono.empty());
        when(redis.opsForValue()).thenReturn(values);
        when(values.delete(anyString())).thenReturn(Mono.just(true));
        when(values.set(anyString(), anyString(), any())).thenReturn(Mono.just(true));
        when(sse.sendToUser(anyString(), anyString(), anyString())).thenReturn(Mono.empty());
        when(vectors.deletePost(anyString(), anyString())).thenAnswer(i -> Mono.fromRunnable(() -> trace.add("fence:" + i.getArgument(0))));
        when(vectors.retryDeletedPost(anyString(), anyString())).thenAnswer(i -> Mono.fromRunnable(() -> trace.add("fence:" + i.getArgument(0))));
        when(vectors.deletePostsByAuthor(anyString(), anyList())).thenAnswer(i -> Mono.fromRunnable(() -> trace.add("bulk-fence:"+ i.getArgument(1))));
        when(publicationMessaging.publishUpdated(any())).thenAnswer(i -> Mono.fromRunnable(() -> trace.add("event:post_update_event")));
        when(publicationMessaging.publishApproved(any(), anyString())).thenAnswer(i -> Mono.fromRunnable(() -> trace.add("event:post_upload_event")));
        when(kafka.send(any())).thenAnswer(i -> Flux.from((org.reactivestreams.Publisher<SenderRecord<String,String,String>>)i.getArgument(0))
                .doOnNext(record -> trace.add("event:"+record.topic()))
                .map(record -> { SenderResult<String> result = mock(SenderResult.class); return result; }));
    }
    @Test void singleDeletionFencesAfterSqlAndRetainsRetryAfterSqlAbsent() {
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("text")), Mono.empty());
        when(posts.deleteById("p")).thenReturn(Mono.fromRunnable(() -> trace.add("sql-delete")));
        service.deletePostById("p", "a").block(); service.deletePostById("p", "a").block();
        assertThat(trace).containsExactly("sql-delete", "fence:p", "fence:p");
    }
    @Test void fenceFailureAfterSqlSuccessReachesCallerAndCanRetryWithoutSource() {
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("text")), Mono.empty());
        when(posts.deleteById("p")).thenReturn(Mono.fromRunnable(() -> trace.add("sql-delete")));
        RuntimeException failure = new IllegalStateException("ES unavailable"); when(vectors.deletePost("p", "a")).thenReturn(Mono.error(failure));
        StepVerifier.create(service.deletePostById("p", "a")).expectErrorMatches(e -> e.getCause() == failure).verify();
        assertThat(trace).containsExactly("sql-delete");
        when(vectors.retryDeletedPost("p", "a")).thenReturn(Mono.fromRunnable(() -> trace.add("fence:p")));
        service.deletePostById("p", "a").block(); assertThat(trace).containsExactly("sql-delete", "fence:p");
    }
    @Test void sqlDeletionFailurePreservesLiveVectorWithoutPermanentTombstone() {
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("text")));
        RuntimeException failure = new RuntimeException("SQL unavailable"); when(posts.deleteById("p")).thenReturn(Mono.error(failure));
        StepVerifier.create(service.deletePostById("p", "a")).expectErrorMatches(e -> e.getCause() == failure).verify();
        verifyNoInteractions(vectors);
    }
    @Test void bulkDeletionCarriesSqlIdsAndStillFencesWhenSqlAlreadyAbsent() {
        when(posts.findAllByUserId("a")).thenReturn(Flux.just(PostVectorServiceTest.post("text")), Flux.empty());
        when(posts.deleteByUserId("a")).thenReturn(Mono.fromRunnable(() -> trace.add("sql-bulk-delete")));
        service.deletePostsByUserId("a").block(); service.deletePostsByUserId("a").block();
        assertThat(trace).containsExactly("sql-bulk-delete", "bulk-fence:[p]", "sql-bulk-delete", "bulk-fence:[]");
    }
    @Test void updateHashtagsAndCaptionsPublishesRebuildAfterSourcePersistence() {
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("old")));
        when(items.findByPostIdOrderByOrderNumberAsc("p")).thenReturn(Flux.empty());
        when(posts.save(any(PostDetails.class))).thenAnswer(i -> Mono.fromSupplier(() -> { trace.add("sql-save"); return i.getArgument(0); }));
        PostUpdateRequest update = new PostUpdateRequest(); update.setPostId("p"); update.setUserId("a"); update.setContent("new"); update.setHashtag(List.of("tag"));
        service.updatePost(update).block(); assertThat(trace).containsExactly("sql-save", "event:post_update_event");
    }
    @Test void approvedTextCreatePublishesFromPersistedPost() {
        when(sql.insert(PostDetails.class).using(any(PostDetails.class))).thenAnswer(i -> Mono.fromSupplier(() -> { trace.add("sql-insert"); return i.getArgument(0); }));
        PostCreateRequest create = new PostCreateRequest(); create.setUserId("a"); create.setContent("hello"); create.setHashtags(List.of("tag"));
        service.createPost(create).block(); assertThat(trace).containsExactly("sql-insert", "event:post_upload_event");
    }
    @Test void publicationFailureCannotTurnAnUpdateIntoSuccessfulPublish() {
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("old")));
        when(items.findByPostIdOrderByOrderNumberAsc("p")).thenReturn(Flux.empty());
        when(posts.save(any(PostDetails.class))).thenAnswer(i -> Mono.just(i.getArgument(0)));
        RuntimeException failure = new IllegalStateException("publication rejected");
        when(publicationMessaging.publishUpdated(any())).thenReturn(Mono.error(failure));
        PostUpdateRequest update = new PostUpdateRequest(); update.setPostId("p"); update.setUserId("a"); update.setContent("new");
        StepVerifier.create(service.updatePost(update)).expectErrorMatches(e -> e.getCause() == failure).verify();
    }
    @Test void rejectedModerationCleanupFencesSourceEvenWhenAssetCleanupFails() throws Exception {
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("text")));
        MediaAssets assets = mock(MediaAssets.class);
        when(assets.deleteAssets(anyList())).thenReturn(Mono.error(new RuntimeException("asset cleanup")));
        when(assets.deleteAssetsForOwner("p", com.dauducbach.clone.modules.media.constant.OwnerType.POST))
                .thenReturn(Mono.empty());
        when(posts.deleteById("p")).thenReturn(Mono.fromRunnable(() -> trace.add("sql-delete")));
        when(items.deleteByPostId("p")).thenReturn(Mono.empty());
        PostMediaModerationOrchestrator orchestrator = new PostMediaModerationOrchestrator(posts, assets, items, redis, sse, kafka,
                mock(MediaModerationProvider.class), vectors);
        var cleanup = PostMediaModerationOrchestrator.class.getDeclaredMethod("cleanupFailedPost", String.class, List.class); cleanup.setAccessible(true);
        ((Mono<Void>)cleanup.invoke(orchestrator, "p", List.of())).block();
        assertThat(trace).containsExactly("sql-delete", "fence:p");
    }
}
