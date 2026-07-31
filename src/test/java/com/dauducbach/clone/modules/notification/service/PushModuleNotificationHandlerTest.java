package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.PostNotificationCacheKeys;
import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.CommentService;
import com.dauducbach.clone.modules.post.service.LikeService;
import com.dauducbach.clone.modules.post.service.PostService;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserFollowerService;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PushModuleNotificationHandlerTest {
    @Mock
    NotificationService notificationService;
    @Mock
    NotificationTemplatesRepository notificationTemplatesRepository;
    @Mock
    ReactiveRedisTemplate<String, Object> redisTemplate;
    @Mock
    UserFollowerService userFollowerService;
    @Mock
    UserIdentityQueryService userIdentityQueryService;
    @Mock
    PostService postService;
    @Mock
    CommentService commentService;
    @Mock
    LikeService likeService;

    @Test
    void handlePostUploadEventSendsNewPostPushNotification() {
        PushModuleNotificationHandler handler = newHandler();

        when(redisTemplate.hasKey(anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100))
                .thenReturn(Mono.just(FollowerListResponse.builder()
                        .followers(List.of(
                                FollowerListResponse.FollowerInfo.builder().userId("follower-1").build(),
                                FollowerListResponse.FollowerInfo.builder().userId("follower-2").build()
                        ))
                        .totalCount(2)
                        .currentPage(0)
                        .pageSize(100)
                        .hasNextPage(false)
                        .build()));
        when(notificationTemplatesRepository.findByActionType(UserActionType.NEW_POST))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(1)
                        .actionType(UserActionType.NEW_POST)
                        .template("{{USERNAME}} vua dang {{CONTENT}}")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handlePostUploadEvent("""
                {"post_id":"post-1","userId":"owner-1","content":"hello"}
                """);

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000)).sendNotification(captor.capture());

        NotificationRequest request = captor.getValue();
        assertThat(request.getActionType()).isEqualTo(UserActionType.NEW_POST);
        assertThat(request.getNotificationType()).isEqualTo(NotificationType.PUSH);
        assertThat(request.getRecipientIds()).containsExactly("follower-1", "follower-2");
        assertThat(request.getEntityId()).isEqualTo("post-1");
        assertThat(request.getContent()).isEqualTo("Bach vua dang hello");
    }

    @Test
    void handlePostLikeUsesCommentServiceForInteractedPeople() {
        PushModuleNotificationHandler handler = newHandler();

        when(redisTemplate.hasKey(anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(postService.getPostById("post-1")).thenReturn(Mono.just(post("post-1", "owner-1", "noi dung bai viet")));
        when(commentService.getDistinctCommenterUserIdsByPostId("post-1"))
                .thenReturn(Flux.just("commenter-1", "owner-1", "actor-1"));
        when(notificationTemplatesRepository.findByActionType(UserActionType.LIKE))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(1)
                        .actionType(UserActionType.LIKE)
                        .template("{{USERNAME}} liked {{CONTENT}}")
                        .build()));
        when(notificationTemplatesRepository.findByActionType(UserActionType.LIKE_OTHER_INTERACT_PEOPLE))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(2)
                        .actionType(UserActionType.LIKE_OTHER_INTERACT_PEOPLE)
                        .template("{{USERNAME}} other liked {{CONTENT}}")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleLikeEvent(new ConsumerRecord<>(
                "like_event",
                0,
                1L,
                "post-1",
                """
                        {"actorId":"actor-1","targetId":"post-1","targetType":"POST","targetOwnerId":"owner-1","likeCount":3}
                        """
        ));

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000).times(2)).sendNotification(captor.capture());

        assertThat(captor.getAllValues())
                .anySatisfy(request -> {
                    assertThat(request.getActionType()).isEqualTo(UserActionType.LIKE);
                    assertThat(request.getRecipientIds()).containsExactly("owner-1");
                    assertThat(request.getContent()).isEqualTo("Nam liked noi dung bai viet");
                })
                .anySatisfy(request -> {
                    assertThat(request.getActionType()).isEqualTo(UserActionType.LIKE_OTHER_INTERACT_PEOPLE);
                    assertThat(request.getRecipientIds()).containsExactly("commenter-1");
                    assertThat(request.getContent()).isEqualTo("Nam other liked noi dung bai viet");
                });
        verify(commentService, times(1)).getDistinctCommenterUserIdsByPostId("post-1");
    }

    @Test
    void handleCommentSuccessSendsOwnerParentOwnerAndInteractedPeopleWithoutOwners() {
        PushModuleNotificationHandler handler = newHandler();

        when(redisTemplate.hasKey(anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(postService.getPostOwnerIdByPostId("post-1")).thenReturn(Mono.just("owner-1"));
        when(postService.getPostById("post-1")).thenReturn(Mono.just(post("post-1", "owner-1", "noi dung bai viet")));
        when(commentService.getCommentById("parent-1"))
                .thenReturn(Mono.just(comment("parent-1", "parent-owner-1", "parent content")));
        when(commentService.countCommentsByPostId("post-1")).thenReturn(Mono.just(3L));
        when(commentService.getDistinctCommenterUserIdsByPostId("post-1"))
                .thenReturn(Flux.just("commenter-1", "owner-1", "parent-owner-1", "actor-1"));
        when(notificationTemplatesRepository.findByActionType(UserActionType.COMMENT))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(3)
                        .actionType(UserActionType.COMMENT)
                        .template("{{USERNAME}} comment {{COMMENT}} on {{CONTENT}}")
                        .build()));
        when(notificationTemplatesRepository.findByActionType(UserActionType.REPLY_COMMENT))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(4)
                        .actionType(UserActionType.REPLY_COMMENT)
                        .template("{{USERNAME}} reply {{REPLY}}")
                        .build()));
        when(notificationTemplatesRepository.findByActionType(UserActionType.COMMENT_OTHER_INTERACT_PEOPLE))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(5)
                        .actionType(UserActionType.COMMENT_OTHER_INTERACT_PEOPLE)
                        .template("{{USERNAME}} other comment {{COMMENT}} on {{CONTENT}}")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleCommentSuccessEvent("""
                {"commentId":"comment-1","userId":"actor-1","postId":"post-1","content":"hello","parentId":"parent-1"}
                """);

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000).times(3)).sendNotification(captor.capture());

        assertThat(captor.getAllValues())
                .anySatisfy(request -> {
                    assertThat(request.getActionType()).isEqualTo(UserActionType.COMMENT);
                    assertThat(request.getRecipientIds()).containsExactly("owner-1");
                    assertThat(request.getContent()).isEqualTo("Nam comment hello on noi dung bai viet");
                })
                .anySatisfy(request -> {
                    assertThat(request.getActionType()).isEqualTo(UserActionType.REPLY_COMMENT);
                    assertThat(request.getRecipientIds()).containsExactly("parent-owner-1");
                    assertThat(request.getContent()).isEqualTo("Nam reply hello");
                })
                .anySatisfy(request -> {
                    assertThat(request.getActionType()).isEqualTo(UserActionType.COMMENT_OTHER_INTERACT_PEOPLE);
                    assertThat(request.getRecipientIds()).containsExactly("commenter-1");
                    assertThat(request.getContent()).isEqualTo("Nam other comment hello on noi dung bai viet");
                });
    }

    @Test
    void handlePostLikeSkipsMutedPostRecipients() {
        PushModuleNotificationHandler handler = newHandler();

        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(postService.getPostById("post-1")).thenReturn(Mono.just(post("post-1", "owner-1", "noi dung bai viet")));
        when(redisTemplate.hasKey(PostNotificationCacheKeys.mutedPostNotification("post-1", "owner-1")))
                .thenReturn(Mono.just(false));
        when(redisTemplate.hasKey(PostNotificationCacheKeys.mutedPostNotification("post-1", "commenter-1")))
                .thenReturn(Mono.just(true));
        when(commentService.getDistinctCommenterUserIdsByPostId("post-1"))
                .thenReturn(Flux.just("commenter-1"));
        when(notificationTemplatesRepository.findByActionType(UserActionType.LIKE))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(6)
                        .actionType(UserActionType.LIKE)
                        .template("{{USERNAME}} liked {{CONTENT}}")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleLikeEvent("""
                {"actorId":"actor-1","targetId":"post-1","targetType":"POST","targetOwnerId":"owner-1","likeCount":3}
                """);

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000).times(1)).sendNotification(captor.capture());

        NotificationRequest request = captor.getValue();
        assertThat(request.getActionType()).isEqualTo(UserActionType.LIKE);
        assertThat(request.getRecipientIds()).containsExactly("owner-1");
    }

    @Test
    void handleCommentLikeRendersUsernameAndCommentContent() {
        PushModuleNotificationHandler handler = newHandler();

        when(redisTemplate.hasKey(anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(commentService.getCommentById("comment-1"))
                .thenReturn(Mono.just(comment("comment-1", "comment-owner-1", "noi dung binh luan")));
        when(notificationTemplatesRepository.findByActionType(UserActionType.LIKE_COMMENT))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(7)
                        .actionType(UserActionType.LIKE_COMMENT)
                        .template("{{USERNAME}} liked comment {{COMMENT}}")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleLikeEvent("""
                {"actorId":"actor-1","targetId":"comment-1","targetType":"COMMENT","targetOwnerId":"comment-owner-1","postId":"post-1"}
                """);

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService, timeout(1000)).sendNotification(captor.capture());

        NotificationRequest request = captor.getValue();
        assertThat(request.getActionType()).isEqualTo(UserActionType.LIKE_COMMENT);
        assertThat(request.getRecipientIds()).containsExactly("comment-owner-1");
        assertThat(request.getContent()).isEqualTo("Nam liked comment noi dung binh luan");
    }

    @Test
    void handleStoryLikeNotifiesOnlyTheOwnerWithInteractionDedupMetadata() {
        PushModuleNotificationHandler handler = newHandler();

        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(notificationTemplatesRepository.findByActionType(UserActionType.LIKE_STORY))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(8)
                        .actionType(UserActionType.LIKE_STORY)
                        .template("{{USERNAME}} đã thích tin của bạn")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class))).thenReturn(Mono.just("ok"));

        handler.handleLikeEvent("""
                {"actorId":"actor-1","targetId":"story-1","targetType":"STORY","targetOwnerId":"owner-1","interactionId":"interaction-1"}
                """).join();

        ArgumentCaptor<NotificationRequest> captor = ArgumentCaptor.forClass(NotificationRequest.class);
        verify(notificationService).sendNotification(captor.capture());

        NotificationRequest request = captor.getValue();
        assertThat(request.getActionType()).isEqualTo(UserActionType.LIKE_STORY);
        assertThat(request.getRecipientIds()).containsExactly("owner-1");
        assertThat(request.getEntityId()).isEqualTo("story-1");
        assertThat(request.getEntityType()).isEqualTo("STORY");
        assertThat(request.getContent()).isEqualTo("Nam đã thích tin của bạn");
        assertThat(request.getMetadata())
                .containsEntry("STORY_ID", "story-1")
                .containsEntry("STORY_OWNER_ID", "owner-1")
                .containsEntry("INTERACTION_ID", "interaction-1");
        assertThat(request.getDedupKey()).isEqualTo("LIKE_STORY:interaction-1");
    }

    private PushModuleNotificationHandler newHandler() {
        return new PushModuleNotificationHandler(
                notificationService,
                notificationTemplatesRepository,
                redisTemplate,
                userFollowerService,
                userIdentityQueryService,
                postService,
                commentService,
                likeService
        );
    }

    private UserDetails userDetails(String userId, String username) {
        return UserDetails.builder()
                .userId(userId)
                .username(username)
                .build();
    }

    private PostDetails post(String postId, String userId, String content) {
        return PostDetails.builder()
                .postId(postId)
                .userId(userId)
                .content(content)
                .build();
    }

    private Comment comment(String commentId, String userId, String content) {
        return Comment.builder()
                .id(commentId)
                .userId(userId)
                .content(content)
                .build();
    }
}
