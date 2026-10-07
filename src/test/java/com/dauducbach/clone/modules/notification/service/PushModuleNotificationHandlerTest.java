package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.notification.incoming.post.PushModuleNotificationHandler;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.publicapi.PostQuery;
import com.dauducbach.clone.modules.post.publicapi.CommentQuery;
import com.dauducbach.clone.modules.post.publicapi.PostNotificationMuteQuery;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.publicapi.UserRelationshipQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.dauducbach.clone.modules.notification.service.NotificationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    PostNotificationMuteQuery postNotificationMuteQuery;
    @Mock
    UserRelationshipQuery userFollowerService;
    @Mock
    UserIdentityQuery userIdentityQueryService;
    @Mock
    PostQuery postQuery;
    @Mock
    PostInteractionQuery postInteractionQuery;
    @Mock
    CommentQuery commentQuery;

    @Test
    void handlePostUploadEventSendsNewPostPushNotification() {
        PushModuleNotificationHandler handler = newHandler();

        when(postNotificationMuteQuery.isMuted(anyString(), anyString())).thenReturn(Mono.just(false));
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
    void postNotificationPersistenceFailureEscapesListenerForKafkaRetry() {
        PushModuleNotificationHandler handler = newHandler();
        when(postNotificationMuteQuery.isMuted(anyString(), anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("owner-1")).thenReturn(Mono.just("Bach"));
        when(userFollowerService.getFollowers("owner-1", 0, 100))
                .thenReturn(Mono.just(FollowerListResponse.builder()
                        .followers(List.of(FollowerListResponse.FollowerInfo.builder().userId("follower-1").build()))
                        .hasNextPage(false)
                        .build()));
        when(notificationTemplatesRepository.findByActionType(UserActionType.NEW_POST))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .actionType(UserActionType.NEW_POST)
                        .template("{{USERNAME}} posted")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class)))
                .thenReturn(Mono.error(new IllegalStateException("database unavailable")));

        assertThatThrownBy(() -> handler.handlePostUploadEvent(
                "{\"postId\":\"post-2\",\"userId\":\"owner-1\"}").join())
                .hasRootCauseMessage("database unavailable");
    }

    @Test
    void handlePostLikeUsesCommentServiceForInteractedPeople() {
        PushModuleNotificationHandler handler = newHandler();

        when(postNotificationMuteQuery.isMuted(anyString(), anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(postQuery.findSnapshot("post-1")).thenReturn(Mono.just(new PostQuery.PostSnapshot("post-1", "owner-1", "noi dung bai viet")));
        when(commentQuery.findDistinctCommenterUserIdsByPostId("post-1"))
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
        verify(commentQuery, times(1)).findDistinctCommenterUserIdsByPostId("post-1");
    }

    @Test
    void handleCommentSuccessSendsOwnerParentOwnerAndInteractedPeopleWithoutOwners() {
        PushModuleNotificationHandler handler = newHandler();

        when(postNotificationMuteQuery.isMuted(anyString(), anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(postQuery.findSnapshot("post-1")).thenReturn(Mono.just(new PostQuery.PostSnapshot("post-1", "owner-1", "noi dung bai viet")));
        when(commentQuery.findById("parent-1"))
                .thenReturn(Mono.just(new CommentQuery.CommentSnapshot("parent-1", "post-1", "parent-owner-1", null, "parent content")));
        when(commentQuery.countByPostId("post-1")).thenReturn(Mono.just(3L));
        when(commentQuery.findDistinctCommenterUserIdsByPostId("post-1"))
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
        when(postQuery.findSnapshot("post-1")).thenReturn(Mono.just(new PostQuery.PostSnapshot("post-1", "owner-1", "noi dung bai viet")));
        when(postNotificationMuteQuery.isMuted("post-1", "owner-1"))
                .thenReturn(Mono.just(false));
        when(postNotificationMuteQuery.isMuted("post-1", "commenter-1"))
                .thenReturn(Mono.just(true));
        when(commentQuery.findDistinctCommenterUserIdsByPostId("post-1"))
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

        when(postNotificationMuteQuery.isMuted(anyString(), anyString())).thenReturn(Mono.just(false));
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(commentQuery.findById("comment-1"))
                .thenReturn(Mono.just(new CommentQuery.CommentSnapshot("comment-1", "post-1", "comment-owner-1", null, "noi dung binh luan")));
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

    @Test
    void handleStoryLikePropagatesNotificationPersistenceFailureForKafkaRetry() {
        PushModuleNotificationHandler handler = newHandler();
        when(userIdentityQueryService.resolveUsername("actor-1")).thenReturn(Mono.just("Nam"));
        when(notificationTemplatesRepository.findByActionType(UserActionType.LIKE_STORY))
                .thenReturn(Mono.just(NotificationTemplates.builder()
                        .id(8)
                        .actionType(UserActionType.LIKE_STORY)
                        .template("{{USERNAME}} liked your story")
                        .build()));
        when(notificationService.sendNotification(any(NotificationRequest.class)))
                .thenReturn(Mono.error(new IllegalStateException("notification database unavailable")));

        assertThatThrownBy(() -> handler.handleLikeEvent("""
                {"actorId":"actor-1","targetId":"story-1","targetType":"STORY","targetOwnerId":"owner-1","interactionId":"interaction-1"}
                """).join())
                .hasRootCauseMessage("notification database unavailable");
    }
    private PushModuleNotificationHandler newHandler() {
        return new PushModuleNotificationHandler(
                notificationService,
                notificationTemplatesRepository,
                postNotificationMuteQuery,
                userFollowerService,
                userIdentityQueryService,
                postQuery,
                postInteractionQuery,
                commentQuery
        );
    }

    private UserDetails userDetails(String userId, String username) {
        return UserDetails.builder()
                .userId(userId)
                .username(username)
                .build();
    }

}
