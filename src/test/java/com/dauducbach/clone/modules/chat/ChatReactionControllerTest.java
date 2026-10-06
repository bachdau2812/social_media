package com.dauducbach.clone.modules.chat;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.chat.constant.ReactionType;
import com.dauducbach.clone.modules.chat.controller.ChatController;
import com.dauducbach.clone.modules.chat.dto.request.*;
import com.dauducbach.clone.modules.chat.dto.response.*;
import com.dauducbach.clone.modules.chat.service.*;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ChatReactionControllerTest {
    MessageReactionService reactions = mock(MessageReactionService.class);
    ChatController controller = new ChatController(mock(ConversationService.class), mock(SendMessageService.class),
            mock(ChatMessageQueryService.class), mock(ChatCursorService.class), mock(ConversationMemberService.class),
            mock(ChatPresenceService.class), mock(ConversationMediaQueryService.class), reactions);
    UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken("me", "");
    @Test void putUsesAuthenticatedActorAndExactPayload() {
        var state = new ReactionState("m", 5, 1, "me", ReactionType.WOW, 0, List.of(new ReactionCount(ReactionType.WOW, 1)));
        when(reactions.set("me", "c", "m", ReactionType.WOW)).thenReturn(Mono.just(state));
        StepVerifier.create(controller.setReaction("me", auth, "c", "m", new SetMessageReactionRequest(ReactionType.WOW)))
                .assertNext(response -> assertThat(response.getResult()).isEqualTo(state)).verifyComplete();
    }
    @Test void spoofedActorIsRejectedBeforeMutation() {
        assertThatThrownBy(() -> controller.setReaction("other", auth, "c", "m",
                new SetMessageReactionRequest(ReactionType.HEART))).isInstanceOf(AppException.class);
        verifyNoInteractions(reactions);
    }
    @Test void missingAuthenticationIsRejectedBeforeMutation() {
        assertThatThrownBy(() -> controller.setReaction("me", null, "c", "m",
                new SetMessageReactionRequest(ReactionType.HEART))).isInstanceOf(AppException.class);
        verifyNoInteractions(reactions);
    }
    @Test void removeReturnsActorState() {
        var state = new ReactionState("m", 5, 2, "me", null, 0, List.of());
        when(reactions.remove("me", "c", "m")).thenReturn(Mono.just(state));
        StepVerifier.create(controller.removeReaction("me", auth, "c", "m"))
                .assertNext(response -> assertThat(response.getResult()).isEqualTo(state)).verifyComplete();
    }
    @Test void batchReturnsSnapshotArray() {
        var snapshots = List.of(new ReactionSnapshot("m", 5, 2, null, false, 0, List.of()));
        when(reactions.getSnapshots("me", "c", List.of("m"))).thenReturn(Mono.just(snapshots));
        StepVerifier.create(controller.getReactionSnapshots("me", auth, "c", new ReactionSnapshotsRequest(List.of("m"))))
                .assertNext(response -> assertThat(response.getResult()).isEqualTo(snapshots)).verifyComplete();
    }
    @Test void listReturnsExistingCursorEnvelope() {
        CursorPageResponse<MessageReactorResponse> page = new CursorPageResponse<>(List.of(), null, false);
        when(reactions.list("me", "c", "m", ReactionType.HEART, "a", 30)).thenReturn(Mono.just(page));
        StepVerifier.create(controller.getReactors("me", auth, "c", "m", ReactionType.HEART, "a", 30))
                .assertNext(response -> assertThat(response.getResult()).isEqualTo(page)).verifyComplete();
    }
}
