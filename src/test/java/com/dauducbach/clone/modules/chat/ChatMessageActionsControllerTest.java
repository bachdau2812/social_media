package com.dauducbach.clone.modules.chat;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.chat.controller.ChatController;
import com.dauducbach.clone.modules.chat.service.*;
import com.dauducbach.clone.modules.chat.dto.request.*;
import com.dauducbach.clone.modules.chat.dto.response.*;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.List;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class ChatMessageActionsControllerTest {
    ChatRecallService recall=mock(ChatRecallService.class);ChatPinService pins=mock(ChatPinService.class);
    ChatForwardService forward=mock(ChatForwardService.class);ChatMessageStateService state=mock(ChatMessageStateService.class);
    ChatController controller=new ChatController(mock(ConversationService.class),mock(SendMessageService.class),mock(ChatMessageQueryService.class),
        mock(ChatCursorService.class),mock(ConversationMemberService.class),mock(ChatPresenceService.class),mock(ConversationMediaQueryService.class),
        mock(MessageReactionService.class),recall,pins,forward,state);
    UsernamePasswordAuthenticationToken auth=new UsernamePasswordAuthenticationToken("me", "");
    @Test void allActionsRejectSpoofedActorBeforeServiceCalls() {
        assertThatThrownBy(()->controller.recall("other",auth,"c","m")).isInstanceOf(AppException.class);
        assertThatThrownBy(()->controller.states("other",auth,"c",new MessageStatesRequest(List.of("m")))).isInstanceOf(AppException.class);
        assertThatThrownBy(()->controller.forward("other",auth,"c",new ForwardMessageRequest("source","m","key"))).isInstanceOf(AppException.class);
        assertThatThrownBy(()->controller.pins("other",auth,"c")).isInstanceOf(AppException.class);
        assertThatThrownBy(()->controller.pin("other",auth,"c","m")).isInstanceOf(AppException.class);
        assertThatThrownBy(()->controller.unpin("other",auth,"c","m")).isInstanceOf(AppException.class);
        verifyNoInteractions(recall,pins,forward,state);
    }
    @Test void missingAuthenticationRejected() {assertThatThrownBy(()->controller.recall("me",null,"c","m")).isInstanceOf(AppException.class);verifyNoInteractions(recall);}
    @Test void pinAndStateReturnExistingApiEnvelope() {
        var collection=new PinCollectionResponse(4,true,List.of());when(pins.get("me","c")).thenReturn(Mono.just(collection));
        StepVerifier.create(controller.pins("me",auth,"c")).assertNext(r->assertThat(r.getResult()).isEqualTo(collection)).verifyComplete();
        when(state.get("me","c",List.of("m"))).thenReturn(Mono.just(List.of()));
        StepVerifier.create(controller.states("me",auth,"c",new MessageStatesRequest(List.of("m")))).assertNext(r->assertThat(r.getResult()).isEmpty()).verifyComplete();
    }
}
