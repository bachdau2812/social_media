package com.dauducbach.clone.modules.chat.service;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ChatMessageActionHydrationTest {
    @Test void configurableLargePinCollectionUsesBoundedReactionBatches() {
        var reactions=mock(MessageReactionService.class);
        when(reactions.getSnapshots(eq("me"),eq("c"),anyList())).thenAnswer(i->{List<String> ids=i.getArgument(2);
            return ids.size()>100?Mono.error(ChatMessageAccess.invalid("Supply at most 100 message IDs")):Mono.just(List.of());});
        var mapper=new ChatResponseMapper();
        var messages=java.util.stream.IntStream.range(0,101).mapToObj(i->mapper.toChatMessageResponse(ChatMessage.builder()
            .id("m"+i).conversationId("c").messageSeq(i+1).messageType(MessageType.TEXT).build())).toList();
        var service=new ChatMessageQueryService(mock(ChatAccessService.class),mock(ChatReadRepository.class),mapper,
            mock(ChatCursorService.class),mock(StoryAvailabilityPort.class),reactions);
        StepVerifier.create(service.hydrateMessages("me","c",messages)).expectNextMatches(items->items.size()==101).verifyComplete();
        verify(reactions,times(2)).getSnapshots(eq("me"),eq("c"),anyList());
    }
}
