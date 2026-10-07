package com.dauducbach.clone.modules.chat.service;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.List;

@Service @RequiredArgsConstructor
public class ChatMessageWriter {
    private final R2dbcEntityTemplate template;
    private final ConversationRepository conversations;
    private final ChatOutboxRepository outbox;
    private final ChatResponseMapper mapper;

    /** Conversation is already locked; attachmentWork participates in the same transaction. */
    public Mono<ChatMessage> write(ChatMessage message, Conversation conversation,
            Mono<Void> attachmentWork, List<String> recipientIds) {
        return template.insert(ChatMessage.class).using(message).flatMap(saved -> attachmentWork
                .then(conversations.updateMessageSummary(conversation.getId(), saved.getMessageSeq(), saved.getId(), saved.getCreatedAt()))
                .then(Mono.defer(() -> {
                    ChatMessageResponse response = mapper.toChatMessageResponse(saved);
                    return outbox.append(ChatEvent.messageCreated(response, recipientIds));
                }))
                .thenReturn(saved));
    }
}
