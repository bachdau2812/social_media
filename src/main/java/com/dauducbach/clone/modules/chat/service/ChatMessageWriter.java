package com.dauducbach.clone.modules.chat.service;
import com.dauducbach.clone.modules.chat.entity.*;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service @RequiredArgsConstructor
public class ChatMessageWriter {
    private final R2dbcEntityTemplate template;
    private final ConversationRepository conversations;
    /** Conversation is already locked; attachmentWork participates in the same transaction. */
    public Mono<ChatMessage> write(ChatMessage message,Conversation conversation,Mono<Void> attachmentWork) {
        return template.insert(ChatMessage.class).using(message).flatMap(saved -> attachmentWork
                .then(conversations.updateMessageSummary(conversation.getId(),saved.getMessageSeq(),saved.getId(),saved.getCreatedAt())).thenReturn(saved));
    }
}
