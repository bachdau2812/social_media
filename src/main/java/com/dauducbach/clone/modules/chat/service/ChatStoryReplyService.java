package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.CreateDirectConversationRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.request.StoryContextRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ChatStoryReplyService implements StoryReplyMessaging {
    private final ConversationService conversationService;
    private final SendMessageService sendMessageService;

    @Override
    public Mono<ChatMessageResponse> send(StoryReplyCommand command) {
        return conversationService.createDirect(
                        command.senderId(),
                        new CreateDirectConversationRequest(command.ownerId()))
                .flatMap(conversation -> sendMessageService.sendMessage(
                        command.senderId(),
                        conversation.id(),
                        new SendMessageRequest(
                                command.clientMessageId(),
                                MessageType.STORY_REPLY,
                                command.content(),
                                null,
                                null,
                                command.ownerId(),
                                null,
                                new StoryContextRequest(
                                        command.storyId(),
                                        command.ownerId(),
                                        command.mediaType(),
                                        command.previewAtMs(),
                                        command.expiresAt()))));
    }
}
