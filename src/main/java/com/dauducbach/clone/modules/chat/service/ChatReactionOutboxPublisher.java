package com.dauducbach.clone.modules.chat.service;
import com.dauducbach.clone.modules.chat.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
/** Legacy Java adapter; only ChatOutboxPublisher is scheduled in production. */
@Deprecated
public class ChatReactionOutboxPublisher extends ChatOutboxPublisher {
    public ChatReactionOutboxPublisher(ChatReactionOutboxRepository repository, ChatEventPublisher kafka,
            MessageReactionRepository reactions, ObjectMapper mapper) { super(repository,kafka,reactions,mapper); }
}
