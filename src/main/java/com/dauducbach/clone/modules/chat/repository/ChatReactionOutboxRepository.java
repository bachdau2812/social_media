package com.dauducbach.clone.modules.chat.repository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.r2dbc.core.DatabaseClient;
/** Legacy Java adapter; production uses the single generic ChatOutboxRepository bean. */
@Deprecated
public class ChatReactionOutboxRepository extends ChatOutboxRepository {
    public ChatReactionOutboxRepository(DatabaseClient client, ObjectMapper mapper) { super(client,mapper); }
}
