# Chat Module Design

## Goal

Implement the chat module in `social_media` using the richer `social_media-chat-core-realtime` design: persistent MySQL/R2DBC storage, monotonic per-conversation message sequences, idempotent client retries, conversation membership checks, cursor sync, and Kafka event publication after message creation.

## Scope

- Use the four core business tables: `conversations`, `conversation_members`, `conversation_member_requests`, and `messages`.
- Add `direct_key` on `conversations` and `pending_key` on `conversation_member_requests` so the database can reject duplicate direct conversations and duplicate pending member requests.
- Replace the incomplete chat skeleton with focused constants, entities, repositories, DTOs, services, controller endpoints, and manual SQL.
- Keep the project style: Java 21, Spring Boot WebFlux, Spring Data R2DBC, Reactor `Mono`/`Flux`, `ApiResponse`, `AppException`, and request-supplied actor/user ids.

## API Surface

- `POST /chat/conversations/direct`: create or fetch a direct conversation for two users.
- `POST /chat/conversations/group`: create a group conversation with the creator as admin.
- `GET /chat/conversations/{conversationId}`: get one conversation if the actor is an active member.
- `GET /chat/conversations`: cursor-list active conversations for an actor.
- `POST /chat/conversations/{conversationId}/messages`: send a message with server-assigned `messageSeq`.
- `GET /chat/conversations/{conversationId}/messages`: sync messages after or before a sequence.
- `PUT /chat/conversations/{conversationId}/cursor`: advance delivered/read cursors monotonically.
- `POST /chat/conversations/{conversationId}/members`: admin adds directly; non-admin creates a pending request.
- `POST /chat/member-requests/{requestId}/approve` and `/reject`: admin resolves pending requests.

## Data Model

Entities map to the SQL in `src/main/resources/db/manual/chat_schema.sql`.

- `Conversation`: stores type, title, `directKey`, last-message summary, creator, timestamps.
- `ConversationMember`: stores role, status, joined sequence, delivered/read cursors, mute/leave timestamps.
- `ConversationMemberRequest`: stores target user, requester, status, resolver, `pendingKey`, timestamps.
- `ChatMessage`: stores conversation sequence, `clientMessageId`, sender, type, content/metadata, reply and edit/delete timestamps.

## Business Rules

- Direct conversations are unique by `direct_key`, generated from the sorted pair of user ids.
- A message sender must be an active conversation member.
- User-created messages require a canonical UUID `clientMessageId`.
- Retry with the same `(senderId, clientMessageId)` returns the already-created message instead of inserting another row.
- Message sequence is assigned by locking the conversation row with `SELECT ... FOR UPDATE`, incrementing `last_message_seq`, inserting the message, then updating the conversation summary in one reactive transaction.
- `replyToSeq` must reference an existing earlier message in the same conversation.
- Cursor updates only move forward using `GREATEST`.
- Non-admin member additions create a pending request guarded by `pending_key`; admin additions insert an active member directly.

## Events

After a message transaction succeeds, publish a JSON event to Kafka topic `chat.message.created` with key `conversationId`. Database remains the source of truth; if Kafka publish fails, the API will surface `CHAT_EVENT_PUBLISH_FAILED`.

## Testing

- Unit tests cover validator, access checks, conversation creation, message send/idempotency, cursor updates, and member request behavior.
- Repository/contract tests cover SQL naming, error codes, enum values, and schema constraints.
- Final verification uses `.\mvnw.cmd test` from `social_media`.
