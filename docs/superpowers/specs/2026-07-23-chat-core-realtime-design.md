# Chat Core and Realtime Design

## 1. Scope

The chat module is implemented inside the current Spring Boot WebFlux application
and reuses its JWT authentication, R2DBC MySQL connection, reactive Redis
templates, Reactor Kafka sender, Firebase push infrastructure, response wrapper,
and exception handling.

Version one supports:

- `DIRECT` conversations between exactly two users.
- `GROUP` conversations with only `USER` and `ADMIN` roles.
- Text, image, video, file, and audio messages.
- Replies, edits, soft deletion, history pagination, and offline synchronization.
- Monotonic delivered and read cursors per user and conversation.
- Native WebSocket delivery with multi-session presence.
- Kafka-based domain event distribution, Redis gateway routing, and push fallback.

End-to-end encryption and persistent chat-device keys are outside this scope.
The incomplete `ChatDevice` entity is not part of this implementation.

## 2. Business Rules

### Conversations

A `DIRECT` conversation is uniquely identified by a SHA-256 `directKey` generated
from the two lexicographically sorted user IDs. Creating the same pair again
returns the existing conversation. A user cannot create a direct conversation
with themselves.

A `GROUP` has a non-blank title and at least two initial users. Its creator is
the first `ADMIN`; other initial users are `USER`. A group must always retain at
least one active admin.

Direct conversations do not use administrative privileges. Both participants
have the `USER` role, cannot remove each other, and cannot leave the conversation
in version one.

### Membership

An active `ADMIN` can add a user directly. An active `USER` creates a pending
membership request instead. Only an active admin can approve or reject a request.
Approval and member activation occur in the same transaction.

Group admins can:

- Add users directly.
- Approve or reject membership requests.
- Change an active member between `USER` and `ADMIN`.
- Remove or ban another member.

An admin cannot demote, remove, or leave when that operation would leave no
active admin. A banned member cannot be reactivated through the normal add flow.

New and reactivated group members receive:

```text
joinedSeq = conversation.lastMessageSeq + 1
lastDeliveredSeq = conversation.lastMessageSeq
lastReadSeq = conversation.lastMessageSeq
```

They cannot fetch messages older than `joinedSeq`. Direct participants start at
sequence `1`.

### Messages

The authenticated JWT subject is always the actor and sender. Client-supplied
sender IDs are not accepted.

Every user message requires a UUID `clientMessageId`. The unique pair
`(senderId, clientMessageId)` makes retries idempotent. A retry returns the
previously persisted message.

`messageSeq` is scoped to a conversation and allocated while holding a database
row lock on that conversation. `SELECT MAX(message_seq) + 1` is forbidden.

Message rules:

- `TEXT` requires sanitized, non-blank content.
- `IMAGE`, `VIDEO`, `FILE`, and `AUDIO` require pre-uploaded media metadata.
- Media metadata contains at least `url`, `publicId`, `mimeType`, `size`, and
  `fileName`; dimensions and duration are optional.
- Media URLs must use HTTPS and MIME type must match the message type.
- `replyToSeq` must reference a visible, non-deleted earlier message in the same
  conversation.
- Only the sender can edit an undeleted text message.
- The sender can soft-delete their message. A group admin can soft-delete any
  message in that group.
- A deleted message retains its ID and sequence but responses omit its content
  and metadata.

## 3. Persistence Model

The schema contains four chat tables and uses `VARCHAR(36)` UUID primary keys and
`VARCHAR(64)` user IDs to match the current repository conventions.

### `conversations`

Stores `id`, `conversation_type`, `title`, nullable unique `direct_key`,
`last_message_seq`, `last_message_id`, `last_message_at`, `created_by`,
`created_at`, and `updated_at`.

### `conversation_members`

Stores `id`, `conversation_id`, `user_id`, `member_role`, `member_status`,
`joined_seq`, `last_delivered_seq`, `last_read_seq`, `muted_until`, `joined_at`,
and `left_at`.

The pair `(conversation_id, user_id)` is unique. Role values are `USER` and
`ADMIN`. Status values are `ACTIVE`, `LEFT`, `REMOVED`, and `BANNED`.

### `conversation_member_requests`

Stores the target user, requester, status, resolver, resolution time, and
timestamps. `pending_key` is a deterministic nullable hash. It is populated only
while status is `PENDING` and has a unique index, preventing concurrent duplicate
pending requests. Resolution clears `pending_key`.

Request statuses are `PENDING`, `APPROVED`, and `REJECTED`.

### `messages`

Stores `id`, `conversation_id`, `message_seq`, `client_message_id`, `sender_id`,
`message_type`, `content`, JSON `metadata`, `reply_to_seq`, `created_at`,
`edited_at`, and `deleted_at`.

Required unique indexes:

```text
UNIQUE(conversation_id, message_seq)
UNIQUE(sender_id, client_message_id)
```

The manual DDL is stored at `src/main/resources/db/manual/chat_schema.sql`.

## 4. REST API

All successful endpoints return the existing `ApiResponse<T>` shape.

```text
POST   /chat/conversations/direct
POST   /chat/conversations/groups
GET    /chat/conversations
GET    /chat/conversations/{conversationId}

POST   /chat/conversations/{conversationId}/members
GET    /chat/conversations/{conversationId}/member-requests
POST   /chat/member-requests/{requestId}/approve
POST   /chat/member-requests/{requestId}/reject
PATCH  /chat/conversations/{conversationId}/members/{userId}/role
DELETE /chat/conversations/{conversationId}/members/{userId}
POST   /chat/conversations/{conversationId}/leave

POST   /chat/conversations/{conversationId}/messages
GET    /chat/conversations/{conversationId}/messages
PATCH  /chat/messages/{messageId}
DELETE /chat/messages/{messageId}

POST   /chat/conversations/{conversationId}/delivered
POST   /chat/conversations/{conversationId}/read
```

Conversation listing uses a stable cursor based on `lastMessageAt` and ID.
Unread count is:

```text
max(0, lastMessageSeq - max(lastReadSeq, joinedSeq - 1))
```

Message retrieval supports one cursor mode per request:

- `afterSeq`: ascending offline sync after the last locally known sequence.
- `beforeSeq`: older history, fetched below the cursor and returned in ascending
  display order.

Limits default to `50` and are capped at `100`.

## 5. Transaction Boundaries

Reactive database work uses `TransactionalOperator`. The following operations
are atomic:

- Create a conversation and its initial memberships.
- Allocate sequence, insert message, and update conversation summary.
- Add or reactivate a member.
- Approve a request and activate its target member.
- Change role while preserving at least one admin.
- Update delivered/read cursors with `GREATEST`.

Sending a message follows this order:

```text
validate request and active membership
  -> return existing idempotent message when found
  -> lock conversation row
  -> validate reply and allocate sequence
  -> insert message and update conversation
  -> commit database transaction
  -> publish Kafka event
```

Kafka publication is deliberately outside the database transaction. Publication
is retried with a bounded reactive retry. If it still fails, the API keeps the
successful database result and logs `CHAT_EVENT_PUBLISH_FAILED`; clients recover
through the sync endpoint. A transactional outbox is deferred because this
version is constrained to four chat tables.

## 6. Realtime Delivery

Clients send messages through REST. WebSocket is used for server delivery and
cursor control, not as the source of message persistence.

The authenticated endpoint is:

```text
/ws/chat
```

Client frames:

```text
PING
ACK
READ
```

Server frames:

```text
PONG
EVENT
ERROR
```

Each JSON frame has `type`, `requestId`, and `payload`. An `ERROR` additionally
contains the shared numeric error `code`, public `message`, and `traceId`.

Each socket receives a server-generated session ID. Presence is refreshed by a
`PING` every 30 seconds:

```text
key: chat:presence:{userId}:{sessionId}
TTL: 120 seconds
value: gatewayId
```

Multiple sessions per user are supported. Disconnect removes the local session
and its Redis presence key.

All chat domain events use Kafka topic `chat.events`, keyed by conversation ID,
so events for one conversation stay ordered in a partition. Event types are:

```text
MESSAGE_CREATED
MESSAGE_UPDATED
MESSAGE_DELETED
MEMBER_REQUESTED
MEMBER_ADDED
MEMBER_REMOVED
MEMBER_ROLE_CHANGED
```

The delivery consumer resolves active recipients, including the sender so their
other devices receive the event. A local session is written directly. Events for
another application instance are routed over a Redis Pub/Sub gateway channel.
Redis Pub/Sub is transport only; losing a routed frame does not lose persisted
chat data.

When no active session exists, the delivery service invokes the existing push
notification infrastructure. If no ACK is observed after 10 seconds, it checks
the persisted delivered cursor and sends a push fallback if still needed.
Push data contains conversation ID, message ID, sequence, and type; the client
fetches authoritative content from the API.

`ACK` advances `lastDeliveredSeq`. `READ` advances both delivered and read
cursors. A cursor cannot exceed the conversation's current sequence and cannot
reference content before the member's `joinedSeq`.

## 7. Error Handling and Conventions

Chat uses the existing `AppException`, `ErrorCode`, `GlobalExceptionHandler`, and
`ApiResponse<T>`. No chat-specific REST exception handler is introduced.

The following entries are added directly to `ErrorCode`:

| Code | Name | HTTP status |
|---:|---|---|
| 1300 | `CHAT_REQUEST_INVALID` | `BAD_REQUEST` |
| 1301 | `CONVERSATION_NOT_FOUND` | `NOT_FOUND` |
| 1302 | `CONVERSATION_CREATE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1303 | `CONVERSATION_FETCH_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1304 | `CONVERSATION_FORBIDDEN` | `FORBIDDEN` |
| 1305 | `DIRECT_CONVERSATION_SELF_NOT_ALLOWED` | `BAD_REQUEST` |
| 1310 | `CHAT_MEMBER_NOT_FOUND` | `NOT_FOUND` |
| 1311 | `CHAT_MEMBER_ALREADY_EXISTS` | `CONFLICT` |
| 1312 | `CHAT_MEMBER_REQUEST_ALREADY_PENDING` | `CONFLICT` |
| 1313 | `CHAT_MEMBER_REQUEST_NOT_FOUND` | `NOT_FOUND` |
| 1314 | `CHAT_MEMBER_REQUEST_ALREADY_RESOLVED` | `CONFLICT` |
| 1315 | `CHAT_ADMIN_REQUIRED` | `FORBIDDEN` |
| 1316 | `CHAT_LAST_ADMIN_CANNOT_LEAVE` | `CONFLICT` |
| 1317 | `CHAT_MEMBER_UPDATE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1320 | `CHAT_MESSAGE_NOT_FOUND` | `NOT_FOUND` |
| 1321 | `CHAT_MESSAGE_CONTENT_INVALID` | `BAD_REQUEST` |
| 1322 | `CHAT_MESSAGE_TYPE_INVALID` | `BAD_REQUEST` |
| 1323 | `CHAT_MESSAGE_CREATE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1324 | `CHAT_MESSAGE_FETCH_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1325 | `CHAT_MESSAGE_UPDATE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1326 | `CHAT_MESSAGE_DELETE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1327 | `CHAT_MESSAGE_FORBIDDEN` | `FORBIDDEN` |
| 1328 | `CHAT_MESSAGE_REPLY_INVALID` | `BAD_REQUEST` |
| 1329 | `CHAT_MESSAGE_SEQUENCE_INVALID` | `BAD_REQUEST` |
| 1330 | `CHAT_CURSOR_UPDATE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1340 | `CHAT_WEBSOCKET_PROTOCOL_INVALID` | `BAD_REQUEST` |
| 1341 | `CHAT_PRESENCE_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1342 | `CHAT_DELIVERY_FAILED` | `INTERNAL_SERVER_ERROR` |
| 1343 | `CHAT_EVENT_PUBLISH_FAILED` | `INTERNAL_SERVER_ERROR` |

Services preserve an existing `AppException` and map unexpected infrastructure
errors to an operation-specific chat code. Public responses use the static
`ErrorCode.message`; internal detail is logged only. Logs follow the existing
pattern, for example:

```text
|ChatMessageService|sendMessage|conversationId={}|senderId={}|...
```

Code follows the repository's current package and reactive conventions:

- `controller`, `dto/request`, `dto/response`, `dto/event`, `entity`,
  `repository`, `service`, `listener`, `constant`, and `configuration`.
- Lombok entities and constructor injection with `@RequiredArgsConstructor`.
- `Mono`/`Flux` chains are returned; services do not invoke `subscribe()`.
- Kafka payloads are JSON strings sent with `KafkaSender<String, String>`.

## 8. Testing Strategy

Development follows red-green-refactor. Each service behavior starts with a
failing test.

Automated coverage includes:

- Service tests with JUnit 5, Mockito, StepVerifier, and Reactor virtual time.
- Controller response and validation tests with WebTestClient.
- WebSocket frame parsing, authenticated identity, presence refresh, disconnect,
  ACK, READ, and timeout fallback tests.
- MySQL Testcontainers repository tests for DDL, custom queries, constraints,
  row locks, and transactions.
- A concurrency test proving simultaneous sends receive distinct ordered
  sequences.
- Idempotency races for direct creation, membership requests, and
  `clientMessageId`.
- Authorization tests for inactive members, normal users, admins, message
  owners, and the last-admin invariant.
- Kafka/Redis delivery tests for local, remote-gateway, offline, and no-ACK
  branches.

The complete Maven test suite must pass before completion. Integration tests that
require Docker are kept in a separately selectable test group but are executed
during final verification when Docker is available.

## 9. Deferred Work

The following are explicitly deferred:

- End-to-end encryption and `ChatDevice` key management.
- Transactional outbox and guaranteed Kafka publication after database commit.
- Per-message, per-device delivery receipts.
- Calls, reactions, mentions, typing indicators, and message search.
- Client-side storage and user interface implementation.

