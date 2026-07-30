# Thiết kế Chat Service tối giản

## 1. Mục tiêu thiết kế

Kiến trúc này hướng tới các yêu cầu:

- Lưu tin nhắn bền vững trước khi phân phối realtime.
- Giữ đúng thứ tự tin nhắn trong từng cuộc trò chuyện.
- Chống lưu trùng khi client retry request.
- Hỗ trợ người dùng online, offline và đồng bộ lại khi reconnect.
- Có cơ chế duyệt thành viên khi người yêu cầu không phải ADMIN.
- Có thể scale Kafka consumer, WebSocket Gateway và Push Worker độc lập.
- Giữ mô hình database tối giản với đúng bốn bảng nghiệp vụ.

Database là **source of truth** của tin nhắn. Kafka chỉ dùng để truyền sự kiện; WebSocket và push notification chỉ là các kênh phân phối.

---

## 2. Kiến trúc tổng thể

```text
Client
  │
  │ HTTPS
  ▼
Chat API
  │
  │ MySQL transaction
  ▼
MySQL
  ├── conversations
  ├── conversation_member
  ├── conversation_member_requests
  └── messages
  │
  │ Sau khi transaction commit
  ▼
Kafka: chat.message.created
Key = conversationId
  │
  ▼
Delivery Service
  ├── tra Redis Presence
  ├── online  → WebSocket Gateway
  └── offline → Push Notification
```

Khi client reconnect:

```text
Client
  │
  └── GET /conversations/{id}/messages?afterSeq=...
            │
            ▼
        MySQL messages
```

### Vai trò từng thành phần

#### MySQL

Lưu dữ liệu bền vững:

- Cuộc trò chuyện.
- Thành viên.
- Yêu cầu thêm thành viên.
- Tin nhắn.
- Sequence và read cursor.

#### Kafka

Truyền event giữa các service:

```text
Topic: chat.message.created
Key: conversationId
```

Không sử dụng Kafka làm nơi lưu hộp thư offline.

#### Redis

Lưu dữ liệu ngắn hạn:

- Online presence.
- WebSocket session.
- `userId → gatewayId`.
- Heartbeat TTL.
- Trạng thái foreground/background nếu cần.

#### WebSocket Gateway

- Giữ socket local.
- Ping/pong.
- Gửi tin realtime.
- Nhận ACK từ client.
- Không phải source of truth.

#### Push Worker

- Gửi FCM/APNs.
- Push chỉ dùng để thông báo hoặc đánh thức ứng dụng.
- Client vẫn phải đồng bộ tin nhắn từ API.

---

## 3. Quy ước enum đề xuất

Các enum có thể lưu bằng `TINYINT` trong database và ánh xạ sang enum trong Java.

### `conversation_type`

```text
1 = DIRECT
2 = GROUP
3 = CHANNEL
```

### `member_role`

```text
1 = MEMBER
2 = MODERATOR
3 = ADMIN
```

### `member_status`

```text
1 = ACTIVE
2 = LEFT
3 = REMOVED
4 = BANNED
```

### `request_status`

```text
1 = PENDING
2 = APPROVED
3 = REJECTED
4 = CANCELLED
```

### `message_type`

```text
1 = TEXT
2 = IMAGE
3 = VIDEO
4 = FILE
5 = AUDIO
6 = SYSTEM
```

---

# 4. Database schema

## 4.1 Bảng `conversations`

### Các trường

```text
Table conversations:
    - id
    - conversation_type
    - title
    - last_message_seq
    - last_message_id
    - last_message_at
    - created_at
    - created_by
    - updated_at
```

### Mục đích

- Đại diện cho một cuộc trò chuyện.
- `last_message_seq` là bộ đếm sequence mới nhất của conversation.
- `last_message_id` và `last_message_at` giúp lấy danh sách conversation nhanh hơn.
- Khi chưa có tin nhắn:

```text
last_message_seq = 0
last_message_id  = NULL
last_message_at  = NULL
```

### DDL đề xuất

```sql
CREATE TABLE conversations (
    id                    BIGINT UNSIGNED NOT NULL,
    conversation_type     TINYINT UNSIGNED NOT NULL,
    title                 VARCHAR(255) NULL,

    last_message_seq      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_message_id       BIGINT UNSIGNED NULL,
    last_message_at       DATETIME(3) NULL,

    created_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    created_by            BIGINT UNSIGNED NOT NULL,
    updated_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                        ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (id),

    CONSTRAINT chk_conversation_type
        CHECK (conversation_type IN (1, 2, 3)),

    CONSTRAINT chk_last_message_seq
        CHECK (last_message_seq >= 0)
) ENGINE = InnoDB;
```

### Lưu ý về direct conversation

Với đúng các trường hiện tại, database **không thể tự bảo đảm một cặp user chỉ có một DIRECT conversation**.

Có hai lựa chọn:

1. Chấp nhận kiểm tra ở application.
2. Sau này thêm bảng ánh xạ direct conversation hoặc thêm hai cột user pair.

Với mô hình tối giản hiện tại, service có thể tìm conversation DIRECT bằng cách join `conversation_member`. Tuy nhiên vẫn phải chấp nhận rằng unique theo cặp user không được database bảo vệ tuyệt đối.

---

## 4.2 Bảng `conversation_member`

### Các trường

```text
Table conversation_member:
    - id
    - conversation_id
    - nickname
    - user_id
    - member_role
    - member_status
    - joined_seq
    - last_delivered_seq
    - last_read_seq
    - muted_until
    - joined_at
    - left_at
```

### Mục đích

- Lưu những user đã thực sự là thành viên.
- Không đưa request pending vào bảng này.
- Lưu read cursor và delivered cursor theo từng user.

### Ý nghĩa cursor

```text
joined_seq
→ sequence đầu tiên user có quyền xem

last_delivered_seq
→ client đã xác nhận nhận đến sequence nào

last_read_seq
→ user đã đọc đến sequence nào
```

Ví dụ user tham gia khi conversation đã có 500 message:

```text
joined_seq         = 501
last_delivered_seq = 500
last_read_seq      = 500
```

Nếu cho phép xem toàn bộ lịch sử:

```text
joined_seq = 1
```

### DDL đề xuất

```sql
CREATE TABLE conversation_member (
    id                    BIGINT UNSIGNED NOT NULL,
    conversation_id       BIGINT UNSIGNED NOT NULL,
    nickname              VARCHAR(100) NULL,
    user_id               BIGINT UNSIGNED NOT NULL,

    member_role           TINYINT UNSIGNED NOT NULL DEFAULT 1,
    member_status         TINYINT UNSIGNED NOT NULL DEFAULT 1,

    joined_seq            BIGINT UNSIGNED NOT NULL DEFAULT 1,
    last_delivered_seq    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_read_seq         BIGINT UNSIGNED NOT NULL DEFAULT 0,

    muted_until           DATETIME(3) NULL,
    joined_at             DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    left_at               DATETIME(3) NULL,

    PRIMARY KEY (id),

    UNIQUE KEY uk_conversation_user (
        conversation_id,
        user_id
    ),

    KEY idx_user_conversations (
        user_id,
        member_status,
        conversation_id
    ),

    KEY idx_conversation_members (
        conversation_id,
        member_status,
        user_id
    ),

    CONSTRAINT fk_member_conversation
        FOREIGN KEY (conversation_id)
        REFERENCES conversations(id),

    CONSTRAINT chk_member_role
        CHECK (member_role IN (1, 2, 3)),

    CONSTRAINT chk_member_status
        CHECK (member_status IN (1, 2, 3, 4)),

    CONSTRAINT chk_member_cursor
        CHECK (
            joined_seq >= 1
            AND last_delivered_seq >= 0
            AND last_read_seq >= 0
        )
) ENGINE = InnoDB;
```

### Cập nhật cursor

Cursor phải chỉ tăng, không được lùi.

```sql
UPDATE conversation_member
SET last_read_seq = GREATEST(last_read_seq, :newReadSeq)
WHERE conversation_id = :conversationId
  AND user_id = :userId
  AND member_status = 1;
```

Tương tự với delivered cursor:

```sql
UPDATE conversation_member
SET last_delivered_seq =
        GREATEST(last_delivered_seq, :newDeliveredSeq)
WHERE conversation_id = :conversationId
  AND user_id = :userId
  AND member_status = 1;
```

Service cần kiểm tra:

```text
last_read_seq <= last_delivered_seq
last_delivered_seq <= conversations.last_message_seq
```

---

## 4.3 Bảng `conversation_member_requests`

### Các trường

```text
Table conversation_member_requests:
    - id
    - conversation_id
    - target_user_id
    - requested_by
    - request_status
    - approved_by
    - rejected_by
    - accepted_at
    - rejected_at
    - created_at
```

### Mục đích

- Lưu yêu cầu thêm user vào group.
- Nếu người yêu cầu là ADMIN, có thể thêm trực tiếp vào `conversation_member`.
- Nếu người yêu cầu không phải ADMIN, tạo request ở trạng thái `PENDING`.

### DDL đề xuất

```sql
CREATE TABLE conversation_member_requests (
    id                    BIGINT UNSIGNED NOT NULL,
    conversation_id       BIGINT UNSIGNED NOT NULL,
    target_user_id        BIGINT UNSIGNED NOT NULL,
    requested_by          BIGINT UNSIGNED NOT NULL,

    request_status        TINYINT UNSIGNED NOT NULL DEFAULT 1,

    approved_by           BIGINT UNSIGNED NULL,
    rejected_by           BIGINT UNSIGNED NULL,

    accepted_at           DATETIME(3) NULL,
    rejected_at           DATETIME(3) NULL,
    created_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (id),

    KEY idx_conversation_request_status (
        conversation_id,
        request_status,
        created_at
    ),

    KEY idx_target_request (
        target_user_id,
        request_status,
        created_at
    ),

    KEY idx_requested_by (
        requested_by,
        created_at
    ),

    CONSTRAINT fk_member_request_conversation
        FOREIGN KEY (conversation_id)
        REFERENCES conversations(id),

    CONSTRAINT chk_request_status
        CHECK (request_status IN (1, 2, 3, 4)),

    CONSTRAINT chk_request_resolution
        CHECK (
            (
                request_status = 1
                AND approved_by IS NULL
                AND rejected_by IS NULL
                AND accepted_at IS NULL
                AND rejected_at IS NULL
            )
            OR
            (
                request_status = 2
                AND approved_by IS NOT NULL
                AND rejected_by IS NULL
                AND accepted_at IS NOT NULL
                AND rejected_at IS NULL
            )
            OR
            (
                request_status = 3
                AND approved_by IS NULL
                AND rejected_by IS NOT NULL
                AND accepted_at IS NULL
                AND rejected_at IS NOT NULL
            )
            OR
            request_status = 4
        )
) ENGINE = InnoDB;
```

### Ngăn request pending trùng

Với đúng các trường hiện tại, MySQL không có partial unique index trực tiếp để viết:

```text
UNIQUE(conversation_id, target_user_id)
WHERE request_status = PENDING
```

Do đó service cần:

1. Query request pending hiện tại.
2. Vẫn xử lý duplicate/race condition trong transaction.
3. Có thể dùng unique rộng hơn nếu chấp nhận giới hạn lịch sử.

Không nên dùng:

```sql
UNIQUE (conversation_id, target_user_id)
```

nếu muốn lưu nhiều lần request theo thời gian.

Nếu cần database tự bảo vệ tuyệt đối chỉ một pending request, sau này có thể thêm generated column phục vụ unique index.

### Luồng approve

Trong cùng transaction:

```text
1. SELECT request FOR UPDATE.
2. Kiểm tra request_status = PENDING.
3. Kiểm tra current user là ADMIN.
4. Kiểm tra target user chưa là active member.
5. Insert conversation_member.
6. Update request thành APPROVED.
7. Commit.
```

Ví dụ:

```sql
UPDATE conversation_member_requests
SET request_status = 2,
    approved_by = :adminUserId,
    accepted_at = NOW(3)
WHERE id = :requestId
  AND request_status = 1;
```

### Luồng reject

```sql
UPDATE conversation_member_requests
SET request_status = 3,
    rejected_by = :adminUserId,
    rejected_at = NOW(3)
WHERE id = :requestId
  AND request_status = 1;
```

---

## 4.4 Bảng `messages`

### Các trường

```text
Table messages:
    - id
    - conversation_id
    - message_seq
    - client_message_id
    - sender_id
    - message_type
    - content
    - metadata
    - reply_to_seq
    - created_at
    - edited_at
    - deleted_at
```

### Mục đích

- Lưu toàn bộ tin nhắn bền vững.
- `message_seq` giữ thứ tự trong conversation.
- `client_message_id` chống lưu trùng khi client retry.
- `reply_to_seq` trỏ đến message được reply trong cùng conversation.
- `metadata` chứa dữ liệu mở rộng theo loại message.

### DDL đề xuất

```sql
CREATE TABLE messages (
    id                    BIGINT UNSIGNED NOT NULL,
    conversation_id       BIGINT UNSIGNED NOT NULL,
    message_seq           BIGINT UNSIGNED NOT NULL,

    client_message_id     BINARY(16) NULL,
    sender_id             BIGINT UNSIGNED NULL,

    message_type          TINYINT UNSIGNED NOT NULL,
    content               TEXT NULL,
    metadata              JSON NULL,

    reply_to_seq          BIGINT UNSIGNED NULL,

    created_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    edited_at             DATETIME(3) NULL,
    deleted_at            DATETIME(3) NULL,

    PRIMARY KEY (id),

    UNIQUE KEY uk_conversation_message_seq (
        conversation_id,
        message_seq
    ),

    UNIQUE KEY uk_sender_client_message (
        sender_id,
        client_message_id
    ),

    KEY idx_conversation_created (
        conversation_id,
        created_at,
        id
    ),

    CONSTRAINT fk_message_conversation
        FOREIGN KEY (conversation_id)
        REFERENCES conversations(id),

    CONSTRAINT chk_message_seq
        CHECK (message_seq > 0),

    CONSTRAINT chk_message_type
        CHECK (message_type IN (1, 2, 3, 4, 5, 6)),

    CONSTRAINT chk_reply_seq
        CHECK (
            reply_to_seq IS NULL
            OR reply_to_seq < message_seq
        )
) ENGINE = InnoDB;
```

### Vì sao `client_message_id` cần thiết?

`message_seq` do server cấp sau khi nhận request. Nếu server đã commit nhưng HTTP response bị mất, client không biết sequence đã cấp và sẽ retry.

Client phải giữ nguyên `client_message_id` khi retry:

```text
Lần gửi đầu:
client_message_id = abc

Retry:
client_message_id = abc
```

Constraint:

```sql
UNIQUE (sender_id, client_message_id)
```

ngăn lưu lại cùng một request.

### `client_message_id` có thể nullable không?

Có thể để `NULL` cho:

- SYSTEM message.
- Message do backend tạo.
- Migration dữ liệu cũ.

Với message do người dùng gửi, application nên bắt buộc có giá trị.

### `reply_to_seq`

Ví dụ:

```text
seq 100: "Hôm nay họp lúc mấy giờ?"
seq 101: "3 giờ nhé"
```

Message 101:

```text
message_seq  = 101
reply_to_seq = 100
```

Khi query message gốc:

```sql
SELECT *
FROM messages
WHERE conversation_id = :conversationId
  AND message_seq = :replyToSeq;
```

Không được query chỉ bằng `message_seq`, vì sequence chỉ unique trong một conversation.

---

# 5. Luồng cấp `message_seq`

## 5.1 Nguyên tắc

Không dùng:

```sql
SELECT MAX(message_seq) + 1
FROM messages
WHERE conversation_id = ?;
```

Không dùng `SELECT last_message_seq` thông thường rồi cộng một ở application.

Hai request đồng thời có thể cùng đọc một giá trị.

Cần:

```text
Transaction
+
row lock hoặc atomic UPDATE
+
UNIQUE(conversation_id, message_seq)
```

## 5.2 Cách dễ hiểu với `SELECT ... FOR UPDATE`

```sql
START TRANSACTION;

SELECT last_message_seq
FROM conversations
WHERE id = :conversationId
FOR UPDATE;
```

Backend tính:

```text
newMessageSeq = last_message_seq + 1
```

Insert message:

```sql
INSERT INTO messages (
    id,
    conversation_id,
    message_seq,
    client_message_id,
    sender_id,
    message_type,
    content,
    metadata,
    reply_to_seq,
    created_at
)
VALUES (
    :messageId,
    :conversationId,
    :newMessageSeq,
    :clientMessageId,
    :senderId,
    :messageType,
    :content,
    :metadata,
    :replyToSeq,
    :createdAt
);
```

Cập nhật conversation:

```sql
UPDATE conversations
SET last_message_seq = :newMessageSeq,
    last_message_id = :messageId,
    last_message_at = :createdAt
WHERE id = :conversationId;
```

Sau đó commit:

```sql
COMMIT;
```

Nếu bất kỳ bước nào lỗi:

```sql
ROLLBACK;
```

## 5.3 Hai người cùng gửi message đầu tiên

Conversation đã tồn tại:

```text
last_message_seq = 0
```

Hai request A và B cùng gửi:

```text
A lock row conversation
A nhận sequence 1

B muốn lock cùng row
B phải chờ

A commit
B đọc giá trị mới là 1
B nhận sequence 2
```

Kết quả:

```text
Message A → seq 1
Message B → seq 2
```

---

# 6. Luồng gửi message trong Spring

Transaction boundary nên đặt ở service, không viết toàn bộ transaction trong một `@Query`.

```java
@Service
@RequiredArgsConstructor
public class MessageService {

    private final ConversationRepository conversationRepository;
    private final ConversationMemberRepository memberRepository;
    private final MessageRepository messageRepository;

    @Transactional
    public SendMessageResponse sendMessage(
            SendMessageCommand command
    ) {
        ConversationMember member =
                memberRepository.findActiveMember(
                        command.conversationId(),
                        command.senderId()
                ).orElseThrow(NotConversationMemberException::new);

        Message existing =
                messageRepository.findBySenderAndClientMessageId(
                        command.senderId(),
                        command.clientMessageId()
                ).orElse(null);

        if (existing != null) {
            return SendMessageResponse.from(existing);
        }

        Conversation conversation =
                conversationRepository.findByIdForUpdate(
                        command.conversationId()
                ).orElseThrow(ConversationNotFoundException::new);

        long newMessageSeq =
                conversation.getLastMessageSeq() + 1;

        Message message = Message.create(
                generateMessageId(),
                command.conversationId(),
                newMessageSeq,
                command.clientMessageId(),
                command.senderId(),
                command.messageType(),
                command.content(),
                command.metadata(),
                command.replyToSeq()
        );

        messageRepository.save(message);

        conversation.setLastMessageSeq(newMessageSeq);
        conversation.setLastMessageId(message.getId());
        conversation.setLastMessageAt(message.getCreatedAt());

        return SendMessageResponse.from(message);
    }
}
```

Repository lock:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query(
    "SELECT c FROM Conversation c WHERE c.id = :conversationId"
)
Optional<Conversation> findByIdForUpdate(
        @Param("conversationId") Long conversationId
);
```

Vẫn bắt buộc giữ unique constraint trong database:

```text
UNIQUE(conversation_id, message_seq)
UNIQUE(sender_id, client_message_id)
```

---

# 7. Kafka và cơ chế publish

Với đúng bốn bảng hiện tại, chưa có bảng Transactional Outbox.

Do đó có hai lựa chọn.

## Phương án đơn giản

```text
1. Commit message vào MySQL.
2. Sau commit, publish Kafka.
3. Nếu publish thất bại, retry bằng application/job.
```

Nhược điểm:

```text
DB commit thành công
Kafka publish thất bại
→ realtime bị miss
```

Client vẫn có thể lấy lại bằng Sync API, nhưng realtime không được bảo đảm.

## Phương án production an toàn hơn

Sau này nên thêm bảng `outbox_events`.

Vì bạn yêu cầu giữ đúng bốn bảng, tài liệu này không thêm bảng outbox vào DDL. Tuy nhiên đây là giới hạn cần ghi nhận rõ.

---

# 8. Kafka topic

Topic chính:

```text
chat.message.created
```

Producer:

```text
key   = conversationId
value = MessageCreatedEvent
```

Ví dụ:

```json
{
  "eventId": "event-uuid",
  "messageId": 9001,
  "conversationId": 100,
  "messageSeq": 51,
  "senderId": 10,
  "createdAt": "2026-07-23T15:00:00Z"
}
```

Cùng `conversationId`:

```text
→ cùng Kafka key
→ cùng partition
→ giữ được thứ tự trong partition
```

Kafka Listener lắng nghe topic chung:

```java
@KafkaListener(
    topics = "chat.message.created",
    groupId = "chat-delivery-group"
)
public void consume(
        ConsumerRecord<Long, MessageCreatedEvent> record
) {
    long conversationId = record.key();
    MessageCreatedEvent event = record.value();

    deliveryService.deliver(event);
}
```

Không tạo topic theo user hoặc theo conversation.

---

# 9. Online presence

Không lưu online status trong MySQL.

Redis key:

```text
presence:{userId}:{sessionId}
```

Value:

```json
{
  "gatewayId": "ws-gateway-03",
  "deviceId": "device-123",
  "appState": "FOREGROUND",
  "lastSeenAt": 1784800000000
}
```

Đề xuất:

```text
Ping interval: 30–60 giây
TTL: 90–180 giây
Offline: sau 2–3 heartbeat bị miss
```

User được coi là online khi còn ít nhất một session chưa hết TTL.

---

# 10. Phân phối tin nhắn

```text
Kafka Listener
  │
  ▼
Lấy danh sách recipient
  │
  ▼
Tra Redis Presence
  ├── online
  │     └── gửi WebSocket
  │           ├── ACK → delivered
  │           └── timeout → push fallback
  │
  └── offline
        └── gửi push
```

Không nên coi `socket.write()` là delivered.

Các trạng thái nên hiểu:

```text
SENT
→ message đã lưu vào MySQL

DELIVERED
→ client đã ACK

READ
→ user đã cập nhật last_read_seq
```

---

# 11. Offline sync

Client lưu:

```text
lastReceivedSeq
lastReadSeq
```

Khi reconnect:

```http
GET /conversations/{conversationId}/messages
    ?afterSeq=100
    &limit=100
```

SQL:

```sql
SELECT *
FROM messages
WHERE conversation_id = :conversationId
  AND message_seq > :afterSeq
  AND message_seq >= :joinedSeq
ORDER BY message_seq ASC
LIMIT :limit;
```

Nếu client đang có seq `100` nhưng WebSocket nhận `103`:

```text
Phát hiện gap 101–102
→ gọi Sync API
→ merge theo message_seq
```

Push notification không phải source of truth.

---

# 12. Lưu message ở client

## Web

Dùng:

```text
IndexedDB
```

## Mobile

Dùng:

```text
SQLite / Room / Core Data
```

Client tạo `client_message_id` trước khi gửi.

```text
PENDING
→ SENDING
→ SENT
→ DELIVERED
→ READ
```

Nếu timeout:

```text
Giữ nguyên client_message_id
→ retry cùng ID
```

Nếu app restart:

```text
Đọc pending messages từ local DB
→ gửi lại cùng client_message_id
```

---

# 13. Các constraint quan trọng nhất

```text
conversation_member:
UNIQUE(conversation_id, user_id)

messages:
UNIQUE(conversation_id, message_seq)
UNIQUE(sender_id, client_message_id)
```

Các constraint này là lớp bảo vệ cuối cùng, kể cả khi application có bug hoặc có hai request đồng thời.

---

# 14. Các giới hạn của mô hình bốn bảng

Do chỉ giữ đúng bốn bảng và đúng các trường đã chọn, hệ thống có các giới hạn:

1. Không có Transactional Outbox.
2. Không có database constraint bảo đảm duy nhất một direct conversation cho một cặp user.
3. Không có partial unique index bảo đảm chỉ một pending member request.
4. Attachment chỉ có thể lưu trong `metadata`; file thật phải ở object storage.
5. Không có bảng delivery/read receipt theo từng message.
6. Không có read model riêng cho danh sách conversation của user.

Các giới hạn trên chấp nhận được cho phiên bản đầu, nhưng cần được theo dõi khi hệ thống lớn lên.

---

# 15. Thiết kế cuối cùng

```text
MySQL
├── conversations
├── conversation_member
├── conversation_member_requests
└── messages

Redis
└── online presence và WebSocket session

Kafka
└── chat.message.created

WebSocket Gateway
└── realtime delivery

Push Worker
└── FCM/APNs

Object Storage
└── file, ảnh, video
```

Nguyên tắc cốt lõi:

```text
Message source of truth
→ MySQL

Message ordering
→ message_seq do server cấp dưới row lock

Retry idempotency
→ client_message_id + unique constraint

Kafka ordering
→ key = conversationId

Offline recovery
→ Sync API đọc MySQL

Presence
→ Redis session với TTL

Read state
→ last_read_seq

Delivered state
→ last_delivered_seq
```
