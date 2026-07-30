package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Repository
@RequiredArgsConstructor
public class ChatReadRepository {

    private final DatabaseClient databaseClient;

    public Flux<ConversationListRow> findConversations(String userId, Instant cursorAt, String cursorId, int limit) {
        String sql = """
                SELECT c.id,
                       c.conversation_type,
                               c.is_dissolved,

                       CASE
                           WHEN c.conversation_type = 'GROUP' THEN c.title
                           ELSE COALESCE(NULLIF(peer_member.nickname, ''),
                                         NULLIF(peer_details.full_name, ''),
                                         NULLIF(peer_details.username, ''),
                                         peer_member.user_id,
                                         'Direct message')
                       END AS title,
                       (SELECT COALESCE(NULLIF(avatar.secure_url, ''), avatar.url)
                        FROM media avatar
                        WHERE avatar.owner_id = peer_member.user_id
                          AND avatar.owner_type = 'AVATAR'
                        ORDER BY avatar.created_at DESC
                        LIMIT 1) AS avatar_url,
                       last_message.sender_id AS last_message_sender_id,
                       last_message.message_type AS last_message_type,
                       last_message.content AS last_message_content,
                       last_message.deleted_at AS last_message_deleted_at,
                       c.last_message_seq,
                       c.last_message_id,
                       c.last_message_at,
                       c.created_at,
                       cm.member_role,
                       cm.last_read_seq,
                       cm.joined_seq,
                               cm.last_deleted_message_seq,

                       COALESCE((SELECT COUNT(*)
                                 FROM messages unread_message
                                 WHERE unread_message.conversation_id = c.id
                                   AND unread_message.message_seq >= cm.joined_seq
                                   AND unread_message.message_seq > cm.last_read_seq
                                           AND unread_message.message_seq > COALESCE(cm.last_deleted_message_seq, 0)

                                   AND unread_message.sender_id <> :userId), 0) AS unread_count,
                       COALESCE((SELECT MIN(receipt_member.last_delivered_seq)
                                 FROM conversation_members receipt_member
                                 WHERE receipt_member.conversation_id = c.id
                                   AND receipt_member.member_status = 'ACTIVE'
                                   AND receipt_member.user_id <> :userId), 0) AS recipient_delivered_seq,
                       COALESCE((SELECT MIN(receipt_member.last_read_seq)
                                 FROM conversation_members receipt_member
                                 WHERE receipt_member.conversation_id = c.id
                                   AND receipt_member.member_status = 'ACTIVE'
                                   AND receipt_member.user_id <> :userId), 0) AS recipient_read_seq,
                       COALESCE(c.last_message_at, c.created_at) AS sort_at
                FROM conversations c
                INNER JOIN conversation_members cm ON cm.conversation_id = c.id
                LEFT JOIN conversation_members peer_member
                  ON c.conversation_type = 'DIRECT'
                 AND peer_member.conversation_id = c.id
                 AND peer_member.user_id <> :userId
                 AND peer_member.member_status = 'ACTIVE'
                LEFT JOIN user_details peer_details ON peer_details.user_id = peer_member.user_id
                LEFT JOIN messages last_message ON last_message.id = c.last_message_id
                WHERE cm.user_id = :userId
                  AND cm.member_status = 'ACTIVE'
                  AND (c.conversation_type = 'GROUP' OR cm.last_deleted_message_seq IS NULL OR c.last_message_seq > cm.last_deleted_message_seq)
                """;

        DatabaseClient.GenericExecuteSpec query;
        if (cursorAt == null) {
            query = databaseClient.sql(sql + " ORDER BY sort_at DESC, c.id DESC LIMIT :limit")
                    .bind("userId", userId)
                    .bind("limit", limit);
        } else {
            query = databaseClient.sql(sql + """
                    AND (COALESCE(c.last_message_at, c.created_at) < :cursorAt
                         OR (COALESCE(c.last_message_at, c.created_at) = :cursorAt AND c.id < :cursorId))
                    ORDER BY sort_at DESC, c.id DESC
                    LIMIT :limit
                    """)
                    .bind("userId", userId)
                    .bind("cursorAt", cursorAt)
                    .bind("cursorId", cursorId)
                    .bind("limit", limit);
        }

        return query.map((row, metadata) -> new ConversationListRow(
                string(row, "id"),
                ConversationType.valueOf(string(row, "conversation_type")),
                booleanValue(row, "is_dissolved"),
                string(row, "title"),
                string(row, "avatar_url"),
                string(row, "last_message_sender_id"),
                messageType(row, "last_message_type"),
                string(row, "last_message_content"),
                instant(row, "last_message_deleted_at"),
                number(row, "last_message_seq"),
                string(row, "last_message_id"),
                instant(row, "last_message_at"),
                MemberRole.valueOf(string(row, "member_role")),
                number(row, "last_read_seq"),
                number(row, "joined_seq"),
                nullableNumber(row, "last_deleted_message_seq"),
                number(row, "unread_count"),
                number(row, "recipient_delivered_seq"),
                number(row, "recipient_read_seq"),
                instant(row, "created_at"),
                instant(row, "sort_at"))).all();
    }

    public Mono<ConversationListRow> findConversation(String userId, String conversationId) {
        return databaseClient.sql("""
                        SELECT c.id,
                               c.conversation_type,
                               c.is_dissolved,

                               CASE
                                   WHEN c.conversation_type = 'GROUP' THEN c.title
                                   ELSE COALESCE(NULLIF(peer_member.nickname, ''),
                                         NULLIF(peer_details.full_name, ''),
                                                 NULLIF(peer_details.username, ''),
                                                 peer_member.user_id,
                                                 'Direct message')
                               END AS title,
                               (SELECT COALESCE(NULLIF(avatar.secure_url, ''), avatar.url)
                                FROM media avatar
                                WHERE avatar.owner_id = peer_member.user_id
                                  AND avatar.owner_type = 'AVATAR'
                                ORDER BY avatar.created_at DESC
                                LIMIT 1) AS avatar_url,
                               last_message.sender_id AS last_message_sender_id,
                               last_message.message_type AS last_message_type,
                               last_message.content AS last_message_content,
                               last_message.deleted_at AS last_message_deleted_at,
                               c.last_message_seq,
                               c.last_message_id,
                               c.last_message_at,
                               c.created_at,
                               cm.member_role,
                               cm.last_read_seq,
                               cm.joined_seq,
                               cm.last_deleted_message_seq,

                               COALESCE((SELECT COUNT(*)
                                         FROM messages unread_message
                                         WHERE unread_message.conversation_id = c.id
                                           AND unread_message.message_seq >= cm.joined_seq
                                           AND unread_message.message_seq > cm.last_read_seq
                                           AND unread_message.message_seq > COALESCE(cm.last_deleted_message_seq, 0)

                                           AND unread_message.sender_id <> :userId), 0) AS unread_count,
                               COALESCE((SELECT MIN(receipt_member.last_delivered_seq)
                                         FROM conversation_members receipt_member
                                         WHERE receipt_member.conversation_id = c.id
                                           AND receipt_member.member_status = 'ACTIVE'
                                           AND receipt_member.user_id <> :userId), 0) AS recipient_delivered_seq,
                               COALESCE((SELECT MIN(receipt_member.last_read_seq)
                                         FROM conversation_members receipt_member
                                         WHERE receipt_member.conversation_id = c.id
                                           AND receipt_member.member_status = 'ACTIVE'
                                           AND receipt_member.user_id <> :userId), 0) AS recipient_read_seq,
                               COALESCE(c.last_message_at, c.created_at) AS sort_at
                        FROM conversations c
                        INNER JOIN conversation_members cm ON cm.conversation_id = c.id
                        LEFT JOIN conversation_members peer_member
                          ON c.conversation_type = 'DIRECT'
                         AND peer_member.conversation_id = c.id
                         AND peer_member.user_id <> :userId
                         AND peer_member.member_status = 'ACTIVE'
                        LEFT JOIN user_details peer_details ON peer_details.user_id = peer_member.user_id
                LEFT JOIN messages last_message ON last_message.id = c.last_message_id
                        WHERE cm.user_id = :userId
                          AND cm.member_status = 'ACTIVE'
                          AND c.id = :conversationId
                        LIMIT 1
                        """)
                .bind("userId", userId)
                .bind("conversationId", conversationId)
                .map((row, metadata) -> new ConversationListRow(
                        string(row, "id"),
                        ConversationType.valueOf(string(row, "conversation_type")),
                booleanValue(row, "is_dissolved"),
                        string(row, "title"),
                        string(row, "avatar_url"),
                        string(row, "last_message_sender_id"),
                        messageType(row, "last_message_type"),
                        string(row, "last_message_content"),
                        instant(row, "last_message_deleted_at"),
                        number(row, "last_message_seq"),
                        string(row, "last_message_id"),
                        instant(row, "last_message_at"),
                        MemberRole.valueOf(string(row, "member_role")),
                        number(row, "last_read_seq"),
                        number(row, "joined_seq"),
                nullableNumber(row, "last_deleted_message_seq"),
                        number(row, "unread_count"),
                        number(row, "recipient_delivered_seq"),
                        number(row, "recipient_read_seq"),
                        instant(row, "created_at"),
                        instant(row, "sort_at")))
                .one();
    }

    public Flux<PendingDeliveryCursor> findPendingDeliveries(String userId) {
        return databaseClient.sql("""
                        SELECT cm.conversation_id, c.last_message_seq
                        FROM conversation_members cm
                        INNER JOIN conversations c ON c.id = cm.conversation_id
                        WHERE cm.user_id = :userId
                          AND cm.member_status = 'ACTIVE'
                  AND (c.conversation_type = 'GROUP' OR cm.last_deleted_message_seq IS NULL OR c.last_message_seq > cm.last_deleted_message_seq)
                          AND c.last_message_seq > GREATEST(cm.last_delivered_seq, COALESCE(cm.last_deleted_message_seq, 0))
                        """)
                .bind("userId", userId)
                .map((row, metadata) -> new PendingDeliveryCursor(
                        string(row, "conversation_id"),
                        number(row, "last_message_seq")))
                .all();
    }
    public Flux<ChatMessage> findAfterSequence(String conversationId, long joinedSeq, long afterSeq, int limit) {
        return databaseClient.sql("""
                        SELECT message.*,
                               COALESCE(NULLIF(sender_member.nickname, ''),
                                        NULLIF(sender_details.full_name, ''),
                                        NULLIF(sender_details.username, ''),
                                        message.sender_id) AS sender_display_name,
                               (SELECT COALESCE(NULLIF(avatar.secure_url, ''), avatar.url)
                                FROM media avatar
                                WHERE avatar.owner_id = message.sender_id
                                  AND avatar.owner_type = 'AVATAR'
                                ORDER BY avatar.created_at DESC
                                LIMIT 1) AS sender_avatar_url,
                               reply_message.message_seq AS reply_message_seq,
                               reply_message.sender_id AS reply_sender_id,
                               COALESCE(NULLIF(reply_sender_member.nickname, ''),
                                        NULLIF(reply_sender_details.full_name, ''),
                                        NULLIF(reply_sender_details.username, ''),
                                        reply_message.sender_id) AS reply_sender_display_name,
                               reply_message.message_type AS reply_message_type,
                               reply_message.content AS reply_content,
                               reply_message.metadata AS reply_metadata,
                               reply_message.deleted_at AS reply_deleted_at
                        FROM messages message
                        LEFT JOIN conversation_members sender_member
                          ON sender_member.conversation_id = message.conversation_id
                         AND sender_member.user_id = message.sender_id
                        LEFT JOIN user_details sender_details ON sender_details.user_id = message.sender_id
                        LEFT JOIN messages reply_message
                          ON reply_message.conversation_id = message.conversation_id
                         AND reply_message.message_seq = message.reply_to_seq
                         AND reply_message.message_seq >= :joinedSeq
                        LEFT JOIN conversation_members reply_sender_member
                          ON reply_sender_member.conversation_id = message.conversation_id
                         AND reply_sender_member.user_id = reply_message.sender_id
                        LEFT JOIN user_details reply_sender_details
                          ON reply_sender_details.user_id = reply_message.sender_id
                        WHERE message.conversation_id = :conversationId
                          AND message.message_seq >= :joinedSeq
                          AND message.message_seq > :afterSeq
                        ORDER BY message.message_seq ASC
                        LIMIT :limit
                        """)
                .bind("conversationId", conversationId)
                .bind("joinedSeq", joinedSeq)
                .bind("afterSeq", afterSeq)
                .bind("limit", limit)
                .map((row, metadata) -> mapMessage(row))
                .all();
    }

    public Flux<ChatMessage> findBeforeSequence(String conversationId, long joinedSeq, long beforeSeq, int limit) {
        return databaseClient.sql("""
                        SELECT * FROM (
                            SELECT message.*,
                                   COALESCE(NULLIF(sender_member.nickname, ''),
                                            NULLIF(sender_details.full_name, ''),
                                            NULLIF(sender_details.username, ''),
                                            message.sender_id) AS sender_display_name,
                                   (SELECT COALESCE(NULLIF(avatar.secure_url, ''), avatar.url)
                                    FROM media avatar
                                    WHERE avatar.owner_id = message.sender_id
                                      AND avatar.owner_type = 'AVATAR'
                                    ORDER BY avatar.created_at DESC
                                    LIMIT 1) AS sender_avatar_url,
                               reply_message.message_seq AS reply_message_seq,
                               reply_message.sender_id AS reply_sender_id,
                               COALESCE(NULLIF(reply_sender_member.nickname, ''),
                                        NULLIF(reply_sender_details.full_name, ''),
                                        NULLIF(reply_sender_details.username, ''),
                                        reply_message.sender_id) AS reply_sender_display_name,
                               reply_message.message_type AS reply_message_type,
                               reply_message.content AS reply_content,
                               reply_message.metadata AS reply_metadata,
                               reply_message.deleted_at AS reply_deleted_at
                            FROM messages message
                            LEFT JOIN conversation_members sender_member
                              ON sender_member.conversation_id = message.conversation_id
                             AND sender_member.user_id = message.sender_id
                            LEFT JOIN user_details sender_details ON sender_details.user_id = message.sender_id
                        LEFT JOIN messages reply_message
                          ON reply_message.conversation_id = message.conversation_id
                         AND reply_message.message_seq = message.reply_to_seq
                         AND reply_message.message_seq >= :joinedSeq
                        LEFT JOIN conversation_members reply_sender_member
                          ON reply_sender_member.conversation_id = message.conversation_id
                         AND reply_sender_member.user_id = reply_message.sender_id
                        LEFT JOIN user_details reply_sender_details
                          ON reply_sender_details.user_id = reply_message.sender_id
                            WHERE message.conversation_id = :conversationId
                              AND message.message_seq >= :joinedSeq
                              AND message.message_seq < :beforeSeq
                            ORDER BY message.message_seq DESC
                            LIMIT :limit
                        ) AS message_page
                        ORDER BY message_seq ASC
                        """)
                .bind("conversationId", conversationId)
                .bind("joinedSeq", joinedSeq)
                .bind("beforeSeq", beforeSeq)
                .bind("limit", limit)
                .map((row, metadata) -> mapMessage(row))
                .all();
    }

    private ChatMessage mapMessage(Row row) {
        return ChatMessage.builder()
                .id(string(row, "id"))
                .conversationId(string(row, "conversation_id"))
                .messageSeq(number(row, "message_seq"))
                .clientMessageId(string(row, "client_message_id"))
                .senderId(string(row, "sender_id"))
                .senderDisplayName(string(row, "sender_display_name"))
                .senderAvatarUrl(string(row, "sender_avatar_url"))
                .messageType(MessageType.valueOf(string(row, "message_type")))
                .content(string(row, "content"))
                .metadata(string(row, "metadata"))
                .replyToSeq(nullableNumber(row, "reply_to_seq"))
                .replyMessageSeq(nullableNumber(row, "reply_message_seq"))
                .replySenderId(string(row, "reply_sender_id"))
                .replySenderDisplayName(string(row, "reply_sender_display_name"))
                .replyMessageType(messageType(row, "reply_message_type"))
                .replyContent(string(row, "reply_content"))
                .replyMetadata(string(row, "reply_metadata"))
                .replyDeletedAt(instant(row, "reply_deleted_at"))
                .createdAt(instant(row, "created_at"))
                .editedAt(instant(row, "edited_at"))
                .deletedAt(instant(row, "deleted_at"))
                .build();
    }

    private String string(Row row, String column) {
        return row.get(column, String.class);
    }

    private MessageType messageType(Row row, String column) {
        String value = string(row, column);
        return value == null || value.isBlank() ? null : MessageType.valueOf(value);
    }
    private long number(Row row, String column) {
        return ((Number) row.get(column)).longValue();
    }

    private boolean booleanValue(Row row, String column) {
        Object value = row.get(column);
        if (value instanceof Boolean bool) return bool;
        return value instanceof Number number && number.intValue() != 0;
    }

    private Long nullableNumber(Row row, String column) {
        Number value = (Number) row.get(column);
        return value == null ? null : value.longValue();
    }

    private Instant instant(Row row, String column) {
        return toInstant(row.get(column));
    }

    static Instant toInstant(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof LocalDateTime dateTime) {
            return dateTime.atZone(ZoneId.systemDefault()).toInstant();
        }
        throw new IllegalStateException("Unsupported timestamp type: " + value.getClass().getName());
    }

    public record ConversationListRow(
            String id,
            ConversationType conversationType,
            boolean dissolved,
            String title,
            String avatarUrl,
            String lastMessageSenderId,
            MessageType lastMessageType,
            String lastMessageContent,
            Instant lastMessageDeletedAt,
            long lastMessageSeq,
            String lastMessageId,
            Instant lastMessageAt,
            MemberRole currentUserRole,
            long lastReadSeq,
            long joinedSeq,
            Long lastDeletedMessageSeq,
            long unreadCount,
            long recipientDeliveredSeq,
            long recipientReadSeq,
            Instant createdAt,
            Instant sortAt) {
    }

    public record PendingDeliveryCursor(String conversationId, long sequence) {
    }
}
