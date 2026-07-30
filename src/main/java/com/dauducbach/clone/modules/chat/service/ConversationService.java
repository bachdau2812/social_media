package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.service.UserAccountQueryService;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.constant.SystemMessageAction;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.request.CreateDirectConversationRequest;
import com.dauducbach.clone.modules.chat.dto.request.CreateGroupConversationRequest;
import com.dauducbach.clone.modules.chat.dto.response.ConversationResponse;
import com.dauducbach.clone.modules.chat.dto.response.CursorPageResponse;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;

import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ConversationService {
    private static final Logger log = LoggerFactory.getLogger(ConversationService.class);

    static final int DEFAULT_PAGE_SIZE = 50;
    static final int MAX_PAGE_SIZE = 100;

    ConversationRepository conversationRepository;
    ConversationMemberRepository memberRepository;
    UserAccountQueryService userAccountQueryService;

    ChatReadRepository chatReadRepository;
    ChatAccessService accessService;
    ChatResponseMapper mapper;
    ChatSystemMessageService systemMessageService;
    ChatEventPublisher eventPublisher;
    TransactionalOperator transactionalOperator;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<ConversationResponse> createDirect(String actorId, CreateDirectConversationRequest request) {
        if (request == null) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Direct conversation request is required"));
        }

        String actor = requireIdentifier(actorId, "actorId");
        String target = requireIdentifier(request.targetUserId(), "targetUserId");
        if (actor.equals(target)) {
            return Mono.error(new AppException(
                    ErrorCode.DIRECT_CONVERSATION_SELF_NOT_ALLOWED,
                    "A direct conversation requires a different target user"));
        }

        String directKey = directKey(actor, target);
        Mono<Conversation> databaseWork = ensureUserExists(target)
                .then(Mono.defer(() -> findOrCreateDirect(actor, target, directKey)));

        return transactionalOperator.transactional(databaseWork)
                .flatMap(conversation -> toResponse(conversation, actor))
                .onErrorMap(error -> mapError(error, ErrorCode.CONVERSATION_CREATE_FAILED,
                        "Create direct conversation failed"));
    }

    public Mono<ConversationResponse> createGroup(String actorId, CreateGroupConversationRequest request) {
        if (request == null) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Group conversation request is required"));
        }

        String actor = requireIdentifier(actorId, "actorId");
        String title = requireIdentifier(request.title(), "title");
        List<String> targetUserIds = normalizeTargets(actor, request.initialUserIds());
        if (targetUserIds.isEmpty()) {
            return Mono.error(new AppException(
                    ErrorCode.CHAT_REQUEST_INVALID,
                    "A group conversation requires at least one distinct target user"));
        }

        Mono<GroupCreationOutcome> databaseWork = ensureUsersExist(targetUserIds)
                .then(Mono.defer(() -> createGroupRows(actor, title, targetUserIds)))
                .flatMap(conversation -> systemMessageService.insert(
                                conversation.getId(), actor, SystemMessageAction.GROUP_CREATED, null, null)
                        .map(system -> new GroupCreationOutcome(conversation, system)));

        return transactionalOperator.transactional(databaseWork)
                .flatMap(outcome -> publishGroupCreation(outcome, actor, targetUserIds)
                        .then(toResponse(outcome.conversation(), actor)))
                .onErrorMap(error -> mapError(error, ErrorCode.CONVERSATION_CREATE_FAILED,
                        "Create group conversation failed"));
    }

    public Mono<ConversationResponse> getConversation(String actorId, String conversationId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");

        return accessService.requireActiveMember(id, actor)
                .then(conversationRepository.findById(id)
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.CONVERSATION_NOT_FOUND,
                                "Chat conversation was not found"))))
                .flatMap(conversation -> toResponse(conversation, actor))
                .onErrorMap(error -> mapError(error, ErrorCode.CONVERSATION_FETCH_FAILED,
                        "Fetch conversation failed"));
    }

    public Mono<CursorPageResponse<ConversationResponse>> getConversations(String actorId, String cursor, int limit) {
        String actor = requireIdentifier(actorId, "actorId");
        ConversationCursor decodedCursor;
        try {
            decodedCursor = decodeCursor(cursor);
        } catch (AppException error) {
            return Mono.error(error);
        }

        int pageSize = normalizeLimit(limit);
        return chatReadRepository.findConversations(
                        actor, decodedCursor.sortAt(), decodedCursor.conversationId(), pageSize + 1)
                .collectList()
                .map(rows -> toCursorPage(rows, pageSize))
                .onErrorMap(error -> mapError(error, ErrorCode.CONVERSATION_FETCH_FAILED,
                        "Fetch chat conversations failed"));
    }

    private Mono<Conversation> findOrCreateDirect(String actorId, String targetUserId, String directKey) {
        return conversationRepository.findByDirectKey(directKey)
                .switchIfEmpty(Mono.defer(() -> createDirectRows(actorId, targetUserId, directKey)
                        .onErrorResume(DataIntegrityViolationException.class, error ->
                                conversationRepository.findByDirectKey(directKey)
                                        .switchIfEmpty(Mono.error(error)))));
    }

    private Mono<Conversation> createDirectRows(String actorId, String targetUserId, String directKey) {
        Instant now = Instant.now();
        Conversation conversation = Conversation.builder()
                .id(UUID.randomUUID().toString())
                .conversationType(ConversationType.DIRECT)
                .directKey(directKey)
                .createdBy(actorId)
                .createdAt(now)
                .updatedAt(now)
                .build();

        return r2dbcEntityTemplate.insert(Conversation.class).using(conversation)
                .flatMap(saved -> Flux.concat(
                                r2dbcEntityTemplate.insert(ConversationMember.class).using(newMember(saved.getId(), actorId, MemberRole.USER, now)),
                                r2dbcEntityTemplate.insert(ConversationMember.class).using(newMember(saved.getId(), targetUserId, MemberRole.USER, now)))
                        .then(Mono.just(saved)));
    }

    private Mono<Conversation> createGroupRows(String actorId, String title, List<String> targetUserIds) {
        Instant now = Instant.now();
        Conversation conversation = Conversation.builder()
                .id(UUID.randomUUID().toString())
                .conversationType(ConversationType.GROUP)
                .title(title)
                .createdBy(actorId)
                .createdAt(now)
                .updatedAt(now)
                .build();

        return r2dbcEntityTemplate.insert(Conversation.class).using(conversation)
                .flatMap(saved -> Flux.concat(
                                r2dbcEntityTemplate.insert(ConversationMember.class).using(newMember(saved.getId(), actorId, MemberRole.ADMIN, now)),
                                Flux.fromIterable(targetUserIds)
                                        .concatMap(userId -> r2dbcEntityTemplate.insert(ConversationMember.class).using(
                                                newMember(saved.getId(), userId, MemberRole.USER, now))))
                        .then(Mono.just(saved)));
    }

    private ConversationMember newMember(String conversationId, String userId, MemberRole role, Instant now) {
        return ConversationMember.builder()
                .id(UUID.randomUUID().toString())
                .conversationId(conversationId)
                .userId(userId)
                .memberRole(role)
                .memberStatus(MemberStatus.ACTIVE)
                .joinedSeq(1L)
                .lastDeliveredSeq(0L)
                .lastReadSeq(0L)
                .joinedAt(now)
                .build();
    }

    private Mono<Void> publishGroupCreation(
            GroupCreationOutcome outcome,
            String actorId,
            List<String> targetUserIds
    ) {
        Mono<Void> publishSystem = systemMessageService.publish(outcome.system())
                .onErrorResume(error -> {
                    log.error("|ConversationService|publishGroupSystem|failed|conversationId={}|error={}",
                            outcome.conversation().getId(), error.getMessage());
                    return Mono.empty();
                });
        Mono<Void> publishMembership = eventPublisher.publish(ChatEvent.groupCreated(
                        outcome.conversation().getId(), actorId, targetUserIds))
                .onErrorResume(error -> {
                    log.error("|ConversationService|publishGroupMembership|failed|conversationId={}|error={}",
                            outcome.conversation().getId(), error.getMessage());
                    return Mono.empty();
                });
        return Mono.when(publishSystem, publishMembership);
    }
    private Mono<ConversationResponse> toResponse(Conversation conversation, String actorId) {
        return memberRepository.findActive(conversation.getId(), actorId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CONVERSATION_FETCH_FAILED,
                        "Conversation membership was not found")))
                .then(chatReadRepository.findConversation(actorId, conversation.getId()))
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CONVERSATION_FETCH_FAILED,
                        "Conversation presentation was not found")))
                .map(mapper::toConversationResponse);
    }
    private Mono<Void> ensureUsersExist(List<String> userIds) {
        return Flux.fromIterable(userIds)
                .concatMap(this::ensureUserExists)
                .then();
    }

    private Mono<Void> ensureUserExists(String userId) {
        return userAccountQueryService.exists(userId)
                .flatMap(exists -> Boolean.TRUE.equals(exists)
                        ? Mono.<Void>empty()
                        : Mono.error(new AppException(ErrorCode.USER_NOT_FOUND, "Target user was not found")));
    }

    private List<String> normalizeTargets(String actorId, List<String> initialUserIds) {
        if (initialUserIds == null) {
            return List.of();
        }

        Set<String> distinctTargets = new LinkedHashSet<>();
        for (String initialUserId : initialUserIds) {
            if (initialUserId == null) {
                continue;
            }
            String userId = initialUserId.trim();
            if (!userId.isEmpty() && !actorId.equals(userId)) {
                distinctTargets.add(userId);
            }
        }
        return List.copyOf(distinctTargets);
    }

    private CursorPageResponse<ConversationResponse> toCursorPage(
            List<ChatReadRepository.ConversationListRow> rows, int pageSize) {
        List<ChatReadRepository.ConversationListRow> pageRows = new ArrayList<>(rows);
        boolean hasMore = pageRows.size() > pageSize;
        if (hasMore) {
            pageRows.remove(pageRows.size() - 1);
        }

        List<ConversationResponse> items = pageRows.stream()
                .map(mapper::toConversationResponse)
                .toList();
        String nextCursor = hasMore ? encodeCursor(pageRows.getLast()) : null;
        return new CursorPageResponse<>(items, nextCursor, hasMore);
    }

    private int normalizeLimit(int requestedLimit) {
        if (requestedLimit <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(requestedLimit, MAX_PAGE_SIZE);
    }

    private ConversationCursor decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return new ConversationCursor(null, null);
        }
        try {
            String decoded = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            int separator = decoded.indexOf('|');
            if (separator <= 0 || separator == decoded.length() - 1) {
                throw new IllegalArgumentException("Missing cursor fields");
            }
            return new ConversationCursor(
                    Instant.ofEpochMilli(Long.parseLong(decoded.substring(0, separator))),
                    decoded.substring(separator + 1));
        } catch (IllegalArgumentException error) {
            throw new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Conversation cursor is invalid", error);
        }
    }

    private String encodeCursor(ChatReadRepository.ConversationListRow row) {
        String value = row.sortAt().toEpochMilli() + "|" + row.id();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String requireIdentifier(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.CHAT_REQUEST_INVALID, name + " is required");
        }
        return value.trim();
    }

    private String directKey(String firstUserId, String secondUserId) {
        String normalized = List.of(firstUserId, secondUserId).stream()
                .sorted(Comparator.naturalOrder())
                .reduce((first, second) -> first + ":" + second)
                .orElseThrow();
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(normalized.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private Throwable mapError(Throwable error, ErrorCode errorCode, String message) {
        return error instanceof AppException
                ? error
                : new AppException(errorCode, message, error);
    }

    private record ConversationCursor(Instant sortAt, String conversationId) {
    }

    private record GroupCreationOutcome(
            Conversation conversation,
            ChatSystemMessageService.SystemMessageResult system
    ) {
    }
}
