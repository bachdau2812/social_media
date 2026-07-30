# Chat Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the chat module in `social_media` with persistent conversations, members, member requests, messages, sync cursors, and Kafka message-created events.

**Architecture:** Use Spring WebFlux plus Spring Data R2DBC repositories and `TransactionalOperator`. The database is the message source of truth; Kafka is a post-write delivery signal keyed by conversation id.

**Tech Stack:** Java 21, Spring Boot 3.5.6, WebFlux, R2DBC MySQL, Reactor, Reactor Kafka, Gson, Jsoup, JUnit 5, Mockito, Reactor Test.

## Global Constraints

- Keep code under `src/main/java/com/dauducbach/clone/modules/chat`.
- Keep request/user identity style consistent with this project: pass actor/user ids in request/query values.
- Do not modify auth/config files unless compilation requires it.
- Use `direct_key` and `pending_key` as confirmed by the user.
- Use TDD: write or port failing tests before production implementation where practical.

---

### Task 1: Chat Contracts And Schema

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/commons/exception/ErrorCode.java`
- Create: `src/main/java/com/dauducbach/clone/modules/chat/constant/*.java`
- Create: `src/main/resources/db/manual/chat_schema.sql`
- Test: `src/test/java/com/dauducbach/clone/modules/chat/ChatPersistenceContractTest.java`

**Interfaces:**
- Produces enum constants: `ConversationType`, `MemberRole`, `MemberStatus`, `MemberRequestStatus`, `MessageType`, `ChatEventType`.
- Produces error codes `1300` through `1343` for chat behavior.

- [ ] Write the contract test for enum values and chat error codes.
- [ ] Run `.\mvnw.cmd -Dtest=ChatPersistenceContractTest test`; expected failure because chat constants/error codes are missing.
- [ ] Add constants, error codes, and SQL schema.
- [ ] Run the same test; expected pass.

### Task 2: Persistence Entities And Repositories

**Files:**
- Replace/create chat entities under `src/main/java/com/dauducbach/clone/modules/chat/entity`
- Replace/create repositories under `src/main/java/com/dauducbach/clone/modules/chat/repository`
- Test: `src/test/java/com/dauducbach/clone/modules/chat/repository/*Test.java`

**Interfaces:**
- Produces R2DBC entities `Conversation`, `ConversationMember`, `ConversationMemberRequest`, `ChatMessage`.
- Produces repository methods for active membership, conversation row locking, message retry lookup, sequence history, and cursor advancement.

- [ ] Port repository tests for SQL and entity mapping.
- [ ] Run repository tests; expected failure due missing entities/repositories.
- [ ] Implement entities and repository methods.
- [ ] Run repository tests; expected pass or skip external DB integration if the project test profile lacks a database.

### Task 3: Validation, Mapping, And Access Services

**Files:**
- Create: `ChatMessageValidator.java`, `ChatResponseMapper.java`, `ChatAccessService.java`
- Create DTO request/response records under `modules/chat/dto`
- Test: matching service tests.

**Interfaces:**
- Produces sanitized message content and serialized media metadata.
- Produces conversation/message response DTOs.
- Produces `requireActiveMember` and `requireAdmin`.

- [ ] Write/port tests for HTML sanitization, UUID validation, media metadata checks, unread counts, and access errors.
- [ ] Run targeted tests; expected failure.
- [ ] Implement validators, mapper, DTOs, and access service.
- [ ] Run targeted tests; expected pass.

### Task 4: Conversation And Member Services

**Files:**
- Create: `ConversationService.java`
- Create: `ConversationMemberService.java`
- Test: `ConversationServiceTest.java`, `ConversationMemberServiceTest.java`

**Interfaces:**
- Produces create/get/list conversation flows.
- Produces admin direct-add and non-admin pending request flows.
- Produces approve/reject member request flows.

- [ ] Write/port tests for direct idempotency, group creator admin role, active membership checks, pending request duplication, and admin approval.
- [ ] Run targeted tests; expected failure.
- [ ] Implement services with `TransactionalOperator`.
- [ ] Run targeted tests; expected pass.

### Task 5: Message Service, Cursor Service, And Kafka Publisher

**Files:**
- Replace: `SendMessageService.java` with a complete message service or keep name with complete behavior.
- Create: `ChatCursorService.java`
- Create: `KafkaChatEventPublisher.java`
- Test: message/cursor/event tests.

**Interfaces:**
- Produces send-message flow with row lock, idempotency, reply validation, sequence assignment, and post-transaction Kafka publish.
- Produces message sync and cursor advancement.

- [ ] Write tests for non-member rejection, retry returning existing message, sequence increment, invalid reply rejection, cursor monotonic update, and Kafka event payload.
- [ ] Run targeted tests; expected failure.
- [ ] Implement message, cursor, and event publisher services.
- [ ] Run targeted tests; expected pass.

### Task 6: HTTP Controller And Final Verification

**Files:**
- Create: `src/main/java/com/dauducbach/clone/modules/chat/controller/ChatController.java`
- Test: `src/test/java/com/dauducbach/clone/modules/chat/controller/ChatControllerTest.java`

**Interfaces:**
- Produces REST endpoints under `/chat`.

- [ ] Write controller tests for response wrapping and routing.
- [ ] Run controller tests; expected failure.
- [ ] Implement controller.
- [ ] Run controller tests; expected pass.
- [ ] Run `.\mvnw.cmd test` from `social_media`; record result and any remaining failures.
