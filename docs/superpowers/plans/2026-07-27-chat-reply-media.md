# Chat Reply and Media Messages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add grouped-message avatar placement, rich replies, image messages, and recorded voice messages to full and floating chat.

**Architecture:** Preserve `messages.reply_to_seq` and JSON metadata. Enrich chat query rows with a self-joined reply preview, persist Cloudinary assets through the existing media subsystem with `OwnerType.CHAT_MESSAGE`, and use shared frontend render/upload helpers across both chat surfaces.

**Tech Stack:** Spring WebFlux, R2DBC, MySQL, Cloudinary, React 18, TypeScript, Vite, browser MediaRecorder.

## Global Constraints

- Image and audio messages contain one attachment.
- Maximum upload size is 50 MB.
- Maximum recorded voice duration is five minutes.
- Chat media is not sent through NSFW scanning.
- Existing Kafka, WebSocket, and cursor behavior remains unchanged.

---

### Task 1: Enrich backend reply responses

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/entity/ChatMessage.java`
- Create: `src/main/java/com/dauducbach/clone/modules/chat/dto/response/ReplyMessageResponse.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/dto/response/ChatMessageResponse.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/repository/ChatReadRepository.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/service/ChatResponseMapper.java`

- [x] Add transient original-message fields to query rows.
- [x] Self-join reply message and sender identity in forward/backward message queries.
- [x] Map a compact nested reply preview into every chat response.
- [x] Compile backend.

### Task 2: Persist Cloudinary chat media

**Files:**
- Modify: `src/main/java/com/dauducbach/clone/modules/post/constant/OwnerType.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/service/ChatMessageValidator.java`
- Modify: `src/main/java/com/dauducbach/clone/modules/chat/service/SendMessageService.java`
- Modify: `src/main/resources/db/manual/chat_schema.sql`

- [x] Add `CHAT_MESSAGE` owner type and schema enum migration guidance.
- [x] Enforce image/audio MIME and 50 MB constraints.
- [x] Fetch Cloudinary metadata before transactional inserts.
- [x] Insert message and `media` row together without NSFW scanning.
- [x] Compile backend.

### Task 3: Add shared frontend reply and media rendering

**Files:**
- Modify: `social_media_FE/src/App.tsx`
- Modify: `social_media_FE/src/api.ts`
- Modify: `social_media_FE/src/styles.css`

- [x] Extend message/upload types with reply and media metadata.
- [x] Render linked reply clusters, image previews, and audio players.
- [x] Scroll and highlight original messages from reply previews.
- [x] Place group sender name on the first bubble and avatar on the last bubble.

### Task 4: Add image and voice composition

**Files:**
- Modify: `social_media_FE/src/App.tsx`
- Modify: `social_media_FE/src/styles.css`

- [x] Add one-image selection and upload states to full and floating composers.
- [x] Add MediaRecorder recording, five-minute stop, preview, cancel, and upload.
- [x] Send IMAGE/AUDIO requests with reply and recipient fields.
- [x] Keep optimistic and realtime synchronization behavior.

### Task 5: Verify and document

**Files:**
- Modify: `docs/frontend-backend-change-summary.md`

- [x] Run `./mvnw.cmd -Dmaven.test.skip=true package`.
- [x] Run `npm run build` in `social_media_FE`.
- [x] Append API, schema, and frontend behavior changes to the summary.
