# Chat Reply and Media Messages Design

## Scope

Implement message grouping, rich reply previews, image messages, and recorded voice messages across the full chat page and floating chat. Keep existing conversation, Kafka, WebSocket, delivery/read cursor, and Cloudinary signature flows.

## Message grouping

Consecutive incoming messages from the same sender form one visual group. In group conversations, the sender display name appears above the first message. The sender avatar appears beside the last message in the group. Outgoing messages remain right aligned without an avatar.

## Reply data and interaction

`messages.reply_to_seq` remains the persisted relationship. `ChatMessageResponse` adds a nested reply preview containing the original sequence, sender identity, message type, textual preview, media metadata, and deleted state. Chat history queries self-join the original message and its sender profile. The send response uses the same enriched query, so realtime events contain the preview immediately.

The reply preview is visually joined to the new message bubble. Selecting it locates the original message, scrolls it into view, and applies a temporary highlight. Missing or deleted originals render `Tin nhắn gốc không còn tồn tại`. The composer displays a compact reply bar with a cancel action.

## Media messages

The frontend requests the existing signed Cloudinary upload and uploads one selected image or one recorded audio blob. It sends `IMAGE` or `AUDIO` with authoritative upload identifiers and local file metadata. Backend fetches the Cloudinary resource by `publicId`, builds normalized message metadata, inserts the message, and inserts the resource into `media` with owner type `CHAT_MESSAGE`. No NSFW scan is performed.

Image messages use a constrained rounded preview and support opening the source asset. Audio messages use a compact native audio player. Voice recording uses the browser `MediaRecorder` API, with a five-minute and 50 MB limit, preview, cancel, upload, and error states.

## Failure handling

Invalid MIME types, absent Cloudinary identifiers, files over 50 MB, recordings over five minutes, and Cloudinary lookup failures reject the message. Optimistic media messages show uploading/sending/failed states. Existing text-message behavior remains unchanged.

## Verification

Build the backend with Maven tests skipped and build the frontend with TypeScript and Vite. Manually inspect generated types and SQL/schema updates; no comprehensive UI test is required.
