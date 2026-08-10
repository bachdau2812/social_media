# Central Media Policy and Image-Only Scan Design

## Goal

Make media-size policy configurable from one backend source and temporarily restrict content moderation scans to images. Videos must not be downloaded by MediaScanUtils and must not be sent to the external scan API.

## Scope

- Backend: Post, Comment, Story, Avatar, and Chat media validation.
- Frontend: Post creation/editing/comments, Story creation, profile media, and both full/mini Chat surfaces through their shared composer.
- Image and video maximum size: 100 MB.
- Chat audio maximum size remains 50 MB.
- Existing upload, persistence, Kafka, SSE, and API response flows remain intact.

## Central Policy

Add MediaPolicyProperties under the media module, enabled through Spring configuration properties.

Configuration keys:

    app.media.limits.image: 100MB
    app.media.limits.video: 100MB
    app.media.limits.audio: 50MB

Each value supports an environment override. The backend remains authoritative. Validators consume DataSize values from this bean instead of hard-coded constants.

Expose an authenticated read-only endpoint at /media/upload-policy returning byte values for image, video, and audio. The frontend loads it once, caches the result in memory, and falls back to the same safe defaults if configuration cannot be loaded. A later YAML/environment change therefore requires only a backend restart, not a frontend rebuild.

## Scan Decision

Introduce one shared media-kind resolver using this precedence:

1. Explicit resourceType or mediaType.
2. MIME type when available.
3. File extension from the delivery URL.
4. Unknown defaults to image scanning.

For images:

1. Validate the Cloudinary metadata size against the configured image limit.
2. Download the image through MediaScanUtils.
3. Reject if downloaded bytes exceed the configured image limit.
4. Call the external scan API and use its decision.

For videos:

1. Do not invoke MediaScanUtils.
2. Do not download video bytes from the Cloudinary delivery URL.
3. Do not call the external scan API.
4. Continue the existing Cloudinary metadata lookup needed to obtain assetId, dimensions, resource type, and canonical URL.
5. Validate metadata size against the configured video limit, then continue existing media persistence and success events.

The skip is explicit and logged with media owner/id and resource type.

## Flow Integration

### Post

PostMediaScanItem.resourceType already exists. PostMediaModerationOrchestrator asks the shared moderation provider for a decision. Video receives an approved-without-scan decision, then follows the existing metadata/persistence path.

### Comment

Add nullable resourceType to MediaUploadRequest and Kafka payload parsing. This is an additive contract change. New clients send it; older payloads use URL inference.

### Story

Add mediaType to the Story scan event. The consumer uses that value before URL inference. Video stories bypass the external scan and continue through the existing approval, media persistence, SSE, and Story success event path.

### Avatar

Avatar remains image-only and continues to use the image scan path and configured image limit.

### Chat

Chat does not use the moderation scanner. ChatMessageValidator consumes centralized image/video/audio limits. Both full Chat and mini-chat keep using their shared composer/controller and therefore receive the same policy behavior.

## Frontend Policy

Create a shared media-policy client/cache:

- getMediaUploadPolicy fetches /media/upload-policy once.
- Safe defaults are image 100 MB, video 100 MB, audio 50 MB.
- A shared validator formats limit messages from returned byte values.
- Post, Story, Comment, profile, and Chat stop declaring local size constants.
- Existing upload payloads remain unchanged except additive resourceType for Comment where needed.

Policy loading must not block application bootstrap. Selection validation uses the cached policy or safe defaults; a successful fetch updates later validations.

## Error Handling

- Unknown media is scanned as an image.
- Missing or invalid policy values fail application startup through configuration validation.
- Policy endpoint failure uses frontend defaults and does not block upload UI.
- Video metadata lookup, size validation, persistence, Kafka, and SSE failures retain existing failure handling.
- No video scan failure can leave a publication pending because no asynchronous external scan call is made for video.

## Tests

- Moderation provider proves image invokes scanner and video does not.
- Post mixed image/video proves only image is scanned and both allowed items persist.
- Comment and Story video tests prove scanner is never called and success paths complete.
- Image tests retain rejection behavior.
- Backend policy tests prove 100 MB boundary acceptance and over-limit rejection.
- Chat validator tests read configured policy for image/video/audio.
- Frontend shared policy tests cover response parsing, cache/fallback, exact boundary, and over-limit behavior.
- Existing Post, Story, Comment, profile, full Chat, and mini-chat scoped tests remain green.

## Non-goals

- No video-frame extraction or video moderation API.
- No Redis-backed hot reload.
- No changes to Cloudinary upload mechanics.
- No change to audio duration limits.
