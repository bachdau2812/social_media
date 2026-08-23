# Chat Audio Nullable Dimensions Design

## Problem

Cloudinary stores voice recordings as `resourceType=video` and may return `width=0` and `height=0`. The chat request correctly leaves audio dimensions as `null`. `SendMessageService.normalizedMetadata` currently combines primitive media dimensions with nullable request dimensions in a conditional expression, which makes Java unbox the nullable branch and throws `NullPointerException` before the chat message is created.

## Scope

Fix only backend chat media normalization. Do not change the request contract, database schema, Cloudinary upload flow, or frontend payload. Audio metadata continues to allow `width` and `height` to be absent.

## Design

Normalize each dimension without mixing primitive and nullable values in one conditional expression:

- Prefer the Cloudinary dimension when it is greater than zero.
- Otherwise preserve the nullable value supplied by the request.
- Keep existing URL, byte-size, MIME type, filename, duration, persistence, and event behavior unchanged.

## Error Handling

An audio asset with zero Cloudinary dimensions and null request dimensions must create a chat message with null normalized dimensions. Invalid negative dimensions remain rejected by `ChatMessageValidator`.

## Testing

Add a `SendMessageServiceTest` that sends an AUDIO request whose metadata has null width/height while the fetched `Media` has zero width/height. The test must fail with the current unboxing exception, then pass after the normalization fix and assert persisted/returned audio metadata retains null dimensions.

## Success Criteria

- The reported `Integer.intValue()` failure is reproduced before the fix.
- Audio message creation completes when width/height are unavailable.
- Existing backend chat validator, mapper, and send-service tests remain green.
- Only the backend repository is changed and committed on `social_media/main`.
