# Frontend and Backend Change Summary

## Backend Additions

Added a frontend-support module under:

`src/main/java/com/dauducbach/clone/modules/frontend`

New backend capability:

- Home screen aggregate API for Discover/Friends tabs.
- Profile summary API for publication-style profile pages.
- Notification list, unread count, mark-read and mark-all-read APIs.
- User settings API with privacy, notification, content, appearance and accessibility fields.
- Private library APIs for saved posts, saved collections, drafts and archive.

## Backend Fixes From Direct API Testing

- Fixed profile summary when a user has no avatar by replacing `defaultIfEmpty(null)` with an `Optional<Media>` flow.
- Fixed default settings creation for assigned IDs by inserting new `UserSettings` with `R2dbcEntityTemplate` instead of `repository.save` update semantics.
- Added Friends feed logic backed by mutual rows in `user_follower` instead of returning the Discover feed.
- Updated feed retrieval so an empty response caused by exhausted seen_post user marker clears the seen marker once and reloads feed candidates.
- Added conditional migration support for `notification_events.action_type`.
- Fixed notification timestamp mapping for R2DBC values returned as `OffsetDateTime`/`ZonedDateTime`.

## New Backend Entities

- `UserSettings` mapped to `user_settings`
- `SavedCollection` mapped to `saved_collections`
- `SavedItem` mapped to `saved_items`
- `UserDraft` mapped to `user_drafts`
- `UserArchiveItem` mapped to `user_archive_items`

## New Backend Endpoints

- `GET /app/home?userId=&tab=DISCOVER|FRIENDS&limit=`
- `GET /app/profiles/{userId}/summary?viewerId=&postLimit=`
- `GET /app/notifications?userId=&filter=ALL|UNREAD&page=&size=`
- `GET /app/notifications/unread-count?userId=`
- `POST /app/notifications/{notificationId}/read`
- `POST /app/notifications/read-all?userId=`
- `GET /app/me/{userId}/settings`
- `PATCH /app/me/{userId}/settings`
- `GET /app/me/{userId}/saved`
- `GET /app/me/{userId}/saved/collections`
- `POST /app/me/{userId}/saved/collections`
- `POST /app/me/{userId}/saved/items`
- `DELETE /app/me/{userId}/saved/items/{postId}`
- `GET /app/me/{userId}/drafts`
- `POST /app/me/{userId}/drafts`
- `DELETE /app/me/{userId}/drafts/{draftId}`
- `GET /app/me/{userId}/archive`
- `POST /app/me/{userId}/archive`
- `POST /app/me/{userId}/archive/{contentId}/restore`

## Database Scripts

Added:

- `src/main/resources/db/manual/frontend_support_schema.sql`
- `src/main/resources/db/manual/frontend_demo_seed.sql`

`frontend_support_schema.sql` creates the frontend support tables and applies conditional indexes/column migrations.

`frontend_demo_seed.sql` is idempotent and imports demo data for users, follows, stories, posts, post media, comments, likes, notifications, saved collections/items, drafts, archive items, settings, and chat conversation/messages.

Imported demo data into the local MySQL container after implementation. Current demo counts include 7 demo users, 13 seeded posts, 7 seeded stories, 11 seeded comments, 15 follower rows, and 4 chat conversations.

## Frontend Project

Added a new React + Vite + TypeScript project at:

`../social_media_FE`

Main files:

- `package.json`
- `vite.config.ts`
- `.env.example`
- `src/main.tsx`
- `src/App.tsx`
- `src/api.ts`
- `src/types.ts`
- `src/mockData.ts`
- `src/styles.css`

Implemented screens/components from `frontend.md` where backend exists or was added:

- Login/session flow using `/auth/login` and `/auth/session`.
- Pulse Social responsive shell.
- Home with Discover/Friends tabs from `/home`.
- Story timeline from backend story tray.
- Post card and post detail/comments panel.
- Search using `/posts/search` and `/posts/{postId}`.
- Create draft and publish flow using backend APIs.
- Profile summary from `/profiles/{userId}/summary`.
- Notifications from `/notifications`.
- Saved, collections, drafts and archive from `/me/{userId}` APIs.
- Settings from `/me/{userId}/settings`.
- Chat screen from `/chat/conversations`.
- Offline, empty, permission and session state examples.

Skipped or represented as static/system states because matching backend is not available yet:

- Full onboarding interests and suggested-users workflow.
- Advanced report/block/restrict/mute management.
- Featured story highlight management.
- Reels.
- Rich media processing controls.
- Advanced security sessions.

## Verification

- Backend: `mvn test` passed with 135 tests, 0 failures, 0 errors.
- Frontend: `npm run build` passed.
- Direct API checks passed for login/session, home Friends, profile, notifications, unread count, saved, collections, drafts, archive, settings, chat, and search.
- Playwright audit via `http://localhost:5173` passed for Home layout, nav hover, suggested friends, story tray, post detail, profile navigation, chat conversations/messages, and mobile/desktop overflow checks.
## Latest Frontend Alignment Pass

- Reworked the left navigation rail to show icon-only categories by default and reveal labels on hover.
- Replaced the old right context panel with suggested friends loaded from backend search/profile APIs.
- Replaced old brand strings with Pulse Social in the app shell and HTML title.
- Updated post actions to a compact pill-style like/comment/repost/share/save bar.
- Reworked post detail into an Instagram-style media/detail split panel and wired comments to `GET /comments/post/{postId}`.
- Wired feed author name/avatar clicks to `GET /profiles/{userId}/summary` so profiles open from Home and detail views.
- Updated Chat to load real conversation messages and send text through `/chat/conversations/{conversationId}/messages`.

## Latest Backend/Data Alignment Pass

- Added `UserStoriesRepository.findHomeStoryTray` so Home story tray returns approved, non-expired stories for the viewer and followed users.
- Enriched demo seed with Quang, Mai, Bao, additional posts, stories, comments, mutual friend relations, direct conversations, and a group conversation.
- Re-imported the idempotent seed into local MySQL after the update.
- Story viewer was reworked to match rontend.md: immersive near-black viewer, segmented progress, overlay author header, left/right navigation zones, and bottom reply controls.
- Home screen was reworked for responsive feed requirements: two-tab switcher with independent scroll restoration, mobile swipe tab navigation, sticky mobile tab/story area, current-user story/add-story controls, centered 680px feed, carousel controls/counter/progress, richer post content area, and loading/error/empty/end feed states.
- Post detail was reworked for responsive media and comments: desktop media/panel layout, mobile vertical flow, detail carousel, keyboard/swipe navigation, image zoom, full-screen media view, comment loading/empty/error/restricted states, nested replies, comment sort, like/reply/report actions, load-more replies, and backend-backed comment composer with sending/failed states.

## Connections Screen Update

- Added backend `GET /app/profiles/{userId}/connections` for `FOLLOWERS`, `FOLLOWING`, and `FRIENDS` tabs. The response enriches each row with username, display name, avatar URL, relationship context, viewer relationship booleans, and a display action.
- Added mutual-follow friends query in `UserFollowerRepository.findFriendsByUserId` so the Friends tab is backed by the same `user_follower` relationship model used by the feed.
- Added frontend `connections` view opened from profile metrics. The reusable screen includes Followers, Following, Friends tabs, search, sorting, suggested accounts, loading skeleton, empty state, and failed loading state.
- Updated the responsive layout so desktop uses a centered max-width panel and mobile uses a full-width list with simple row dividers instead of heavy cards.
## Immersive Story Viewer Update

- Reworked the frontend story viewer into a full-screen near-black immersive viewer with centered portrait media, segmented progress, author identity, relative time, close and more actions.
- Added story interactions for tap previous/next zones, mobile swipe, desktop keyboard navigation, pause/resume, press-and-hold pause, video mute/unmute, reply composer, and restrained monochrome like action.
- Added frontend states for loading, unavailable media, deleted story, network interruption, reply disabled, and end of story collection. The same viewer can render regular story rows and featured story collections through the shared `StoryItem` model.
## Content Creation Entry And Post Flow Update

- Replaced the basic create form with a responsive content creation entry screen for Post, Story, and disabled future Reel.
- Added a multi-step post creation flow: select media, edit media, post details, and review/publish.
- Added UI states for uploading, video processing, publish success/failure, draft confirmation, permission denied, and unsaved changes warning.
- The desktop layout uses a compact modal-style panel with media preview and controls side by side. Mobile uses a full-height sheet with safe-area-aware footer actions.
## Story Creation Screen Update

- Added a responsive story creation screen under the global Create -> Story path.
- The screen supports take photo, record video, choose media from device, and multiple story items with a portrait preview canvas.
- Added restrained monochrome editing tools for text, mention, link, location, drawing, crop, mute video, and background adjustment, with refined text styles only.
- Added publishing options for share to story, share to selected friends, add to featured story after publishing, save draft, and download original if supported.
- Added frontend states for camera permission denied, microphone permission denied, uploading, video processing, publishing, publish failure, draft save, and unsaved changes.
## Backend-Aligned Create Flow And Post Music Update

- Reduced the Post creation UI to fields currently backed by the backend: media list, caption/content, hashtags, optional shared music, save draft, and publish.
- Removed unsupported post UI controls from the active flow, including visibility, comment permissions, collaborator, hide engagement count, camera/gallery permission action, rotate, aspect ratio, video cover, and per-media alt text.
- Reduced the Story creation UI to backend-supported actions: choose media, optional shared music, save draft, and share story. Unsupported editor/publishing controls such as link, location, drawing, selected friends, featured story, and download original were removed from the active UI path.
- Post music metadata now uses only `musicId`, `musicStart`, and `musicEnd` on `PostCreateRequest` and `PostDetails`.
- `PostService` stores either one common image-music segment on `post_details` or per-image segments on `post_items`; video items always keep their music fields null.
- The schema script adds or normalizes `post_details.music_start/music_end` as `BIGINT` and removes the obsolete `music_display_name/music_url` columns.
- FE Create loads tracks from existing `GET /app/musics`, supports common image music or per-image music segments, and sends the selected mode through `POST /app/posts`.
## Responsive Notifications Screen Update

- Reworked the FE notifications screen into a responsive centered desktop list and full-width mobile list.
- Added notification filters: `All`, `Interactions`, `Connections`, and `System`.
- Grouped notifications by `Today`, `This week`, and `Earlier`.
- Added unread styling through font weight and subtle row background, not only a dot indicator.
- Added loading skeleton, failed loading, empty state, mark-all-as-read, contextual actions, thumbnails, system icons, and removed-source-content state.
- Extended `GET /app/notifications` with richer response fields for FE display: actor username/display name/avatar, entity id/type, content thumbnail, and `entityAvailable`.
- Extended backend notification filtering while keeping the existing API path and `UNREAD` compatibility.
- Enriched `frontend_demo_seed.sql` with more notification types for UI testing across interactions, connections, system, story, shared post, tag, and removed content cases.
## Repost Module Update

- Added the post-module `PostRepost` entity mapped to `post_reposts`, plus `PostRepostRepository`, `RepostService`, `RepostController`, and `RepostToggleResponse`.
- Added repost APIs under `/app/posts`: create repost, remove repost, check current-user repost status, count reposts for a post, and list posts reposted by a user.
- Extended feed responses with `repostCount` and `repostedByCurrentUser`, backed by `RepostService` instead of frontend-only state.
- Extended profile summaries with `friendCount` and `repostedPosts`, so the frontend profile can render separate Posts and Reposts sections.
- Confirmed Friends remains mutual-follow based: the user follows them and they also follow the user. Added `countFriends` for profile metrics.
- Updated frontend profile navigation so clicking another user's avatar/name opens that user's profile instead of being overwritten by the current user's profile route reload.
- Updated the frontend repost action to call the backend repost/unrepost API and synchronize the returned repost count.
- Added idempotent SQL for `post_reposts` and enriched frontend demo seed data with repost rows.
## Post Items And Story Music Update

- Added post-module `PostItem` mapped to `post_items`, with repository support and create-flow persistence after `post_details` is inserted.
- Extended `PostCreateRequest` with optional `items` for per-media order, caption, media id, image music id and music segment. Existing `mediaList` requests still work; when `items` is omitted the backend creates basic item rows from `mediaList`.
- Added profile fields for `school` and `job` on `user_details`, plus `user_social_media` schema/seed support. Profile summary now returns `school`, `job`, and `socialMedia` for FE display.
- Added `user_stories.music_id` while keeping `music_url` nullable for backward compatibility. Story create now accepts `musicId`; `musicUrl` remains a fallback field.
- Changed Home story tray response to `StoryTrayItemResponse`, including `musicId`, `musicName`, and `durationSeconds`. Image stories default to 5 seconds; video stories let the FE use video metadata duration.
- Updated FE profile header to render school, job and social media links; updated create post flow to collect per-media captions and send `items`; updated story creation to send `musicId`; updated story viewer to show music name under the author line.
- Enriched manual seed with profile school/job/social links, sample musics, image/video stories, image/video posts, and `post_items` rows.
## Repost Schema Guard And Feed Story Layout Fix

- Removed the temporary `PostRepostSchemaInitializer` runtime schema guard after the `post_reposts` table was created. `RepostService` now accesses the repository and entity template directly; schema changes remain managed outside application startup.
- Constrained the home feed story row inside the feed column with `min-width: 0`, `overflow: hidden`, and bounded desktop widths so stories cannot overlap the right-side suggestions panel.
## Common And Per-Image Post Music Update

- Changed `PostDetails.musicStart` and `PostDetails.musicEnd` to `Long`; post-level music is represented only by `musicId`, `musicStart`, and `musicEnd`.
- Replaced `PostCreateRequest.musicDisplayName/musicUrl` with `musicStart/musicEnd`.
- Added backend validation requiring complete, non-negative music segments with `musicEnd > musicStart`.
- When post-level `musicId` is present, `PostService` clears all `post_items` music fields. When it is absent, each image may persist its own `musicId/musicStart/musicEnd`.
- Video media never persists post-item music. A video-only post also does not persist common music.
- Updated post-upload Kafka payloads to contain music id and segment only.
- Updated the Create Post editor so common music is selected before item editing. Per-image selectors and From/To inputs appear only while common music is empty; video items expose caption editing only.
- Updated draft and publish payloads, review summary, responsive styles, migration SQL, and seed data for the two music modes.
## Create Post Music Studio And Cloudinary Transformers

- Extracted the post publishing flow into `PostCreationStudio` with a master-detail media editor, active-item preview, compact per-item music status, and one expanded item editor at a time.
- Kept per-item music controls exclusive to Step 2 and shared post music exclusive to Step 3. Shared selection hides item controls, requires confirmation when item music exists, and restores item controls after removal.
- Added compact music search rows with artwork, artist, duration, preview controls, selected state, loading and empty states.
- Added selected-track summaries, clip-length choices, dual range controls, monochrome waveform feedback, live preview attribution, mode status, confirmation toast, and three Step 4 music summaries.
- Added desktop, tablet, mobile and reduced-motion styles scoped to the Create Post studio.
- Extended `CloudinaryMediaService` with `MediaTransformRequest`, `MediaTransformationResult`, and `MediaTransformType` for MUSIC, IMAGE and VIDEO delivery transformations.
- Added reactive and synchronous transformed-URL methods. Music transformations currently emit validated `so_` and `du_` directives; image and video accept validated extension directives for future processing.
## Expanded Create Post And Story Workspaces

- Expanded the Create Post and Create Story app shell from the normal centered-page limit to a viewport-aware workspace up to 1480px wide.
- Both creators now use nearly the full viewport height while keeping their headers, editors, and action footers inside a stable grid.
- Added bounded internal scrolling for editor panels so long post music, media, and story settings remain accessible without being covered.
- On mobile, both creators switch to a true full-screen layout and hide the persistent mobile navigation while editing so the bottom actions remain visible above safe-area insets.
## Route-Aware Floating Messenger Launcher

- Kept the full floating message launcher on Home while using a compact icon-only launcher on other non-Chat screens.
- Both launcher variants open the existing floating conversation panel; only the panel Maximize action navigates to the full Chat page.
- The floating panel now collapses when moving between Home and another screen so the destination always starts with its correct launcher presentation.
- Preserved existing conversation, unread, draft, close, and maximize behavior.
## Create Post Header And Media Selection Redesign

- Removed the Post/Create post title block and replaced numbered step buttons with four connected progress dots centered in the Create Post header.
- The active dot is emphasized while completed dots and connectors use the stronger monochrome state; all steps remain keyboard accessible and selectable.
- Step 1 now uses one centered upload workspace while no media exists and does not render the empty preview placeholder.
- After media is selected, Step 1 switches to a preview-and-list layout, hides the original upload surface, and exposes one Add more action below the media list.
- Added compact image/video rows with order, thumbnail, filename, type, processing state, active selection and remove action. Selecting a row updates the large preview through the existing `activeMediaId` state.
- Existing upload, music, draft, publish and backend request flows were preserved.
## Create Post Step 2 Accordion And Music Range

- Replaced the Step 2 master-detail item editor with a single accordion list. Opening an item also selects it for the left media preview and closes the previously expanded item.
- Each expanded image row contains its caption and one-track music editor; selecting another track replaces the existing item track. Video rows remain caption-only.
- Selecting an item track now starts its default clip immediately.
- Added a draggable waveform range window that moves Start and End together while preserving clip length, plus keyboard arrow movement for the selected interval.
- Start and End handles remain ordered and bounded by the track duration. Releasing either handle or the range window restarts playback from the committed Start and stops at End.
- Removed the repeated Music for this item label, selected-segment summary, duration summary and Preview/Restart control.
- Restyled Play, Replace and Remove as compact centered pill controls while retaining mobile touch targets.
- Preserved existing post item music fields and backend request payloads.
## Natural-Size Create Post Preview

- Changed the Create Post media preview from a forced 4:5 cover presentation to intrinsic-size contained rendering.
- Images and videos are no longer enlarged to fill the preview; they retain their original dimensions unless they must be reduced to fit the available bounds.
- Removed preview cropping and stretching by using automatic aspect ratio and `object-fit: contain`.
- Matched the preview panel, frame and unused letterbox space to the surrounding white application background.

## Orientation-Aware Create Post Preview

- Updated Create Post image previews to scale by natural orientation: landscape and square images fill the preview width, while portrait images fill the preview height.
- Kept aspect ratio unchanged and preserved the white empty area around non-matching image dimensions.

## Async Item-Based Post Creation

- Changed post creation to use item-based media payloads only. `PostCreateRequest` no longer depends on `mediaList`; each `PostItemCreateRequest` now carries `secureUrl`, `publicId`, optional `resourceType`, caption, and optional item music segment.
- Frontend now validates create-post media before upload: only image/video files are accepted, images are limited to 50MB, and videos are limited to 500MB.
- Frontend now requests a Cloudinary signature from `/media/signature`, uploads selected media to Cloudinary, then submits `/posts` with uploaded `items`.
- Backend now saves media posts as `PENDING_SCAN`, publishes `check_media_event`, and immediately returns `Bài viết mất một chút thời gian để tải lên, vui lòng đợi`.
- `ImageScanWorker` now scans each post item independently, deletes rejected media from Cloudinary, inserts approved `media` rows, creates `post_items` with the saved media asset id, approves or rejects the post, then sends `post_upload` SSE.
- Removed the obsolete `wait_for_upload_post:*` Redis flow from post creation and post deletion cleanup.
- Added global frontend SSE handling for `/posts/sse/{userId}` so `post_upload` messages appear as toast notifications.
- Removed post/comment/like audit persistence while keeping audit logging for auth/user-owned actions.
- Create Post footer no longer shows Save draft by default. Draft saving is offered only from the unsaved-changes close dialog, and Back/Next/Publish are centered.
## Elasticsearch Startup Dependency Fix

- Removed hard startup dependency on reactive Elasticsearch repository beans for post/user vector flows by marking vector repository interfaces as non-instantiable and using `ReactiveElasticsearchOperations` directly in vector services.
- Feed and vector enrichment now degrade gracefully when Elasticsearch is unavailable: vector reads/searches return empty fallback data and vector writes are swallowed after logging instead of preventing application startup.

## Post Media Scan Failure Cleanup

- Updated `ImageScanWorker` so media processing errors are no longer counted as community-policy rejections.
- When post media processing fails technically, the worker now cleans up Cloudinary media, `post_items`, `media`, Redis post cache, and the temporary `post_details` row before sending the SSE failure message.
- When every media item is rejected by policy, the worker now deletes the post from `post_details` instead of keeping a rejected caption-only post in the database.
- Added scan finalization logging for approved, rejected, and processing-failure counts.

## R2DBC Insert Boundary Cleanup

- Updated `ImageScanWorker` so scanned post media and `post_items` are inserted through `R2dbcEntityTemplate` instead of repository `save`.
- Updated comment media insertion inside `ImageScanWorker` to use `R2dbcEntityTemplate` as well.
- Updated `LibraryService` creation flows for saved collections, saved items, new drafts, and archive items to use `R2dbcEntityTemplate.insert`.
- Kept repository `save` only for update flows such as existing draft updates, post scan status updates, and comment updates.


## Post Detail Items, Captions, And Music Playback

- Changed GET /posts/{postId} to return a post-owned PostDetailResponse containing ordered post_items, resolved media metadata, per-item captions, and optional transformed music playback metadata.
- Added shared post music and per-item music response models. Music segments are delivered through CloudinaryMediaService.transformMusicUrl.
- Updated the post detail modal to hydrate from the detail API instead of relying only on legacy feed media.
- Changed the detail media stage to white and added an animated thought-bubble caption that expands on hover or keyboard focus.
- Shared music now keeps one continuous playback timeline across image transitions. Per-item music keeps an independent playback position for every item and resumes when the user returns to it. Video items pause post music.
## Display-Aware Cloudinary Media Delivery

- Added MediaDisplayType in the post module with frontend-derived delivery targets: POST 1440x1800 using c_fit, STORY 1080x1920 using c_fill, and AVATAR 256x256 using face-aware c_fill.
- Added CloudinaryMediaService.transformDeliveryUrl to apply display-size, automatic format, and automatic quality directives while falling back to the stored URL for non-Cloudinary or unsupported URL shapes.
- GET /posts/{postId} now accepts mediaType with POST as the default and returns transformed media URLs without changing the response DTO or stored media rows.
- GET /profile-media/{userId}/stories and GET /profile-media/{userId}/avatar/current accept the same enum, defaulting to STORY and AVATAR.
- The frontend now requests post details and search-result details with mediaType=POST, renders transformed images at intrinsic size, and only scales them down when they exceed the media stage.
- Post Detail now uses direction-aware carousel transitions, provides a music mute control opposite the caption control, and no longer renders zoom or fullscreen media actions.
- POST delivery now uses c_fit instead of c_limit so smaller source images may be enlarged while preserving aspect ratio; Cloudinary fits landscape media by width, portrait media by height, and chooses the limiting edge for square or unusual ratios without overflowing the target bounds.
## Post Engagement Lists And Comment Pagination

- Added `GET /likes/targets/{targetId}/actors?targetType=POST&page=0&size=20` to return paged actor IDs ordered by newest like.
- Added `GET /posts/{postId}/reposts/actors?page=0&size=20` to return paged actor IDs ordered by newest repost.
- Added `GET /comments/post/{postId}/page?page=0&size=10` with `PageResponse<Comment>` so the frontend can load additional root comments reliably.
- Root comments and replies are ordered newest first. Root comment responses include a transient `replyCount` without adding a database column.
- Comment creation validates that the parent comment belongs to the same post and rejects replies deeper than three levels.
- The frontend now hydrates liker and reposter profile rows through the existing profile summary API, loads ten root comments at a time, and fetches direct replies only after selecting View replies.

## Three-Level Comments And Comment Likes

- Comment creation now supports three levels: root, reply, and reply-to-reply. A fourth level is rejected by backend validation.
- Root and child comment APIs accept an optional `viewerId` and return transient `replyCount` and `hasLiked` fields without changing the comments table.
- The frontend sends `viewerId`, recursively loads child comments up to level three, shows Reply through level two, and hides Reply at level three.
- Comment like toggles call the existing `POST /likes/users/{actorId}` API with `targetType=COMMENT`, use the returned state, and roll back the optimistic UI state on failure.
- Post Detail comment overflow, nesting width, and modal height were adjusted to prevent horizontal drift and provide more vertical space.
- Comment option menus now center their actions and close on outside click or Escape.
## Single-Media Comments

- The Post Detail comment composer now opens a native file picker and accepts one image or video per comment.
- The frontend validates media MIME type and enforces the existing 50 MB image and 500 MB video limits before upload.
- Comment media is uploaded to Cloudinary through the server-issued signature, then the comment request sends one `mediaList` entry containing `secureUrl` and `publicId`.
- Media-only comments are supported; text remains optional when an attachment is present.
- Comment media results are received through `comment_success_event` and `comment_failed_event`; approved comments refresh the visible comment thread and display toast feedback.
- Backend comment creation rejects payloads containing more than one media item or missing Cloudinary identifiers.
- `ImageScanWorker` verifies fetched Cloudinary metadata is an image or video within the configured size limit before inserting the comment-owned `media` row. Invalid media removes the pending comment through the existing rejection flow.
## Comment Media Delivery And Viewer

- Added `MediaDisplayType.COMMENT` with a 720x900 `c_fit` Cloudinary delivery target, sized for the frontend's 280x350 comment-media frame while remaining smaller than POST delivery.
- Root comments, paged root comments, child replies, comment-by-id, and user comment lists now return transformed `mediaUrl` values through `CloudinaryMediaService.transformDeliveryUrl`.
- The frontend renders comment images and videos inside a borderless responsive 4:5 frame with `object-fit: contain`, preserving landscape-by-width and portrait-by-height behavior.
- Avatar sizing selectors now target only direct children of each comment row, preventing them from shrinking nested comment attachments to 32px or 28px.
- Selecting a comment attachment opens a portal-based full-screen media viewer. Images can be inspected at a larger size, while videos open with playback controls.
## Feed Engagement Count Cache Separation

- Removed like and comment counts from `FeedPostDetailsCache`; repost count is also kept outside this aggregate cache.
- `FeedService` now resolves like, comment, and repost counts through their owning services while hydrating every feed response.
- Added a dedicated Redis cache for repost totals using `post_repost_count:{postId}` with a 24-hour TTL and database fallback on cache miss.
- Repost and unrepost operations initialize the count cache before mutation, then update the cached total atomically after the database operation.
## Feed Media Delivery And Music Playback

- Added `MediaDisplayType.FEED` with a 1360x1496 aspect-preserving `c_fit` Cloudinary delivery target for the shorter 10:11 home feed frame.
- `/home` and `/feed` now accept `mediaType`, defaulting to `FEED`; both Discover and Friends hydrate posts through the same FeedService path.
- Versioned `FeedPostDetailsCache` now stores original media URLs plus raw ordered post-item and music metadata. Older cache payloads are rebuilt automatically.
- Feed media and nested post-item URLs are transformed only while creating the API response; transformed delivery URLs are never written back to entities or Redis post-detail cache.
- Feed responses now include shared post music and per-item caption/music metadata so the frontend does not issue one post-detail request per feed card.
- The home feed maps ordered items directly, keeps shared music continuous across every carousel item, preserves playback positions for per-item music, suppresses unsupported per-item music on video items, loops completed clips, and exposes compact playback/mute controls.
- A visibility coordinator grants playback to only the most visible feed post, pausing other carousel audio while scrolling.
- Feed carousel height is calculated from all media items: any portrait or square item keeps the 10:11 maximum height, while an all-landscape carousel uses the tallest landscape item and shrinks the frame accordingly.

## Feed Shared Music And Carousel Synchronization

- Feed cache schema version 3 now retains the raw post-level musicId, musicStart, and musicEnd; FeedService resolves shared music while hydrating each response and falls back to cached metadata when the music service is temporarily unavailable.
- Fixed a Redis namespace collision where PostService and FeedService both wrote different DTO shapes to post_details:{postId}. Source post cache now uses post:details:v2:{postId}, while feed aggregate cache uses feed:post-details:v3:{postId}; old mixed cache entries are bypassed automatically so shared musicId is reloaded from the database.
- Shared post music uses one stable playback key across the complete carousel, including item transitions, so its timeline is not reset or paused when the active media changes.
- Feed cards no longer display track title or artist metadata for shared or per-item music; only the compact mute control remains when the active item is an image.
- Post Detail no longer displays shared track metadata below the author name; playback and mute behavior remain attached to the media viewer.
- Shared post music pauses when the active carousel item is a video and resumes from the preserved playback position when the user moves to an image.
- Feed and Post Detail carousels preload the target media and animate outgoing and incoming layers with full horizontal slide transitions. This prevents the previous image from flashing while the next transformed Cloudinary asset loads.
- Opening Post Detail suspends all feed audio while preserving its playback position. Closing the modal restores the visibility-owned feed audio without competing with the detail player.
- The feed mute control was reduced to 32px with a lighter translucent background.
## Post Media Ratio And Detail Delivery

- Added post_details.media_ratio with the supported values 1:1, 4:5, 3:4, 9:16, 4:3, 3:2, and 16:9; missing or legacy values are exposed as 4:5.
- Added backend validation and included mediaRatio in post creation, post detail, feed, profile post, event, and versioned feed-cache data.
- Added post_media_ratio_schema.sql and the equivalent idempotent update in frontend_support_schema.sql.
- Added a ratio selector to Create Post, persisted it in draft payloads, submitted it to POST /posts, and included it in the review summary.
- Feed media frames now use the post-selected ratio and keep media contained inside the generated frame.
- Changed MediaDisplayType.POST from Cloudinary c_fit to c_limit, preventing Post Detail delivery from enlarging source images while retaining automatic format and quality transforms.
- Bumped source post and feed aggregate cache namespaces so stale payloads without mediaRatio are rebuilt.
## Narrower Home Feed Canvas

- Limited the complete Home feed canvas, including stories and tabs, to 630px.
- Limited post cards, feed skeletons, and feed states to 550px and centered them within the 630px canvas.
- Updated FEED Cloudinary delivery bounds to 1100x1956 with c_fit, providing a 2x source for the 550px frame through the tallest supported 9:16 ratio without cropping.
## Media Focus, Post Detail Frame, And Scroll Isolation

- Added a global browser focus and visibility controller that pauses currently playing audio and video whenever the tab is hidden or the browser window loses focus.
- Only media that was playing before suspension is resumed after focus returns; explicit Story pause state remains preserved.
- Post Detail now locks document scrolling while open and restores the exact previous feed scroll position when closed.
- Post Detail media is rendered inside a fixed frame derived from the post mediaRatio, with contained intrinsic-size images and neutral background space around unmatched dimensions.
- Reduced the desktop Post Detail modal width to 1120px so the fixed media frame and discussion sidebar remain balanced.
- No backend API change was required because POST media delivery already uses Cloudinary c_limit.
## Post Detail Intrinsic Media Sizing Correction

- Limited the Post Detail media frame to 550px so landscape images fit by width and portrait images fit by height without appearing larger than the Home feed post frame.
- Kept intrinsic image sizing with max-width and max-height constraints, preserving contained media and background space for unmatched dimensions.
- Updated POST Cloudinary c_limit bounds from 1440x1800 to 1100x1956, matching a 2x 550px frame through the tallest supported 9:16 ratio.
- The backend bound change controls delivery resolution and payload size; the frontend frame constraint controls the visible size.
## Post Detail Orientation-Based Media Fit

- Removed the Home feed mediaRatio frame and 550px viewer limit from Post Detail.
- Post Detail now uses its own complete media region instead of reusing feed-frame sizing.
- Landscape media explicitly fits the Post Detail region by width; portrait media explicitly fits by height.
- Both orientations remain contained without cropping, and unused space keeps the Post Detail background.
- Restored POST c_limit delivery bounds to 1440x1800 because these dimensions match the larger Post Detail region; c_limit still prevents Cloudinary from enlarging source media.
## Post Detail Media Container Spacing
- Removed desktop and mobile padding from the Post Detail media container so the media reaches the media panel edges.

## Post Detail Media Contain Sizing
- Updated Post Detail image/video sizing to use intrinsic dimensions with both max-width and max-height constraints, preserving aspect ratio and preventing crop or overflow.

## Post Detail Aspect Ratio Convention
- Corrected FE media orientation calculation to use the project's dọc / ngang convention. A vertical 16:9 asset is now treated as portrait and constrained by the frame height while preserving its ratio.

## Post Detail Media Fit and Create Preview Orientation
- Post Detail now uses contain on the full media frame for both images and videos, allowing small assets to scale up to the fitting edge while oversized assets scale down without cropping.
- Create Post preview now detects video orientation from loaded metadata and uses the same height / width orientation convention as images.

## Post Detail Intrinsic Media Scaling
- Post Detail now reads the actual image/video dimensions and calculates a contain scale against the rendered frame, preventing landscape 4:3 and 16:9 assets from being cropped or over-zoomed while still enlarging small media to the fitting edge.

## Post Detail Close Interaction
- Restyled the close control as a transparent circular button and added backdrop click-to-close while keeping clicks inside the modal content active.

## Post Detail Default Media Frame Ratio
- Set the desktop Post Detail media frame default to 4:3 using the project's vertical:horizontal convention, implemented as CSS 3 / 4; the existing frame height is preserved while its width is reduced.

## Post Detail Media Column Ratio Alignment
- Reduced the complete desktop media grid column and modal width to the 4:3 vertical:horizontal frame instead of shrinking only the inner viewer, removing the unused horizontal gutters around matching 4:3 media.

## Post Detail Exact 4:3 Column Sizing
- Removed the viewport-width cap that could make the desktop media column narrower than its configured vertical:horizontal 4:3 ratio, allowing matching portrait media to fit both frame height and width.

## Post Detail Final Height/Width Synchronization
- Synchronized the desktop media width with the final overridden modal height (min(900px, 100dvh - 24px)), using an exact 3/4 width calculation so portrait 4:3 media reaches both frame dimensions.

## User Identity Enrichment: fullName

- Added `fullName` to `CreateUserRequest` and the registration form payload. The normal `profile_creation_event` now carries the field automatically.
- Added OAuth full-name extraction for Google, GitHub and Facebook, with username/email fallback, before publishing `profile_creation_event`.
- Added `fullName` to `UserDetails`, profile update input, user-profile vector refresh text and the `user_details` schema/seed data.
- Feed cache schema version was bumped and now carries `authorFullName`; feed and post-detail responses expose `authorUsername` plus `authorFullName`.
- Connections use `fullName` as `displayName`; notification actor display names now use `user_details.full_name` with username fallback.
- Added frontend composition API `GET /frontend/comments/post/{postId}/page` and `GET /frontend/comments/parent/{parentId}`. It enriches post-module comments with username, fullName and avatar URL without moving user repositories/entities into the post module.
- FE identity presentation now shows fullName above `@username` in profiles, suggestions, connections, engagement lists and post authors. Comments show the username only, without `@`, and use enriched comment data.
- Added an idempotent `full_name` migration and populated full names in `frontend_demo_seed.sql`.
## Connections Popup And Author Avatars

- Profile Followers, Following and Friends metrics now open a modal over the current profile instead of changing the active application screen.
- Removed suggested-account content from the connections experience.
- Added own-profile relationship confirmations: removing a follower deletes the inbound follow relationship; unfollowing deletes the outbound relationship. Both confirmations use the selected user's avatar and vertically stacked actions.
- Feed and hydrated profile-post responses now include `authorAvatarUrl`, resolved through the avatar media transformer. The feed cache schema was bumped so cached records are rebuilt with the new field.
- FE post mapping now uses `authorAvatarUrl` instead of forcing the avatar fallback, and comment avatar images are rendered as circular media.
## Avatar Shape And Connection List Stability

- Locked feed, Post Detail, comment and engagement-list avatar images to a square aspect ratio with circular clipping and non-shrinking dimensions.
- Changed the connections modal result container from a stretching grid to a normal scroll list.
- Fixed every connections row at 72px so sparse results no longer expand and long results scroll inside the existing modal height.
## Messaging Experience Redesign

- Refined the floating messenger into a fixed-size list/detail widget with a compact search field, stronger unread and selected states, denser conversation rows, calmer empty states, monochrome message bubbles and an integrated composer.
- Redesigned the full Chat page as a stable two-column workspace with a 320-356px inbox, functional All/Unread filtering, compact search, fixed conversation header, internally scrolling timeline and fixed composer.
- Unified avatar sizing, typography, neutral surfaces, focus/hover states, bubble geometry, metadata and empty states across both messaging surfaces.
- Preserved the existing chat API calls, optimistic sending, failed/queued states, reply state, read cursor updates and mobile inbox/conversation routing.
- Added tablet width reductions, mobile full-screen pane behavior, safe-area composer spacing and reduced-motion handling.
## Feed Video Playback And Media Item Captions

- Active feed videos now use viewport-aware autoplay: they play muted when at least 55% visible, pause when leaving the viewport or carousel active state, and loop while active.
- Profile post grids now render video posts with a paused native video thumbnail positioned on the first scene instead of attempting to load the video URL through an image element.
- Added a compact video indicator to profile thumbnails.
- Feed carousel items with individual captions now expose the same expandable message-style caption trigger used by Post Detail.
## Long Feed Caption Handling

- Common post captions in feed cards now use a word-safe 180-character preview with accessible More/Less expansion.
- Expanded common captions are height-limited and scroll internally so long content does not push the feed card excessively.
- Per-media captions longer than 180 characters receive a larger bounded hover/focus bubble with internal scrolling, long-word wrapping and contained overscroll.
## Navigation Persistence And Feed Infinite Loading

- Removed the sidebar brand/logo block so the navigation starts directly with the navigation list; the existing hover expansion and logout placement remain unchanged.
- Persisted the active view, active feed tab and profile user id in `sessionStorage`, so a browser reload returns to the same application screen instead of defaulting to Home.
- Clicking Home in the desktop sidebar now writes Home to storage and performs a real page reload, which rehydrates the Home screen and requests a fresh first feed batch.
- Added a Home feed sentinel with a large intersection margin. When the user approaches the end of the current list, the FE calls `/home` again and appends unique posts while preventing overlapping requests.
- Added `page` to `GET /home`. Discover continues using the existing seen-feed batch behavior; Friends now uses `LIMIT/OFFSET` pagination and reports `hasMore` correctly.
- The FE resets the feed list, page counter and continuation state whenever Home or its Discover/Friends tab is freshly loaded. If a backend cycle returns only already-rendered posts, the FE stops the continuation instead of creating an endless duplicate loop.
## Profile Post Thumbnail Order

- Preserved `PostItem.orderNumber` in the frontend post-media mapping.
- Profile post and repost grids now explicitly choose the media item with `orderNumber = 1` as the thumbnail, with the first ordered media item as a legacy-data fallback.
- Video thumbnails continue to use the first video frame without autoplay.
## Profile Summary First Item

- Added `ProfilePostResponse` for profile post/repost summaries. It removes the full `media` list and exposes one `firstItem` instead.
- `PostDetailQueryService` now resolves the first `PostItem` by `orderNumber` and joins its `Media`, including the existing media delivery transformation.
- The FE maps `firstItem` into the profile grid and still requests `/posts/{postId}?mediaType=POST` when opening the full post detail.
- Removed the `PROFILE` eyebrow text from the profile header.
## Active Sidebar And Text-Only Posts

- Active desktop sidebar items now show a subtle gray circular background around the active icon without filling the entire navigation row.
- Text-only feed posts and Post Details use a full soft-gray media canvas with centered text, safe wrapping for long words and internal scrolling for content that exceeds the available frame.
## Structured Profile Information

- Profile summary now composes `jobs`, `universities`, and `highSchools` from the user module. Other viewers receive public entries only; the profile owner receives all entries so hidden records can be managed.
- The profile UI no longer creates a biography from location or hobbies and no longer renders an empty biography placeholder.
- The profile header now conditionally shows current city, hometown, featured work/education, hobbies, and social links without reserving space for missing information.
- Added responsive About and Edit Information panels. The owner can update or clear city, hometown, and hobbies; add, edit, delete, and change visibility for work and education; and add, edit, or delete social links.
- Added `PUT /user-universities`, `PUT /user-high-schools`, and `PUT /user-social-media` using the existing request types with optional `id` for updates.
- User details updates now accept empty city, hometown, and hobby values so those profile fields can be removed.
## University Insert Fix

- `user_university` previously used reserved column names `from` and `to`, which could make `R2dbcEntityTemplate.insert` fail with the generic `USER_UNIVERSITY_SAVE_FAILED` message.
- The entity now maps to `from_date` and `to_date`; the repository query was updated accordingly.
- `frontend_support_schema.sql` contains an idempotent migration from the old reserved names and creates the profile tables with safe date column names.
- University creation now logs the original SQL exception as the throwable instead of logging only the wrapped message.
## Remove Legacy School And Job Fields

- Removed school and job from UserDetails; work and education are now sourced exclusively from user_job, user_university, and user_high_school.
- Removed the legacy fields from the profile summary API and frontend profile fallback mapping.
- Updated the support schema to drop the legacy columns idempotently and moved demo profile enrichment into the dedicated work and university tables.

## Chat Conversation Naming And Composer

- Direct-conversation titles are now resolved for the current actor from the other active member's `fullName`, with `username` and user id fallbacks. Group conversations continue to use their stored group title.
- Added `ChatReadRepository.findConversation` so list, detail and newly-created conversation responses use the same actor-specific title mapping.
- Added `GET /user-details/chat-suggestions?viewerId={id}&query={prefix}&limit={n}` in the user module. An empty query returns mutual follows; a non-empty query performs case-insensitive username prefix matching and ranks mutual follows, followers/following, then other accounts.
- The suggestion response contains `id`, `username`, `fullName`, and the latest avatar URL.
- The full Chat page and floating messenger now share a multi-user conversation composer. One selected account creates a direct conversation; multiple selected accounts create a group conversation whose initial title is generated from the selected display names.
- Frontend message sends now generate canonical UUID `clientMessageId` values with `crypto.randomUUID()`, matching the existing backend validation.
- The Chat inbox header now displays only the signed-in username, and the global application Back control is hidden on the Chat screen.
## Chat Nicknames And Assigned-ID Inserts

- Added nullable `nickname` to `conversation_members` and an idempotent migration in `chat_schema.sql`.
- Direct-conversation titles now resolve the other member in this order: conversation-member nickname, full name, username, then user id. Group conversation titles continue to use `conversations.title`.
- Chat message queries now enrich each sender with `senderDisplayName` and `senderAvatarUrl`; group messages resolve sender names using nickname, full name, username, then user id.
- Added `PUT /chat/conversations/{conversationId}/members/me/nickname?actorId={userId}`. A blank or null nickname clears the member nickname.
- Replaced assigned-ID inserts for conversations, conversation members, pending member requests and messages with `R2dbcEntityTemplate.insert`. Repository `save` remains only for updates to existing member requests.
- Stabilized the new-conversation popup height and retained existing results while a debounced username search is loading, preventing the search area from jumping between result and skeleton states.

## Browser Push Permission And Conversation Details

- Added a Request Permission control at the bottom of Settings > Notifications. It opens a focused confirmation dialog, requests the native browser permission, registers the Firebase messaging service worker, obtains an FCM token, and persists the token through POST /notifications/push-tokens.
- Added Firebase Web SDK configuration, a stable browser device id stored in localStorage, optional VITE_FIREBASE_VAPID_KEY support, and a public firebase-messaging-sw.js included in the Vite build.
- Added GET /chat/conversations/{conversationId}/details in the chat module. It returns the current member's notification mute state and active members enriched with nickname, full name, username, avatar, and role.
- Added PUT /chat/conversations/{conversationId}/notifications to persist the current member's conversation notification toggle through conversation_members.muted_until.
- Added a responsive conversation details drawer opened by the Chat header three-dot action. The drawer stays beside the thread on desktop, becomes full-screen on mobile, loads real members, supports mute and nickname updates, closes through its trigger, close action, Escape, or outside click, and preserves the active conversation.

## Realtime Chat Delivery, Presence, And Receipts

- Extended `SendMessageRequest` with `recipientId` for direct chats and `recipientIds` for group chats. The chat service validates supplied recipients against active conversation membership and includes the resolved recipients in the event payload.
- Message persistence remains transactional and uses the per-conversation row lock and sequence. After commit, `chat.message.created` is published with `conversationId` as the Kafka key, a complete enriched message payload, and recipient ids.
- Added separate Kafka consumer groups: `chat-realtime-service` broadcasts message and cursor events to WebSocket sessions; `chat-notification-service` resolves `SEND_MESSAGE` templates and sends FCM notifications only when the recipient is offline and has not muted the conversation.
- Added authenticated WebSocket endpoint `/ws/chat`, local session registration, Redis presence keys with a 90-second TTL, 30-second client heartbeat, stale-session cleanup, and reconnect handling.
- Delivery semantics now use the existing `conversation_members.last_delivered_seq` and `last_read_seq` cursors. Receiving a WebSocket message triggers `DELIVERED_ACK`; opening the matching visible conversation triggers `READ_ACK`. Cursor updates are published on `chat.cursor.updated`.
- Conversation list responses now expose aggregate recipient delivered/read cursors so outgoing message status can be restored after reload. The FE renders `Đã gửi`, `Đã nhận`, and `Đã xem` from those cursors instead of a hard-coded Seen label.
- The full chat page and floating messenger share one reconnecting WebSocket client, append and deduplicate incoming messages, update conversation previews, and synchronize unread badges in the left navigation and floating launcher.
- Added `GET /chat/presence/{userId}` for the direct-conversation activity label.

## Chat presentation and focus-aware read receipts (2026-07-27)

- Restored sidebar icon visibility by separating icon wrappers from collapsible label selectors.
- Fixed consecutive message grouping so hidden avatars no longer reserve a second grid column or constrain bubble width.
- Mini chat now marks messages as read only after the conversation surface receives pointer or keyboard focus; loading or opening the panel alone does not advance the read cursor.
- Direct conversation labels synchronize with member display names returned by conversation details. Backend display priority remains `nickname -> fullName -> username`; group titles remain unchanged.
- Removed relative sent-time labels from inbox rows and message bubbles. Mini chat shows a full date/time separator only when adjacent messages are more than three hours apart.
- Added full-page message actions for reaction, reply, message details, forward, pin, report, and recall presentation.
- Reduced mini-chat bubble height and horizontal spacing, and applied fully rounded message corners.
- No backend contract or database change was required for this refinement.
## Chat nickname, delivery cursor, avatar and preview refinement (2026-07-27)

### Backend

- Added `PUT /chat/conversations/{conversationId}/members/{targetUserId}/nickname?actorId=...` so an active member can set or clear the nickname of another active conversation member. The existing `/members/me/nickname` endpoint remains available for compatibility.
- Extended `ConversationResponse` with `avatarUrl`, `lastMessageSenderId`, `lastMessageType`, and `lastMessagePreview`.
- Conversation queries now join the current last message and resolve the direct peer avatar from the latest `media` row with `owner_type = 'AVATAR'`.
- Fetching message history advances the requesting member's persisted `last_delivered_seq`. `ChatCursorService` publishes a `CURSOR_UPDATED` event so an online sender receives the delivered state immediately.
- Delivery/read state continues to be stored as monotonic cursors in `conversation_members`; no per-message status column and no schema migration are required.
- Confirmed the two Kafka consumers remain independent: `chat-realtime-service` broadcasts through WebSocket, while `chat-notification-service` checks Redis presence and sends FCM only when the recipient is offline. Push notification handling does not wait for WebSocket delivery success.

### Frontend

- Conversation titles and member sender labels use `nickname -> fullName -> username`; direct nickname editing targets the peer instead of the current user, and group chat allows selecting a target member.
- Full and mini inbox rows render backend avatar URLs and real last-message previews. Messages sent by the viewer use the `Bạn:` prefix; received messages do not.
- Full-page message actions render left of outgoing bubbles and right of incoming bubbles. Bubble radius is reduced to avoid clipping long content.
- Mini chat removes paragraph default margins, uses smaller message text, shows sent/delivered/read status for outgoing messages, and adds an emoji picker beside attachment.

### Database

- No database change is required for this update. Existing `conversation_members.nickname`, `last_delivered_seq`, and `last_read_seq` columns are reused.
## Message delivery status visibility (2026-07-27)

- Full chat and mini chat now show delivery status by default only for the latest outgoing message.
- Clicking an outgoing message bubble toggles its delivery status below the message; keyboard users can use Enter or Space.
- No backend contract or database change was required.
## Chat unread, conditional auto-scroll, and reconnect delivery (2026-07-27)

### Backend

- Conversation unread counts now count only messages sent by other members after the current member's read cursor; a sender's own messages no longer create an unread badge.
- WebSocket registration now advances pending last_delivered_seq cursors to each conversation's latest persisted sequence and publishes cursor updates, so online senders receive Đã nhận after an offline recipient reconnects.
- Existing tables and API contracts are unchanged.

### Frontend

- Full chat and mini chat track whether the message viewport is near the bottom before messages are appended.
- New sent or received messages scroll into view only while the user remains near the bottom; scrolling upward preserves the current reading position.
## Chat history synchronization, last-active presence, and receipt race fix (2026-07-27)

### Backend

- Message history without an explicit cursor now returns the newest page instead of the oldest page. Backward pagination removes the extra oldest row and exposes the correct cursor for loading older messages.
- Presence now stores the latest heartbeat/disconnect timestamp in Redis under chat:presence:last-active:{userId}. GET /chat/presence/{userId} returns online and lastActiveAt.
- Existing WebSocket disconnect cleanup remains responsible for removing online presence; no database schema change is required.

### Frontend

- Full chat and mini chat merge fetched history with realtime messages instead of replacing messages that arrive while a request is running.
- Messages sent from either chat surface are published locally through the shared realtime client, keeping both surfaces synchronized without creating an unread badge for the sender.
- The realtime client caches monotonic recipient delivery/read cursors, preventing a late send response from overwriting an earlier Đã nhận or Đã xem event.
- Full chat uses an HTTP read-cursor fallback when an opened conversation receives a message.
- Active status now renders Đang hoạt động, relative minutes/hours/days, or the last active date after three days. Fake index-based active dots were removed.
- Incoming group avatars align with the first bubble and use a size close to a compact message bubble.

## Chat initial positioning and reliable delivery acknowledgement (2026-07-27)

### Frontend

- Full chat adds more horizontal spacing between an incoming avatar and its message group while preserving alignment for grouped messages.
- Opening a conversation now positions the history directly at the latest message without a long smooth scroll from the top. Later messages still scroll smoothly when the viewer remains near the bottom.
- Incoming-message delivery and read acknowledgements use the existing WebSocket events plus an idempotent HTTP cursor fallback, so the sender receives the delivered state as soon as the online recipient receives the message rather than waiting for the recipient to read it.
- No backend contract or database schema change is required for this update.

## Chat reply clusters and media messages (2026-07-27)

### Backend

- `ChatMessageResponse` now includes a nested reply preview with the original sender, message type, content/media metadata, sequence, and deleted state.
- Chat history queries self-join the original message so reply previews remain available even when the original message is outside the currently loaded frontend page.
- IMAGE and AUDIO messages use the existing signed Cloudinary upload flow. The server fetches authoritative Cloudinary resource information, normalizes message metadata, and inserts the asset into `media` with `owner_type = CHAT_MESSAGE` in the same reactive database transaction as the message.
- Chat attachments do not enter the NSFW scan flow. IMAGE and AUDIO are limited to 50 MB; recorded AUDIO is limited to five minutes. Existing VIDEO validation retains the 500 MB limit.
- AUDIO messages now produce an explicit last-message preview in conversation lists.

### Database

- No new table or message column is required. `messages.reply_to_seq` and `messages.metadata` remain the persisted message contract.
- Run the updated statement in `src/main/resources/db/manual/chat_schema.sql` so `media.owner_type` accepts `CHAT_MESSAGE`:

```sql
ALTER TABLE media
    MODIFY COLUMN owner_type VARCHAR(32) NOT NULL;
```

### Frontend

- Consecutive incoming messages display the sender name above the first bubble in group conversations and the sender avatar beside the final bubble in the group.
- Reply messages render as a connected preview-and-message cluster. Selecting the preview fetches the original message when necessary, scrolls to it, and applies a temporary highlight in both full and floating chat.
- The full composer shows a cancellable reply preview. Full and floating composers support one image or one recorded voice message, upload progress through the existing send state, cancellation, validation, and failed-send presentation.
- Voice recording uses the browser `MediaRecorder` API with microphone permission handling, a five-minute timer, local playback, and a 50 MB limit.
- Image messages use constrained rounded previews; audio messages use compact native playback controls. Layouts remain width-safe on mobile.
## Mini chat reply actions and initial positioning (2026-07-27)

### Frontend

- Direct-message reply previews now distinguish replies to the viewer from self-replies. The same explicit direct/group context is used by full chat and mini chat.
- Selecting a reply preview in either chat surface loads the referenced message when needed, scrolls it into view, and applies a temporary highlight.
- Mini chat now reuses the full-chat reaction, reply, and message-options action bar. Replying adds a cancellable preview above the mini composer and sends the existing `replyToSeq` contract.
- Opening a mini-chat conversation positions the message stream directly at the latest message. Smooth scrolling remains enabled only for later messages while the viewer is already near the bottom.
- Message paragraph elements no longer add horizontal padding; spacing remains owned by the bubble container.
- No backend API, entity, or database schema changes are required for this update.
## Messaging media and audio refinement (2026-07-27)

### Frontend

- Full chat and floating chat now support selecting up to ten images, adding more files, removing individual images, drag-and-drop reordering, per-image upload state, retry state, and clearing the complete selection.
- Selected images render in a compact horizontal attachment tray above the unchanged text composer. Caption text remains editable while images are selected.
- The existing single-media backend contract is preserved: the frontend uploads selected images concurrently, sends ordered IMAGE messages through the existing endpoint, and sends the optional caption as the immediately following TEXT message. The display layer combines that ordered sequence into one persistent mosaic without changing request models.
- Image messages use responsive one-, two-, three-, and four-cell mosaic layouts. Larger groups show a +N overlay. Selecting a cell opens a keyboard- and swipe-navigable full-screen viewer at the selected index without losing chat scroll position.
- Optimistic media groups remain visible while uploading and expose stable sending/failure presentation. Failed selections retain the typed caption and can be retried.
- Native visible audio controls were replaced by a shared custom player with play/pause, seek, elapsed/total duration, loading/error/retry states, and optional playback speed. Starting another chat audio pauses the current one.
- Voice recording now has separate recording and recorded-preview surfaces. Image selections remain intact when voice recording is used, and the interface explains that audio is sent separately.
- Reply previews identify multi-image groups and include voice-message duration when available.
- Desktop, tablet, mobile, and floating-chat dimensions use the same media hierarchy with responsive limits and safe overflow behavior.

### Backend

- No API, request payload, entity, repository, database, or server-side behavior was changed.
## Chat media display correction (2026-07-27)

- Removed text-bubble padding and background from image and audio messages in both the full chat page and floating chat.
- Chat images now use a Cloudinary width transform and preserve natural height instead of being cropped into fixed aspect-ratio cells.
- Three- and four-image layouts use fixed grid columns while each image keeps its own aspect ratio.
- Simplified voice-message playback to play/pause, seek progress, and duration.
- Constrained mini-chat action controls so they no longer cover media content.
- Added defensive wrapping for long message and caption text.
- Frontend-only change; backend contracts and persistence are unchanged.
## Progressive chat history and reply navigation (2026-07-27)

- Full chat and floating chat now request older messages with `beforeSeq` when the user approaches the top of the loaded history.
- Prepending older messages preserves the current scroll anchor, so the visible conversation does not jump or reload.
- Reply navigation fetches the complete sequence range between the referenced message and the currently loaded history in batches of up to 100 messages.
- Reply targets within 100 sequence positions use smooth scrolling through the newly inserted messages.
- Longer reply jumps show a focused loading state, load the missing range, and then position the referenced message without a long animated scroll.
- Grouped image messages retain their source message sequences so reply navigation can locate and highlight the correct media group.
- Existing chat APIs are reused; no backend contract or database change was required.
## Full chat history sentinel and independent media cards (2026-07-27)

- Added a top-of-history `IntersectionObserver` sentinel to the full messaging page while retaining the scroll-position threshold as a fallback.
- Full chat now requests older pages using the same progressive prepend behavior as floating chat and preserves the current viewport anchor.
- Chat image groups are rendered as independent media cards rather than normal text bubbles.
- Image groups use a vertical natural-ratio stack with a 5px gap; exactly two landscape images may share one row without cropping.
- All grouped images are rendered, captions appear once below the complete group, and each image continues to open the full media viewer.
- Cloudinary width transformation is retained while image height follows the original aspect ratio without a height limit.
- Frontend-only change; no backend contract or persistence change was required.
## Full chat reply positioning and media width correction (2026-07-27)

- Full chat reply navigation now scrolls the `.dm-message-history` container directly instead of relying on `scrollIntoView` to select the correct scroll ancestor.
- The target lookup retries across animation frames after missing history is merged, ensuring the referenced message exists in the rendered DOM before positioning.
- Full-chat image messages now carry a dedicated row class with explicit row, content, bubble, and media-card widths.
- Desktop media actions are taken out of the image-width calculation so they no longer shrink the media card.
- Frontend-only correction; backend contracts remain unchanged.
## Full chat reply offset and media scale follow-up (2026-07-27)

- Reply positioning in full chat now calculates the target offset from bounding rectangles relative to the actual `.dm-message-history` viewport.
- The located target receives a direct 1.9-second highlight class, independent of React grouping state.
- Full-chat media rows now reserve up to 600px and image cards render up to 440px wide on desktop.
- Media action controls remain outside the media width calculation, preventing flex shrink from restoring the previous small size.