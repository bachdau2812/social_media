# Thiết kế UI/UX và hợp đồng dữ liệu Backend cho ứng dụng mạng xã hội

> Phiên bản: 1.0  
> Mục tiêu: Làm tài liệu đầu vào cho Stitch tạo giao diện và cho Codex/MCP triển khai frontend/backend integration.  
> Ngôn ngữ prompt cho Stitch: English.  
> Ngôn ngữ mô tả sản phẩm và hợp đồng dữ liệu: Tiếng Việt.

---

## 1. Tóm tắt sản phẩm

Ứng dụng là một mạng xã hội dành cho mọi đối tượng, có nhóm tính năng cốt lõi quen thuộc:

- User profile
- Follow / unfollow
- Quan hệ bạn bè khi hai người follow lẫn nhau
- Post ảnh, video và carousel
- Story
- Featured Story / Highlight
- Like, comment, reply, repost, share, save
- Discover
- Friends feed
- Search
- Notification
- Saved content, draft, archive
- Settings và privacy
- Reel và Messaging được để ở giai đoạn sau

Ứng dụng không sao chép giao diện Instagram. Hành vi quan trọng vẫn quen thuộc để người dùng dễ thao tác, nhưng cách tổ chức giao diện mang bản sắc riêng.

### Định vị thiết kế

**Professional Premium Social**

- Cao cấp nhưng không xa cách
- Sạch, nghiêm túc, dễ dùng
- Phù hợp cả nội dung đời sống và nội dung chuyên nghiệp
- Tập trung vào nội dung, typography và khoảng trắng
- Không dùng phong cách giải trí quá sặc sỡ

### Hướng giao diện chính

**Editorial Rail**

- Desktop dùng navigation rail bên trái
- Feed ở giữa
- Action rail hoặc contextual rail có thể xuất hiện ở cạnh bài viết
- Mobile chuyển về bố cục dọc quen thuộc
- Trang Discover mang tính biên tập thay vì lưới ảnh ngẫu nhiên
- Profile giống một trang publication cá nhân
- Nội dung có nhịp điệu nhưng vẫn dự đoán được

Tỷ lệ biến thể post đề xuất:

- 80% standard post
- 15% featured media post
- 5% text, collection hoặc editorial post

---

## 2. Design System tổng thể

### 2.1. Bảng màu

```text
Primary black:      #0B0B0B
Charcoal:           #262626
Medium gray:        #737373
Soft gray:          #A5A5A0
Divider gray:       #E7E7E4
Surface gray:       #F7F7F5
Pure white:         #FFFFFF
```

Chỉ dùng gradient rất nhẹ:

```text
#0B0B0B → #3A3A3A
#E7E7E4 → #FFFFFF
```

Không dùng:

- Gradient màu rực
- Vòng story nhiều màu
- Màu đỏ cho trạng thái liked
- Shadow lớn
- Glassmorphism trang trí
- Bo góc quá mức

### 2.2. Typography

- UI font: modern sans-serif hoặc grotesk
- Username, button: 500–600
- Body: 400
- Metadata: 400, màu xám
- Heading lớn: có thể dùng serif editorial rất hạn chế
- Body line-height: 1.45–1.65

### 2.3. Khoảng cách và radius

- Spacing scale: 4, 8, 12, 16, 24, 32, 48, 64
- Small radius: 6px
- Media radius: 8px
- Modal radius: 12px
- Avatar: circular
- Không bọc mọi thứ bằng card

### 2.4. Icon

- Outline monochrome
- Stroke thống nhất
- Active state: fill black
- Icon-only button phải có accessible name
- Touch target tối thiểu 44x44px

### 2.5. Motion

- Transition 150–250ms
- Fade, slide nhẹ, scale rất nhỏ
- Không bounce
- Không animation quá lớn che nội dung
- Respect `prefers-reduced-motion`

---

## 3. Responsive Layout tổng thể

### Desktop từ 1200px

```text
┌────────────────┬────────────────────────────┬──────────────────┐
│ Navigation rail│ Main content               │ Context rail     │
│ 72–240px       │ controlled width           │ optional         │
└────────────────┴────────────────────────────┴──────────────────┘
```

- Navigation rail có thể collapse chỉ còn icon
- Feed post chuẩn tối đa khoảng 680px
- Featured post có thể lớn hơn nhưng không full screen
- Right rail chỉ hiển thị thông tin phụ

### Tablet 768–1199px

- Navigation rail chỉ còn icon
- Ẩn right rail
- Feed giữ ở giữa
- Modal lớn chuyển thành full-height sheet nếu cần

### Mobile dưới 768px

- Compact top header
- Bottom navigation
- Media gần full width
- Text padding 12–16px
- Story scroll ngang
- Không phụ thuộc hover
- Tôn trọng safe area

---

## 4. Master Prompt dùng chung cho Stitch

Dán prompt này trước prompt của từng màn hình.

```text
Design a complete responsive social media application with an original
“Editorial Rail” visual identity.

The application has familiar social networking capabilities such as user
profiles, following, mutual friends, posts, image and video carousels,
stories, featured stories, likes, comments, reposts, sharing, saving,
search, notifications, drafts, archives and settings.

Do not visually copy Instagram, TikTok, X, Threads or any existing product.
Familiar interaction patterns may be preserved when they improve usability,
but the composition, typography, navigation, spacing and component language
must feel original.

PRODUCT DIRECTION

Create a professional premium social platform that remains accessible to
everyday users.

The experience should feel:
- Professional
- Premium
- Calm
- Refined
- Minimal
- Trustworthy
- Editorial
- Easy to learn
- Suitable for both personal and professional content

VISUAL SYSTEM

Use a strict monochrome palette:
- White
- Soft off-white
- Near black
- Charcoal
- Medium gray
- Light gray
- Very subtle grayscale gradients

Do not introduce colorful accent colors.

Use thin dividers, spacing and typography instead of heavy cards and shadows.
Avoid excessive rounded corners, glassmorphism, playful illustrations,
bright gradients and colorful story rings.

Use a modern grotesk or sans-serif typeface for UI text.
An editorial serif may be used sparingly for major page headings only.

Use an 8px spacing system.
Use 6px radius for compact controls, 8px for media, 12px for large modal
surfaces and circular avatars.

Use consistent monochrome outline icons.
Active states may become filled black.

RESPONSIVE SYSTEM

Desktop:
- Narrow collapsible navigation rail on the left
- Controlled central content width
- Optional contextual rail on the right
- Standard feed posts limited to approximately 680px
- Comments and secondary information may open in a right-side panel

Tablet:
- Collapse navigation to an icon rail
- Hide optional right rails
- Preserve readable centered content

Mobile:
- Compact top header
- Persistent bottom navigation
- Nearly full-width media
- 12px to 16px text padding
- Touch targets at least 44px
- No essential hover-only interactions

MOTION AND ACCESSIBILITY

Use subtle 150ms to 250ms transitions.
Support keyboard navigation, visible focus states, reduced motion and strong
contrast.

Create all important states:
- Default
- Hover
- Pressed
- Focus-visible
- Disabled
- Loading
- Empty
- Error
- Offline
- Permission denied
```

---

# PHẦN A — THIẾT KẾ TOÀN BỘ MÀN HÌNH

---

## 5. Authentication và Onboarding

### 5.1. Danh sách màn

- Welcome
- Sign in
- Sign up
- Verify email / phone
- Forgot password
- Reset password
- Choose username
- Add profile photo
- Add display name và bio
- Choose interests
- Suggested users
- Notification permission
- Onboarding completed

### 5.2. Prompt Stitch

```text
Create a responsive authentication and onboarding flow for the social
platform.

SCREENS

Include:
- Welcome
- Sign in
- Create account
- Verify email or phone
- Forgot password
- Reset password
- Choose username
- Add profile photo
- Add display name and biography
- Choose interests
- Suggested accounts
- Notification permission
- Completion screen

DESIGN

Use white or soft off-white surfaces, near-black typography, minimal form
styling and subtle grayscale imagery.

Desktop:
- Center the form in a controlled column
- An optional editorial brand panel may appear beside it

Mobile:
- Use full-width forms with 16px to 24px horizontal padding
- Keep primary actions reachable above the keyboard
- Use clear back navigation

Include:
- Form validation
- Password strength
- Username availability
- Verification resend timer
- Loading
- Network error
- Permission declined
- Save progress
```

### 5.3. Backend cần trả về

#### `AuthSession`

```json
{
  "accessToken": "jwt-access-token",
  "refreshToken": "jwt-refresh-token",
  "expiresAt": "2026-07-24T09:00:00Z",
  "user": {
    "id": "usr_01JXYZ",
    "username": "bach.dev",
    "displayName": "Bách",
    "avatarUrl": "https://cdn.example.com/avatar/usr_01JXYZ.webp",
    "onboardingCompleted": false
  }
}
```

#### `UsernameAvailability`

```json
{
  "username": "bach.dev",
  "available": true,
  "suggestions": ["bach.dev_", "bachdd", "bach.digital"]
}
```

#### `OnboardingConfig`

```json
{
  "requiredSteps": [
    "USERNAME",
    "PROFILE_PHOTO",
    "PROFILE_INFO",
    "INTERESTS"
  ],
  "optionalSteps": [
    "FOLLOW_SUGGESTIONS",
    "ENABLE_NOTIFICATIONS"
  ],
  "availableInterests": [
    {
      "id": "interest_tech",
      "name": "Technology",
      "slug": "technology"
    }
  ]
}
```

### 5.4. Endpoint gợi ý

```text
POST /api/v1/auth/sign-in
POST /api/v1/auth/sign-up
POST /api/v1/auth/refresh
POST /api/v1/auth/verify
POST /api/v1/auth/forgot-password
POST /api/v1/auth/reset-password
GET  /api/v1/users/username-availability?username=
GET  /api/v1/onboarding/config
PATCH /api/v1/onboarding/profile
PATCH /api/v1/onboarding/interests
POST /api/v1/onboarding/complete
```

---

## 6. Màn Trang chủ

### 6.1. Mục tiêu

Trang chủ có hai tab:

- **Khám phá**: tab mặc định
- **Bạn bè**: chỉ post của người follow lẫn nhau

Story nằm ngang phía trên feed.

### 6.2. Prompt Stitch

```text
Create the responsive home screen using the Editorial Rail interface.

INFORMATION ARCHITECTURE

Include:
- Global navigation
- Discover and Friends feed tabs
- Horizontal story timeline
- Single-column post feed
- Optional context rail on large desktop screens

FEED TABS

Use two tabs:
- Discover
- Friends

Discover is selected by default.

Friends contains only content from mutual follows.

Preserve the independent scroll position of both tabs.

Use an editorial index style instead of pill tabs:

01  DISCOVER     02  FRIENDS
────────────────────────────

The active tab uses near-black semibold text and a thin underline.
The inactive tab uses muted gray.

STORY TIMELINE

Place a horizontally scrollable story row below the feed tabs.

Use circular avatars connected by a thin visual timeline.
The current user’s story appears first.

States:
- Add story
- Unseen story: double black ring
- Seen story: light gray ring
- Muted story: reduced opacity

Do not use colorful gradients.

POST FEED

Use a single-column feed.

Desktop:
- Standard media width up to approximately 680px
- Author metadata may occupy a narrow left gutter
- Post actions may use a vertical action rail on the right
- Caption remains below the media
- Comments may open in a contextual right-side panel

Mobile:
- Author header above media
- Horizontal action row below media
- Caption below actions
- Nearly full-width media
- Persistent bottom navigation

POST VARIANTS

Use a controlled rhythm:
- 80 percent standard posts
- 15 percent featured media posts
- 5 percent text, collection or editorial posts

Create:
- Loading skeleton
- Empty Friends feed
- Offline state
- Failed post state
- End-of-feed state
```

### 6.3. Backend response tổng thể

#### `HomeFeedResponse`

```json
{
  "activeTab": "DISCOVER",
  "tabs": [
    {
      "id": "DISCOVER",
      "label": "Khám phá",
      "unreadCount": 0
    },
    {
      "id": "FRIENDS",
      "label": "Bạn bè",
      "unreadCount": 3
    }
  ],
  "storyTray": {
    "items": [],
    "nextCursor": null
  },
  "feed": {
    "items": [],
    "nextCursor": "cursor_post_020",
    "hasNext": true
  },
  "suggestedUsers": []
}
```

### 6.4. `FeedPost`

```json
{
  "id": "post_01JPOST",
  "layoutVariant": "STANDARD",
  "author": {
    "id": "usr_anna",
    "username": "anna.nguyen",
    "displayName": "Anna Nguyễn",
    "avatarUrl": "https://cdn.example.com/users/anna/avatar.webp",
    "isVerified": true,
    "relationship": {
      "viewerFollowsUser": true,
      "userFollowsViewer": true,
      "isFriend": true
    }
  },
  "createdAt": "2026-07-23T12:20:00Z",
  "editedAt": null,
  "visibility": "PUBLIC",
  "location": {
    "id": "loc_hanoi",
    "name": "Hà Nội, Việt Nam"
  },
  "caption": {
    "text": "Một ngày làm việc yên tĩnh.",
    "mentions": [],
    "hashtags": [
      {
        "tag": "daily",
        "start": 27,
        "end": 33
      }
    ],
    "translationAvailable": true
  },
  "media": [
    {
      "id": "media_001",
      "type": "IMAGE",
      "url": "https://cdn.example.com/posts/post_01JPOST/1.webp",
      "thumbnailUrl": "https://cdn.example.com/posts/post_01JPOST/1-thumb.webp",
      "width": 1440,
      "height": 1800,
      "aspectRatio": 0.8,
      "altText": "Không gian làm việc cạnh cửa sổ",
      "dominantColor": "#D8D8D3",
      "durationMs": null,
      "video": null
    },
    {
      "id": "media_002",
      "type": "VIDEO",
      "url": "https://cdn.example.com/posts/post_01JPOST/2.mp4",
      "thumbnailUrl": "https://cdn.example.com/posts/post_01JPOST/2-cover.webp",
      "width": 1080,
      "height": 1350,
      "aspectRatio": 0.8,
      "altText": "Video ngắn về bàn làm việc",
      "dominantColor": "#A7A7A2",
      "durationMs": 15400,
      "video": {
        "hlsUrl": "https://cdn.example.com/posts/post_01JPOST/2/master.m3u8",
        "hasAudio": true,
        "captionsUrl": null
      }
    }
  ],
  "engagement": {
    "likeCount": 204500,
    "commentCount": 138,
    "repostCount": 25300,
    "shareCount": 4310,
    "saveCount": 9870
  },
  "viewerState": {
    "liked": false,
    "saved": true,
    "reposted": false,
    "canComment": true,
    "canShare": true,
    "canEdit": false,
    "canDelete": false
  },
  "commentPreview": [
    {
      "id": "comment_01",
      "author": {
        "id": "usr_minh",
        "username": "minh.tran",
        "displayName": "Minh Trần",
        "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp",
        "isVerified": false
      },
      "text": "Góc làm việc đẹp quá.",
      "createdAt": "2026-07-23T12:30:00Z",
      "likeCount": 18,
      "viewerLiked": false
    }
  ],
  "recommendationContext": {
    "source": "INTEREST_MATCH",
    "label": "Đề xuất cho bạn"
  }
}
```

### 6.5. `StoryTrayItem`

```json
{
  "user": {
    "id": "usr_minh",
    "username": "minh.tran",
    "displayName": "Minh Trần",
    "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp"
  },
  "storyGroupId": "story_group_minh_20260723",
  "totalItems": 4,
  "seenItems": 1,
  "hasUnseen": true,
  "isMuted": false,
  "latestCreatedAt": "2026-07-23T14:00:00Z"
}
```

### 6.6. Endpoint gợi ý

```text
GET /api/v1/home?tab=DISCOVER&cursor=
GET /api/v1/home?tab=FRIENDS&cursor=
GET /api/v1/stories/tray?cursor=
GET /api/v1/users/suggestions?context=HOME
POST /api/v1/posts/{postId}/like
DELETE /api/v1/posts/{postId}/like
POST /api/v1/posts/{postId}/save
DELETE /api/v1/posts/{postId}/save
POST /api/v1/posts/{postId}/repost
DELETE /api/v1/posts/{postId}/repost
```

---

## 7. Component Post Card

### 7.1. Prompt Stitch

```text
Create a responsive social post component using an Editorial Rail layout.

POST WIDTH

Use fluid width with a strict maximum of approximately 680px for standard
posts.

DESKTOP

- Place compact author identity in a narrow left gutter or header
- Place media in the center
- Place like, comment, repost, share and save in a narrow vertical action rail
- Keep caption and comments preview below the media
- Use large whitespace between posts
- Optionally show a small editorial index such as POST 014

MOBILE

- Author header above media
- Media nearly full width
- Horizontal action row below media
- Caption and comments below
- Do not use side rails

MEDIA CAROUSEL

Support image and video in one carousel.

Use:
- Default 4:5 presentation
- Support 1:1, 4:5 and 16:9
- Previous and next buttons on desktop
- Horizontal swipe on touch devices
- Keyboard navigation
- Indicator such as FRAME 02 / 05
- Thin chapter progress bar
- No colorful dots

VIDEO

Include:
- Play and pause
- Mute and unmute
- Progress
- Full screen
- Captions when available
- Never autoplay with sound

INTERACTIONS

Use monochrome outline icons.
Liked, saved or reposted states become filled black.
Double-tap may like the post using a subtle grayscale ripple.

Include:
- Loading media
- Failed media
- Sensitive content cover
- Deleted content
- Disabled comments
```

### 7.2. Trường backend quan trọng cho post

```text
id
layoutVariant
author
createdAt
editedAt
visibility
location
caption
media[]
engagement
viewerState
commentPreview[]
recommendationContext
contentWarning
moderationState
```

---

## 8. Chi tiết bài viết và bình luận

### 8.1. Prompt Stitch

```text
Create a responsive post detail screen.

DESKTOP

- Use a prominent media viewer
- Show post information and comments in a contextual side panel
- Keep navigation and actions visible without crowding the media
- Allow the comment panel to scroll independently

MOBILE

- Use one vertical flow:
  author, media, actions, caption, comments and composer

COMMENTS

Support:
- Top-level comments
- Replies with limited visual nesting
- Like comment
- Reply
- Mention
- Sort by relevant or recent
- Load more replies
- Report
- Deleted or hidden comment state

COMMENT COMPOSER

Include:
- Viewer avatar
- Text input
- Mention support
- Submit action
- Sending, failed and retry states

Create:
- No comments
- Restricted comments
- Loading comments
- Offline comment queue
```

### 8.2. `PostDetailResponse`

```json
{
  "post": {},
  "comments": {
    "items": [],
    "sort": "RELEVANT",
    "nextCursor": "cursor_comment_30",
    "hasNext": true
  }
}
```

### 8.3. `Comment`

```json
{
  "id": "comment_100",
  "postId": "post_01JPOST",
  "parentCommentId": null,
  "author": {
    "id": "usr_minh",
    "username": "minh.tran",
    "displayName": "Minh Trần",
    "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp",
    "isVerified": false
  },
  "text": "Bố cục rất đẹp.",
  "mentions": [],
  "createdAt": "2026-07-23T13:00:00Z",
  "editedAt": null,
  "likeCount": 12,
  "replyCount": 3,
  "viewerLiked": false,
  "viewerPermissions": {
    "canReply": true,
    "canEdit": false,
    "canDelete": false,
    "canReport": true
  },
  "replyPreview": []
}
```

### 8.4. Endpoint

```text
GET  /api/v1/posts/{postId}
GET  /api/v1/posts/{postId}/comments?sort=RELEVANT&cursor=
POST /api/v1/posts/{postId}/comments
POST /api/v1/comments/{commentId}/replies
POST /api/v1/comments/{commentId}/like
DELETE /api/v1/comments/{commentId}/like
PATCH /api/v1/comments/{commentId}
DELETE /api/v1/comments/{commentId}
```

---

## 9. Trang cá nhân của người khác

### 9.1. Prompt Stitch

```text
Create a responsive public profile screen that feels like a personal
publication rather than an Instagram clone.

PROFILE HEADER

Include:
- Avatar
- Display name
- Username
- Optional monochrome verification badge
- Biography
- Website
- Location
- Mutual connection context
- Post, follower and following counts

Actions:
- Follow or Following
- Message
- More

When both users follow each other, show a subtle Friends status.

FEATURED STORIES

Show featured stories as curated monochrome collections.
Use compact covers, collection title and item count.

PROFILE NAVIGATION

Include:
- Overview
- Gallery
- Journal
- Tagged

Overview:
- Pinned posts
- Featured stories
- Recent highlights

Gallery:
- Structured image and video grid

Journal:
- Timeline-style posts

Tagged:
- Tagged content

PRIVATE PROFILE

Create a private state with:
- Private account message
- Follow action
- Mutual connection information

Also include:
- Follow request pending
- Blocked
- Restricted
- No posts
- Loading
- Profile unavailable
```

### 9.2. `UserProfile`

```json
{
  "id": "usr_anna",
  "username": "anna.nguyen",
  "displayName": "Anna Nguyễn",
  "avatarUrl": "https://cdn.example.com/users/anna/avatar.webp",
  "coverUrl": null,
  "bio": "Designer, reader and occasional photographer.",
  "website": "https://anna.example.com",
  "location": {
    "name": "Hà Nội, Việt Nam"
  },
  "isVerified": true,
  "accountVisibility": "PUBLIC",
  "stats": {
    "postCount": 128,
    "followerCount": 18320,
    "followingCount": 612,
    "friendCount": 190
  },
  "relationship": {
    "viewerFollowsUser": true,
    "userFollowsViewer": true,
    "isFriend": true,
    "followRequestStatus": "NONE",
    "isMuted": false,
    "isRestricted": false,
    "isBlockedByViewer": false
  },
  "mutualConnections": {
    "count": 12,
    "previewUsers": [
      {
        "id": "usr_minh",
        "username": "minh.tran",
        "displayName": "Minh Trần",
        "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp"
      }
    ]
  },
  "featuredStories": [],
  "pinnedPosts": [],
  "viewerPermissions": {
    "canViewPosts": true,
    "canMessage": true,
    "canTag": true,
    "canMention": true
  }
}
```

### 9.3. Endpoint

```text
GET /api/v1/users/{username}
GET /api/v1/users/{userId}/posts?tab=OVERVIEW&cursor=
GET /api/v1/users/{userId}/posts?tab=GALLERY&cursor=
GET /api/v1/users/{userId}/posts?tab=JOURNAL&cursor=
GET /api/v1/users/{userId}/tagged?cursor=
POST /api/v1/users/{userId}/follow
DELETE /api/v1/users/{userId}/follow
POST /api/v1/users/{userId}/mute
POST /api/v1/users/{userId}/restrict
POST /api/v1/users/{userId}/block
```

---

## 10. Trang cá nhân của tôi

### 10.1. Prompt Stitch

```text
Create the authenticated user’s own responsive profile.

Use the same publication-style profile structure as public profiles, but
prioritize management.

Include:
- Avatar
- Display name
- Username
- Bio
- Website
- Statistics
- Edit Profile
- Share Profile
- Settings

Do not show a Follow button.

FEATURED STORIES

Allow:
- Create collection
- Edit collection
- Reorder collection
- Change cover
- Delete collection

PROFILE TABS

Include:
- Overview
- Gallery
- Journal
- Tagged
- Saved, visible only to the owner

OWNER TOOLS

Support:
- Pin post
- Edit post
- Archive
- Delete
- Select multiple posts
- Open drafts
- Open archive

Create:
- No posts
- Profile completion suggestion
- Draft indicator
- Tagged content approval
- Loading and error states
```

### 10.2. `MyProfileResponse`

Kế thừa `UserProfile`, bổ sung:

```json
{
  "profile": {},
  "profileCompletion": {
    "percentage": 80,
    "missingFields": ["BIO", "WEBSITE"]
  },
  "privateCounters": {
    "draftCount": 4,
    "savedCount": 120,
    "archiveCount": 18,
    "pendingTagCount": 2
  },
  "capabilities": {
    "canCreatePost": true,
    "canCreateStory": true,
    "canUseReels": false,
    "canUseMessaging": false
  }
}
```

### 10.3. Endpoint

```text
GET   /api/v1/me/profile
PATCH /api/v1/me/profile
GET   /api/v1/me/content-summary
POST  /api/v1/me/profile/share-link
```

---

## 11. Edit Profile

### 11.1. Prompt Stitch

```text
Create a responsive Edit Profile screen.

Fields:
- Avatar
- Display name
- Username
- Biography
- Website
- Location
- Pronouns, optional
- Account category, optional
- Visibility

Desktop:
- Use a controlled two-column form where appropriate

Mobile:
- Use one column
- Keep Save action visible
- Warn before leaving with unsaved changes

Include:
- Avatar upload progress
- Username availability
- Field validation
- Save success
- Save failure
- Unsaved changes dialog
```

### 11.2. Backend object

```json
{
  "displayName": "Bách",
  "username": "bach.dev",
  "bio": "Building useful things.",
  "website": "https://bach.dev",
  "location": "Hà Nội, Việt Nam",
  "pronouns": null,
  "category": "CREATOR",
  "accountVisibility": "PUBLIC"
}
```

---

## 12. Followers, Following và Friends

### 12.1. Prompt Stitch

```text
Create a reusable responsive connections screen.

Tabs:
- Followers
- Following
- Friends

Friends are mutual follows.

Each row includes:
- Avatar
- Display name
- Username
- Mutual connection context
- Relationship action
- More options

Actions may include:
- Follow
- Following
- Follow back
- Friend
- Remove follower
- Pending

Include search, loading, empty and error states.

Desktop:
- Center the list in a controlled panel

Mobile:
- Use a full-screen list with sticky search
```

### 12.2. `ConnectionItem`

```json
{
  "user": {
    "id": "usr_minh",
    "username": "minh.tran",
    "displayName": "Minh Trần",
    "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp",
    "isVerified": false
  },
  "relationship": {
    "viewerFollowsUser": true,
    "userFollowsViewer": false,
    "isFriend": false,
    "followRequestStatus": "NONE"
  },
  "mutualConnectionsCount": 8
}
```

### 12.3. Endpoint

```text
GET /api/v1/users/{userId}/followers?query=&cursor=
GET /api/v1/users/{userId}/following?query=&cursor=
GET /api/v1/users/{userId}/friends?query=&cursor=
DELETE /api/v1/me/followers/{userId}
```

---

## 13. Search và Discover

### 13.1. Prompt Stitch

```text
Create a responsive Search and Discover experience.

SEARCH

Include:
- Sticky search input
- Recent searches
- Suggested searches
- User results
- Post results
- Hashtags or topics

DISCOVER

Do not use a random Instagram-style mosaic.

Use curated editorial sections:
- Discover This Week
- Featured Post
- People Worth Discovering
- New Perspectives
- Popular Conversations
- Recent Media

Use a controlled modular grid with a limited number of larger featured items.

Filters:
- For You
- People
- Photos
- Videos
- Recent

Include:
- Initial state
- Loading
- No results
- Search error
- Sensitive content placeholder
```

### 13.2. `SearchResponse`

```json
{
  "query": "design",
  "users": {
    "items": [],
    "nextCursor": null
  },
  "posts": {
    "items": [],
    "nextCursor": "cursor_posts"
  },
  "topics": [
    {
      "id": "topic_design",
      "name": "Design",
      "slug": "design",
      "postCount": 125400
    }
  ]
}
```

### 13.3. `DiscoverSection`

```json
{
  "id": "section_weekly",
  "type": "FEATURED_POSTS",
  "title": "Discover This Week",
  "subtitle": "A curated selection for you",
  "layout": "EDITORIAL_HERO_GRID",
  "items": []
}
```

### 13.4. Endpoint

```text
GET /api/v1/search?q=&type=ALL&cursor=
GET /api/v1/search/recent
DELETE /api/v1/search/recent
GET /api/v1/discover
GET /api/v1/discover?sectionId=&cursor=
```

---

## 14. Story Viewer

### 14.1. Prompt Stitch

```text
Create a responsive immersive story viewer.

Use a near-black full-screen environment.

Include:
- Segmented progress indicator
- Author avatar
- Username
- Timestamp
- Close
- More
- Previous and next navigation
- Pause and resume
- Mute and unmute
- Reply field when permitted
- Like action

Mobile:
- Tap left and right
- Swipe between users
- Hold to pause

Desktop:
- Keyboard navigation
- Visible but restrained side navigation controls

Do not use colorful reactions.

Create:
- Loading
- Story unavailable
- Deleted story
- Network interruption
- Reply disabled
- End of collection
```

### 14.2. `StoryGroupResponse`

```json
{
  "groupId": "story_group_minh_20260723",
  "owner": {
    "id": "usr_minh",
    "username": "minh.tran",
    "displayName": "Minh Trần",
    "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp"
  },
  "items": [
    {
      "id": "story_001",
      "type": "IMAGE",
      "mediaUrl": "https://cdn.example.com/stories/story_001.webp",
      "thumbnailUrl": "https://cdn.example.com/stories/story_001-thumb.webp",
      "width": 1080,
      "height": 1920,
      "durationMs": 5000,
      "createdAt": "2026-07-23T14:00:00Z",
      "expiresAt": "2026-07-24T14:00:00Z",
      "caption": null,
      "link": null,
      "viewerState": {
        "seen": false,
        "liked": false,
        "canReply": true
      }
    }
  ],
  "viewerPermissions": {
    "canReply": true,
    "canShare": true,
    "canReport": true
  }
}
```

### 14.3. Endpoint

```text
GET  /api/v1/stories/groups/{groupId}
POST /api/v1/stories/{storyId}/seen
POST /api/v1/stories/{storyId}/like
DELETE /api/v1/stories/{storyId}/like
POST /api/v1/stories/{storyId}/reply
```

---

## 15. Featured Story / Highlight

### 15.1. Prompt Stitch

```text
Create featured story collections that feel like curated publication
chapters.

Each collection includes:
- Cover image
- Title
- Item count
- Updated date

Public profile:
- Horizontal collection row
- Open in story viewer

Owner profile:
- Create
- Edit title
- Change cover
- Add or remove stories
- Reorder stories
- Delete collection

Use monochrome controls and restrained collection covers.
```

### 15.2. `FeaturedStoryCollection`

```json
{
  "id": "highlight_01",
  "ownerId": "usr_anna",
  "title": "Projects",
  "coverUrl": "https://cdn.example.com/highlights/highlight_01-cover.webp",
  "itemCount": 12,
  "updatedAt": "2026-07-20T10:00:00Z",
  "previewItems": [
    {
      "storyId": "story_archived_01",
      "thumbnailUrl": "https://cdn.example.com/stories/archive/01-thumb.webp"
    }
  ]
}
```

### 15.3. Endpoint

```text
GET    /api/v1/users/{userId}/featured-stories
POST   /api/v1/me/featured-stories
PATCH  /api/v1/me/featured-stories/{collectionId}
DELETE /api/v1/me/featured-stories/{collectionId}
POST   /api/v1/me/featured-stories/{collectionId}/items
DELETE /api/v1/me/featured-stories/{collectionId}/items/{storyId}
```

---

## 16. Create Entry

### 16.1. Prompt Stitch

```text
Create a responsive global Create entry screen.

Options:
- Post
- Story

Reserve a disabled or future-ready Reel option without presenting it as
available.

Desktop:
- Use a centered compact modal

Mobile:
- Use a bottom sheet or full-height creation sheet

Each option includes:
- Monochrome icon
- Title
- One-line description

Include:
- Close
- Focus management
- Disabled capability
- Permission denied
```

### 16.2. Backend

```json
{
  "creationCapabilities": {
    "post": {
      "enabled": true,
      "maxMediaItems": 10
    },
    "story": {
      "enabled": true,
      "maxDurationSeconds": 60
    },
    "reel": {
      "enabled": false
    }
  }
}
```

---

## 17. Tạo bài viết

### 17.1. Prompt Stitch

```text
Create a responsive multi-step post creation flow.

STEP 1: SELECT MEDIA

Support:
- Photos
- Videos
- Multiple selection
- Drag and drop on desktop
- Camera or gallery on mobile

STEP 2: EDIT MEDIA

Support:
- Reorder
- Remove
- Crop
- Rotate
- Select 1:1, 4:5 or 16:9
- Choose video cover
- Trim video when supported
- Add alternative text

STEP 3: DETAILS

Include:
- Caption
- Mentions
- Hashtags
- Location
- Collaborator
- Visibility
- Comment permissions
- Hide engagement count
- Sensitive content label

STEP 4: REVIEW

Include:
- Preview
- Carousel order
- Caption
- Visibility
- Publish
- Save draft

Desktop:
- Use a large modal with preview and controls beside it

Mobile:
- Use full-screen steps
- Preserve progress
- Keep primary action reachable

Create:
- Uploading
- Processing
- Publish success
- Publish failure
- Save draft
- Unsaved changes warning
```

### 17.2. `CreatePostRequest`

```json
{
  "mediaIds": ["upload_media_001", "upload_media_002"],
  "caption": "Một ngày mới.",
  "mentions": ["usr_minh"],
  "hashtags": ["daily"],
  "locationId": "loc_hanoi",
  "visibility": "PUBLIC",
  "commentPermission": "EVERYONE",
  "hideEngagementCount": false,
  "collaboratorIds": [],
  "sensitiveContent": false
}
```

### 17.3. `MediaUploadSession`

```json
{
  "uploadId": "upload_media_001",
  "uploadUrl": "https://upload.example.com/presigned-url",
  "expiresAt": "2026-07-23T16:00:00Z",
  "maxSizeBytes": 104857600,
  "allowedMimeTypes": [
    "image/jpeg",
    "image/png",
    "image/webp",
    "video/mp4"
  ]
}
```

### 17.4. Endpoint

```text
POST /api/v1/uploads
POST /api/v1/uploads/{uploadId}/complete
POST /api/v1/posts
POST /api/v1/posts/drafts
PATCH /api/v1/posts/drafts/{draftId}
GET /api/v1/posts/publish-status/{jobId}
```

---

## 18. Tạo Story

### 18.1. Prompt Stitch

```text
Create a responsive story creation screen.

INPUT

Allow:
- Camera
- Video recording
- Device media
- Multiple story items

EDITOR

Include:
- Text
- Mention
- Link
- Location
- Drawing
- Crop
- Mute
- Background adjustment
- Alternative text

Keep editor controls monochrome.
User-generated media may contain color.

PUBLISH

Include:
- Share to Story
- Share to selected friends when supported
- Save draft
- Add to featured story after publishing

Create:
- Camera permission denied
- Microphone permission denied
- Uploading
- Processing
- Publish failure
- Unsaved changes
```

### 18.2. `CreateStoryRequest`

```json
{
  "mediaId": "upload_story_001",
  "durationMs": 5000,
  "visibility": "FOLLOWERS",
  "allowedViewerListId": null,
  "caption": null,
  "link": null,
  "mentions": [],
  "locationId": null,
  "allowReplies": true
}
```

---

## 19. Notifications

### 19.1. Prompt Stitch

```text
Create a responsive notifications screen.

GROUPS

Group notifications by:
- Today
- This week
- Earlier

TYPES

Support:
- Like
- Comment
- Reply
- Follow
- Follow back
- Mutual friendship created
- Mention
- Tag
- Story interaction
- Featured story interaction
- Post shared
- Security
- System

Each row includes:
- Actor avatar or system icon
- Clear sentence
- Timestamp
- Content thumbnail when relevant
- Contextual action

Actions:
- Follow back
- Accept
- View comment
- Review tag
- Open post

Unread state uses typography weight and subtle surface contrast.
Do not rely on colored dots alone.

Optional filters:
- All
- Interactions
- Connections
- System

Include:
- Empty
- Loading
- Error
- Mark all as read
- Removed source content
```

### 19.2. `NotificationItem`

```json
{
  "id": "notif_001",
  "type": "POST_LIKED",
  "createdAt": "2026-07-23T14:30:00Z",
  "readAt": null,
  "actor": {
    "id": "usr_minh",
    "username": "minh.tran",
    "displayName": "Minh Trần",
    "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp"
  },
  "message": {
    "template": "{actor} đã thích bài viết của bạn",
    "renderedText": "Minh Trần đã thích bài viết của bạn"
  },
  "target": {
    "type": "POST",
    "id": "post_01JPOST",
    "thumbnailUrl": "https://cdn.example.com/posts/post_01JPOST/thumb.webp",
    "deepLink": "/posts/post_01JPOST"
  },
  "action": null
}
```

### 19.3. Endpoint

```text
GET /api/v1/notifications?filter=ALL&cursor=
POST /api/v1/notifications/{notificationId}/read
POST /api/v1/notifications/read-all
GET /api/v1/notifications/unread-count
```

---

## 20. Saved, Collections, Drafts và Archive

### 20.1. Prompt Stitch

```text
Create a private content library for the authenticated user.

Sections:
- Saved
- Collections
- Drafts
- Archive

SAVED

Allow:
- View saved posts
- Create collection
- Move items
- Remove items

DRAFTS

Show:
- Thumbnail
- Draft type
- Last edited time
- Continue editing
- Delete

ARCHIVE

Show:
- Archived posts
- Archived stories
- Restore
- Delete permanently

Use a clean grid or list depending on content type.

Include:
- Empty
- Loading
- Error
- Selection mode
- Bulk actions
- Private-only indicator
```

### 20.2. `SavedCollection`

```json
{
  "id": "collection_design",
  "name": "Design",
  "coverThumbnailUrls": [
    "https://cdn.example.com/posts/1-thumb.webp",
    "https://cdn.example.com/posts/2-thumb.webp"
  ],
  "itemCount": 28,
  "createdAt": "2026-06-01T10:00:00Z",
  "updatedAt": "2026-07-20T12:00:00Z"
}
```

### 20.3. `DraftSummary`

```json
{
  "id": "draft_001",
  "type": "POST",
  "thumbnailUrl": "https://cdn.example.com/drafts/draft_001.webp",
  "mediaCount": 3,
  "captionPreview": "Nội dung đang viết...",
  "updatedAt": "2026-07-22T19:00:00Z"
}
```

### 20.4. Endpoint

```text
GET /api/v1/me/saved?cursor=
GET /api/v1/me/saved/collections
POST /api/v1/me/saved/collections
POST /api/v1/me/saved/collections/{collectionId}/items
DELETE /api/v1/me/saved/collections/{collectionId}/items/{postId}
GET /api/v1/me/drafts
GET /api/v1/me/archive?type=POST
POST /api/v1/me/archive/{contentId}/restore
```

---

## 21. Settings

### 21.1. Prompt Stitch

```text
Create a responsive settings experience.

DESKTOP

Use:
- Settings navigation on the left
- Selected content on the right
- Controlled reading width

MOBILE

Use:
- Hierarchical list
- Separate category screens
- Clear back navigation

CATEGORIES

Account:
- Edit profile
- Username
- Email
- Phone
- Password
- Deactivate
- Delete

Privacy:
- Public or private account
- Story visibility
- Comment permissions
- Mention permissions
- Tag approval
- Activity status
- Read receipts

Connections:
- Blocked users
- Muted users
- Restricted users
- Removed followers
- Follow requests

Notifications:
- Likes
- Comments
- Follows
- Mentions
- Stories
- Messages
- Security
- Email
- Push

Content:
- Saved
- Archive
- Drafts
- Hidden content
- Sensitive content

Appearance:
- Light
- Dark
- System
- Reduced motion

Accessibility:
- Text size
- High contrast
- Captions
- Alternative text
- Reduce animation

Security:
- Active sessions
- Login activity
- Two-factor authentication
- Trusted devices

Help:
- Help center
- Report a problem
- Community guidelines
- Terms
- Privacy policy

Use confirmation dialogs for destructive actions.

Include:
- Saving
- Saved
- Error
- Offline
- Unsaved changes
```

### 21.2. `UserSettings`

```json
{
  "account": {
    "email": "bach@example.com",
    "phone": "+84901234567",
    "accountStatus": "ACTIVE"
  },
  "privacy": {
    "accountVisibility": "PUBLIC",
    "storyVisibility": "FOLLOWERS",
    "commentPermission": "EVERYONE",
    "mentionPermission": "EVERYONE",
    "tagApprovalRequired": true,
    "activityStatusVisible": false,
    "readReceiptsEnabled": true
  },
  "notifications": {
    "pushEnabled": true,
    "emailEnabled": false,
    "likes": true,
    "comments": true,
    "follows": true,
    "mentions": true,
    "stories": false,
    "messages": true,
    "security": true
  },
  "content": {
    "sensitiveContentLevel": "STANDARD",
    "autoplayVideo": "WIFI_ONLY"
  },
  "appearance": {
    "theme": "SYSTEM",
    "reducedMotion": false
  },
  "accessibility": {
    "textScale": 1.0,
    "highContrast": false,
    "alwaysShowCaptions": false
  }
}
```

### 21.3. Endpoint

```text
GET   /api/v1/me/settings
PATCH /api/v1/me/settings/privacy
PATCH /api/v1/me/settings/notifications
PATCH /api/v1/me/settings/content
PATCH /api/v1/me/settings/appearance
PATCH /api/v1/me/settings/accessibility
GET   /api/v1/me/security/sessions
DELETE /api/v1/me/security/sessions/{sessionId}
POST  /api/v1/me/deactivate
DELETE /api/v1/me
```

---

## 22. Follow Requests, Blocked, Muted và Restricted

### 22.1. Prompt Stitch

```text
Create reusable relationship management screens.

Include:
- Follow requests
- Blocked users
- Muted users
- Restricted users
- Removed followers

Each row includes:
- Avatar
- Display name
- Username
- Relationship status
- Contextual actions

Actions:
- Accept
- Decline
- Unblock
- Unmute
- Unrestrict
- Follow
- Remove

Use clear confirmation for block and remove actions.

Include:
- Search
- Empty
- Loading
- Error
```

### 22.2. Backend

Dùng `ConnectionItem` và bổ sung:

```json
{
  "requestId": "follow_req_001",
  "requestedAt": "2026-07-22T10:00:00Z",
  "status": "PENDING"
}
```

---

## 23. Share, Report, Block và các Bottom Sheet / Dialog

### 23.1. Prompt Stitch

```text
Create reusable responsive dialogs and bottom sheets for:

- Share post
- Copy link
- Send to friend
- Add to collection
- Report content
- Report account
- Block
- Restrict
- Mute
- Unfollow
- Delete post
- Archive post
- Confirm destructive action

Desktop:
- Use compact centered dialogs

Mobile:
- Use bottom sheets

Every dialog must include:
- Clear title
- Short consequence text
- Primary and secondary actions
- Loading
- Failure
- Keyboard focus management

Destructive actions use near-black or strong text weight rather than bright
red as the main visual language, while still remaining clearly identifiable.
```

### 23.2. `ReportReason`

```json
{
  "id": "SPAM",
  "label": "Spam",
  "description": "Nội dung lặp lại, quảng cáo hoặc gây phiền."
}
```

### 23.3. Endpoint

```text
GET  /api/v1/reports/reasons?targetType=POST
POST /api/v1/reports
POST /api/v1/users/{userId}/block
POST /api/v1/users/{userId}/restrict
POST /api/v1/users/{userId}/mute
```

---

## 24. Full-screen Media Viewer

### 24.1. Prompt Stitch

```text
Create a responsive full-screen media viewer.

Support:
- Images
- Videos
- Carousel
- Zoom
- Pan
- Keyboard navigation
- Swipe
- Download when allowed
- Share
- Close

Use a near-black background.
Keep controls minimal and fade them when idle.

Include:
- Loading
- Failed media
- Unsupported format
- Restricted content
```

### 24.2. Backend

Tận dụng `Post.media[]`, thêm:

```json
{
  "downloadAllowed": false,
  "originalUrl": null,
  "viewerWatermarkRequired": false
}
```

---

## 25. Offline, Empty, Error và System Screens

### 25.1. Prompt Stitch

```text
Create a consistent set of system states:

- Offline
- Session expired
- Account disabled
- Content unavailable
- Deleted post
- Deleted story
- Private content
- Permission denied
- Rate limited
- Maintenance
- 404
- 500
- Empty feed
- Empty notifications
- Empty saved content
- Empty search

Use restrained monochrome illustration or typography.
Always provide one clear recovery action.
Avoid playful error cartoons.
```

### 25.2. Error contract

```json
{
  "error": {
    "code": "POST_NOT_FOUND",
    "message": "Bài viết không tồn tại hoặc đã bị xóa.",
    "details": null,
    "requestId": "req_01JERROR",
    "retryable": false
  }
}
```

---

## 26. Reel — giai đoạn sau

### 26.1. Prompt Stitch

```text
Create a future responsive short-form vertical video experience.

Do not copy TikTok or Instagram Reels directly.

Use:
- Full-height portrait video
- Vertical navigation
- Minimal monochrome actions
- Author
- Caption
- Like
- Comment
- Repost
- Share
- Save

Desktop:
- Center the portrait viewer
- Use optional context beside it
- Do not stretch video across the entire screen

Mobile:
- Use immersive full-screen video
- Respect safe areas

Tabs may include:
- Discover
- Friends

Include:
- Buffering
- Video unavailable
- Restricted content
- End of feed
- Comment drawer
```

### 26.2. `ReelItem`

```json
{
  "id": "reel_001",
  "author": {},
  "video": {
    "hlsUrl": "https://cdn.example.com/reels/reel_001/master.m3u8",
    "thumbnailUrl": "https://cdn.example.com/reels/reel_001/cover.webp",
    "width": 1080,
    "height": 1920,
    "durationMs": 28500,
    "hasAudio": true,
    "captionsUrl": null
  },
  "caption": {
    "text": "A short update.",
    "mentions": [],
    "hashtags": []
  },
  "engagement": {
    "likeCount": 1200,
    "commentCount": 42,
    "repostCount": 18,
    "shareCount": 31,
    "saveCount": 55
  },
  "viewerState": {
    "liked": false,
    "saved": false,
    "reposted": false
  }
}
```

---

## 27. Messaging — giai đoạn sau

### 27.1. Prompt Stitch

```text
Create a future responsive messaging experience.

DESKTOP

Use three areas when space allows:
- Conversation list
- Active conversation
- Conversation details

TABLET

Use:
- Conversation list
- Active conversation
- Collapsible details

MOBILE

Use separate routes:
- Inbox
- Conversation
- Conversation details

INBOX

Include:
- Search
- New message
- Unread state
- Last message
- Timestamp
- Online status when privacy allows

CONVERSATION

Support:
- Text
- Image
- Video
- Shared post
- Shared profile
- Reply
- Reactions
- Read receipts
- Typing
- Message requests
- Block
- Report

Use restrained monochrome message surfaces.

Include:
- Empty inbox
- Request
- Sending
- Failed send
- Offline queue
- Deleted message
- Blocked conversation
```

### 27.2. `ConversationSummary`

```json
{
  "id": "conv_001",
  "type": "DIRECT",
  "participants": [
    {
      "id": "usr_minh",
      "username": "minh.tran",
      "displayName": "Minh Trần",
      "avatarUrl": "https://cdn.example.com/users/minh/avatar.webp"
    }
  ],
  "lastMessage": {
    "id": "msg_100",
    "type": "TEXT",
    "preview": "Hẹn gặp bạn ngày mai.",
    "senderId": "usr_minh",
    "createdAt": "2026-07-23T15:00:00Z"
  },
  "unreadCount": 2,
  "muted": false,
  "updatedAt": "2026-07-23T15:00:00Z"
}
```

### 27.3. `Message`

```json
{
  "id": "msg_100",
  "conversationId": "conv_001",
  "senderId": "usr_minh",
  "type": "TEXT",
  "text": "Hẹn gặp bạn ngày mai.",
  "attachments": [],
  "replyTo": null,
  "createdAt": "2026-07-23T15:00:00Z",
  "editedAt": null,
  "deletedAt": null,
  "delivery": {
    "status": "READ",
    "readBy": [
      {
        "userId": "usr_01JXYZ",
        "readAt": "2026-07-23T15:02:00Z"
      }
    ]
  }
}
```

---

# PHẦN B — HỢP ĐỒNG DỮ LIỆU BACKEND DÙNG CHUNG

---

## 28. Enum đề xuất

### User và relationship

```text
AccountVisibility:
- PUBLIC
- PRIVATE

FollowRequestStatus:
- NONE
- PENDING
- ACCEPTED
- DECLINED

RelationshipType:
- NONE
- FOLLOWING
- FOLLOWED_BY
- FRIEND

AccountStatus:
- ACTIVE
- DEACTIVATED
- SUSPENDED
- DELETED
```

### Content

```text
MediaType:
- IMAGE
- VIDEO

PostLayoutVariant:
- STANDARD
- FEATURED
- TEXT
- COLLECTION

PostVisibility:
- PUBLIC
- FOLLOWERS
- FRIENDS
- ONLY_ME

CommentPermission:
- EVERYONE
- FOLLOWERS
- FRIENDS
- NOBODY

StoryVisibility:
- PUBLIC
- FOLLOWERS
- FRIENDS
- CUSTOM
```

### Notification

```text
NotificationType:
- POST_LIKED
- POST_COMMENTED
- COMMENT_REPLIED
- USER_FOLLOWED
- FOLLOW_BACK
- FRIEND_CREATED
- USER_MENTIONED
- USER_TAGGED
- STORY_LIKED
- STORY_REPLIED
- FEATURED_STORY_UPDATED
- POST_SHARED
- SECURITY_ALERT
- SYSTEM
```

### Upload

```text
UploadStatus:
- CREATED
- UPLOADING
- PROCESSING
- READY
- FAILED
```

---

## 29. UserSummary dùng chung

```json
{
  "id": "usr_01JXYZ",
  "username": "bach.dev",
  "displayName": "Bách",
  "avatarUrl": "https://cdn.example.com/avatar.webp",
  "isVerified": false
}
```

Nên dùng object rút gọn này trong:

- Author post
- Comment
- Notification
- Search result
- Story
- Message participants
- Mutual connections

---

## 30. MediaAsset dùng chung

```json
{
  "id": "media_001",
  "type": "IMAGE",
  "url": "https://cdn.example.com/media/001.webp",
  "thumbnailUrl": "https://cdn.example.com/media/001-thumb.webp",
  "width": 1440,
  "height": 1800,
  "aspectRatio": 0.8,
  "altText": "Mô tả ảnh",
  "dominantColor": "#D8D8D3",
  "blurHash": "LEHV6nWB2yk8pyo0adR*.7kCMdnj",
  "durationMs": null,
  "video": null
}
```

Frontend cần:

- `width`, `height`, `aspectRatio` để tránh layout shift
- `thumbnailUrl` để preload
- `dominantColor` hoặc `blurHash` cho placeholder
- `altText` cho accessibility
- `durationMs`, `hlsUrl`, `hasAudio` cho video

---

## 31. Pagination chuẩn

Dùng cursor pagination.

```json
{
  "items": [],
  "nextCursor": "cursor_abc",
  "hasNext": true
}
```

Không nên trả page number cho feed vì nội dung thay đổi liên tục.

---

## 32. Meta và request tracing

```json
{
  "data": {},
  "meta": {
    "requestId": "req_01JABC",
    "serverTime": "2026-07-23T15:00:00Z"
  }
}
```

---

## 33. Error response chuẩn

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Dữ liệu không hợp lệ.",
    "details": [
      {
        "field": "username",
        "reason": "USERNAME_ALREADY_EXISTS"
      }
    ],
    "requestId": "req_01JABC",
    "retryable": false
  }
}
```

### Một số error code nên có

```text
UNAUTHORIZED
FORBIDDEN
VALIDATION_ERROR
RATE_LIMITED
USER_NOT_FOUND
POST_NOT_FOUND
STORY_NOT_FOUND
PROFILE_PRIVATE
FOLLOW_REQUEST_REQUIRED
COMMENTS_DISABLED
MEDIA_PROCESSING
MEDIA_UPLOAD_FAILED
CONTENT_REMOVED
ACCOUNT_SUSPENDED
SESSION_EXPIRED
NETWORK_UNAVAILABLE
```

---

## 34. Realtime event đề xuất

Dùng WebSocket hoặc SSE cho:

```text
notification.created
notification.read
post.engagement.updated
comment.created
comment.deleted
follow.created
follow.removed
friend.created
story.created
story.seen
message.created
message.read
conversation.updated
```

Ví dụ:

```json
{
  "event": "notification.created",
  "id": "evt_001",
  "occurredAt": "2026-07-23T15:00:00Z",
  "data": {
    "notification": {}
  }
}
```

---

## 35. Nguyên tắc dữ liệu cho frontend

Backend nên luôn trả:

1. **Viewer state**  
   Ví dụ `liked`, `saved`, `viewerFollowsUser`, `canComment`.

2. **Viewer permissions**  
   Frontend không nên tự suy luận quyền từ nhiều field rời rạc.

3. **Display-ready counts**  
   Trả số nguyên gốc, frontend tự format `204.5K`.

4. **Media dimensions trước khi tải file**  
   Giúp tránh layout shift.

5. **Deep link hoặc target type**  
   Đặc biệt cho notification, search và share.

6. **Explicit empty state reason**  
   Ví dụ Friends feed trống vì chưa có bạn bè hay vì không có bài mới.

Ví dụ:

```json
{
  "emptyReason": "NO_MUTUAL_FRIENDS",
  "suggestedAction": {
    "type": "OPEN_DISCOVER",
    "label": "Khám phá người mới"
  }
}
```

7. **Capabilities từ backend**  
   Không hardcode feature rollout ở frontend.

```json
{
  "capabilities": {
    "messaging": false,
    "reels": false,
    "storyLinks": true,
    "collaborativePosts": false
  }
}
```

---

# PHẦN C — ĐỀ XUẤT THỨ TỰ TRIỂN KHAI

## 36. Phase 1 — MVP

1. Authentication
2. User profile
3. Follow và friend relationship
4. Home Discover / Friends
5. Post image/video/carousel
6. Like, comment, save, share link
7. Story
8. Featured Story
9. Search user và post
10. Notifications
11. Settings cơ bản
12. Saved, drafts và archive

## 37. Phase 2

1. Repost
2. Advanced Discover sections
3. Profile Journal
4. Tag approval
5. Advanced moderation
6. Collaborative post
7. Rich story editor

## 38. Phase 3

1. Messaging
2. Reel
3. Realtime presence
4. Advanced recommendation system
5. Creator analytics

---

# PHẦN D — PROMPT TỔNG ĐỂ STITCH SINH CẢ PROJECT

```text
Create a complete responsive social media product using the Editorial Rail
design system described in this project.

Generate a coherent component system and all major product screens:

1. Authentication and onboarding
2. Home with Discover and Friends tabs
3. Horizontal story timeline
4. Standard, featured, text and collection post variants
5. Post detail and comments
6. Public profile
7. Own profile
8. Edit profile
9. Followers, Following and Friends
10. Search
11. Curated Discover
12. Story viewer
13. Featured story collections
14. Create entry
15. Create post flow
16. Create story flow
17. Notifications
18. Saved, collections, drafts and archive
19. Settings
20. Follow requests and relationship management
21. Share, report, mute, restrict and block dialogs
22. Full-screen media viewer
23. Offline, error, empty and permission states
24. Future Reel screens
25. Future Messaging screens

The product must remain easy to use through familiar interaction patterns,
but the visual composition must not resemble Instagram.

Use:
- Professional premium social direction
- Monochrome black, white and gray palette
- Editorial typography
- Large controlled media
- Generous whitespace
- Thin dividers
- Narrow navigation rail
- Contextual side panels on desktop
- Mobile bottom navigation
- Responsive behavior from the beginning
- Accessible states
- Loading, empty, error and offline variants

Create reusable components:
- App navigation
- Feed tabs
- Story timeline
- User identity row
- Post media carousel
- Vertical and horizontal action bars
- Caption block
- Comment item
- Profile header
- Featured story collection
- User connection row
- Notification row
- Search result item
- Settings row
- Modal
- Bottom sheet
- Empty state
- Error state
- Skeleton loader

For every screen, show realistic example content and demonstrate desktop,
tablet and mobile layouts.
```

---

## 39. Ghi chú cho Codex/MCP

Khi chuyển thiết kế từ Stitch sang code:

- Tách component theo domain, không tách chỉ theo trang
- Post dùng chung một model nhưng hỗ trợ variant
- Desktop và mobile có thể thay đổi composition nhưng dùng cùng data
- Dùng CSS container queries hoặc responsive primitives khi phù hợp
- Media component phải xử lý image và video độc lập
- Không tải video autoplay ngoài viewport
- Giữ scroll position cho Discover và Friends
- Dùng optimistic update cho like, save, follow
- Rollback khi API thất bại
- Dùng cursor pagination
- Cache profile, post detail và story tray
- WebSocket/SSE chỉ là lớp bổ sung, không thay REST fallback
- Không để frontend tự suy luận permission từ nhiều field

---

## 40. Checklist trước khi bắt đầu code

- [ ] Xác nhận tên ứng dụng và logo
- [ ] Xác nhận font chính
- [ ] Xác nhận breakpoint
- [ ] Xác nhận max-width của standard post
- [ ] Xác nhận các post variant
- [ ] Xác nhận visibility rule
- [ ] Xác nhận friend = mutual follow
- [ ] Xác nhận giới hạn media mỗi post
- [ ] Xác nhận thời lượng story
- [ ] Xác nhận comment nesting depth
- [ ] Xác nhận API pagination
- [ ] Xác nhận CDN và video processing
- [ ] Xác nhận feature flags cho Messaging và Reel
- [ ] Xác nhận moderation workflow
- [ ] Xác nhận accessibility baseline
