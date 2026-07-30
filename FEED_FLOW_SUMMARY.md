# Tổng hợp luồng Feed

Tài liệu này tổng hợp theo code hiện tại trong module `feed`, `post`, `user` và `audit`. Lưu ý: code không có bảng SQL tên `feed`; “bảng feed” đang được hiện thực bằng Redis Sorted Set theo từng user.

## Thành phần chính

- `FeedController`: expose API `GET /feed` và `POST /feed/long-term-vectors/refresh`.
- `FeedService`: đọc, refill, hydrate và cập nhật Redis feed.
- `FeedEventListener`: nghe Kafka event để cập nhật feed và vector.
- `FeedVectorService`: cập nhật short-term vector và build query vector cho recommendation.
- `FeedLongTermVectorService`: cập nhật long-term vector theo audit log, chạy cron hằng ngày hoặc gọi API thủ công.
- `PostFeedQueryService`: lấy post mới nhất từ MySQL/R2DBC và tìm post đề xuất từ Elasticsearch.

## Redis key liên quan

- `feed:<userId>`: Sorted Set chứa `postId`, score là thời điểm event tính bằng epoch millis. TTL 5 ngày.
- `seen_post:<userId>`: List các post đã trả về cho user. Giữ tối đa 1000 post, TTL 5 ngày.
- `post_details:<postId>`: cache chi tiết post đã hydrate cho feed. TTL 1 ngày.
- `user_short_term_vector:<userId>`: vector sở thích ngắn hạn. TTL 1 ngày.
- `user_long_term_vector_snapshot:<userId>`: snapshot tạm khi refresh long-term vector. TTL 6 giờ.

## Luồng cập nhật feed khi có post mới

### 1. Tạo post text

1. Client gọi tạo post.
2. `PostService.createPost()` lưu `post_details` với `validateStatus = APPROVED` nếu post không có media.
3. Service gửi SSE `post_upload` cho chủ post.
4. Service publish Kafka topic `post_upload_event` với `post_id`, `userId`, `content`, `hashtag`.
5. `PostEventBroadcast` cũng nghe `post_upload_event` để tạo embedding cho post qua `PostVectorService.processPostEmbedding()`.
6. `FeedEventListener.handlePostUploadEvent()` nghe cùng topic, lấy `postId` và `userId`.
7. `UserFollowerService.getFollowerIdsForFeedBroadcast(userId)` lấy toàn bộ follower theo page 500.
8. Với từng follower, `FeedService.appendPostToUserFeed(followerId, postId, now)` ghi vào `feed:<followerId>` bằng Redis ZSet và set TTL 5 ngày.

### 2. Tạo post có media

1. `PostService.createPost()` lưu post với `validateStatus = PENDING_SCAN`.
2. Service ghi `wait_for_upload_post:<postId>` để nhớ user tạo post, TTL 1 giờ.
3. Service publish `check_media_event`.
4. `ImageScanWorker.handlePostScanEvent()` scan media.
5. Nếu scan fail: xóa post, xóa media Cloudinary, gửi SSE failed, không publish `post_upload_event`, nên feed không được cập nhật.
6. Nếu scan pass: update post thành `APPROVED`, lưu metadata media, gửi SSE success, publish `post_upload_event`.
7. Từ đây luồng giống post text: tạo embedding và broadcast `postId` vào `feed:<followerId>`.

## Luồng cập nhật vector từ tương tác

### Like

1. `LikeService.like()` toggle like.
2. Khi tạo like mới, service ghi DB, cập nhật cache count `post_like_count:<postId>`, rồi publish `like_event`.
3. `FeedEventListener.handleLikeEvent()` nghe `like_event`, resolve `actorId` và `postId`.
4. Listener publish tiếp topic `user_interaction_events` với action `LIKE`.
5. `FeedEventListener.handleUserInteractionEvent()` nghe topic này và gọi `FeedVectorService.updateShortTermVector(userId, postId, action)`.
6. `FeedVectorService` lấy vector cũ từ `user_short_term_vector:<userId>` và post vector từ Elasticsearch, tính vector mới:
   - decay cũ: `0.7`
   - weight like: `0.3`
   - sau đó normalize và lưu Redis TTL 1 ngày.

### Comment

1. `CommentService.createComment()` tạo comment.
2. Comment text publish `comment_success_event` ngay; comment có media chờ `ImageScanWorker` duyệt rồi mới publish.
3. `FeedEventListener.handleCommentSuccessEvent()` nghe event, publish `user_interaction_events` với action `COMMENT`.
4. `FeedVectorService.updateShortTermVector()` cập nhật short-term vector giống luồng like, nhưng weight comment là `0.7`.

## Luồng cập nhật long-term vector

1. `FeedLongTermVectorService.updateYesterdayLongTermVectors()` chạy mỗi ngày lúc `02:00` theo zone `Asia/Ho_Chi_Minh`.
2. Service query `audit_logs` trong ngày hôm trước qua `AuditInteractionQueryService`.
3. Repository chỉ lấy log `SUCCESS` có action `LIKE_POST` hoặc `COMMENT_POST`.
4. Log được group theo `actorId`.
5. Mỗi user được tính `todayVector` bằng cách lấy post vector của các post đã tương tác:
   - `LIKE_POST`: weight `0.2`
   - `COMMENT_POST`: weight `0.3`
6. Vector trong ngày được normalize, rồi blend với vector cũ:
   - long-term cũ: `0.7`
   - today vector: `0.3`
7. Kết quả lưu vào `UserDetailVector.userLongTermVector`.
8. Có thể chạy thủ công qua `POST /feed/long-term-vectors/refresh?from=...&to=...&userId=...`.

## Luồng lấy feed

API: `GET /feed?userId=<id>&limit=20`

1. `FeedController.getFeed()` gọi `FeedService.getFeed(userId, limit)`.
2. `FeedService` validate `userId`; `limit <= 0` dùng mặc định 20, tối đa 50.
3. Service đọc `seen_post:<userId>` để tránh trả lại post đã xem.
4. Service đọc `feed:<userId>` bằng `reverseRange(0, 79)`, lọc post rỗng, lọc seen, distinct, lấy đủ `limit`.
5. Nếu số post trong cache chưa đủ `limit`, service refill:
   - build query vector từ long-term vector, short-term vector và fallback user vector.
   - tìm post đề xuất từ Elasticsearch bằng cosine similarity.
   - đồng thời lấy post `APPROVED` mới nhất từ DB.
   - merge theo thứ tự: vector candidates trước, recent candidates sau.
   - append các candidate vào `feed:<userId>`.
   - đọc lại `feed:<userId>`.
6. Với mỗi `postId`, service hydrate item:
   - đọc `post_details:<postId>` nếu có.
   - nếu miss cache, gọi `PostService.getPostById()`, chỉ nhận post `APPROVED`.
   - lấy username tác giả từ `UserDetailsService`.
   - lấy media qua `MediaService`.
   - lấy like count, comment count và trạng thái `likedByCurrentUser`.
   - cache lại `post_details:<postId>` TTL 1 ngày.
7. Sau khi trả item:
   - xóa các `postId` đã đọc khỏi `feed:<userId>`.
   - ghi các post đã trả vào `seen_post:<userId>`.
   - nếu feed còn dưới threshold 10 item, refill nền.
   - `hasMore = true` nếu `feed:<userId>` vẫn còn item.

## Nhận xét kỹ thuật

- Feed là dạng hybrid: push-based cho post mới từ người đang follow, pull/refill-based khi cache thiếu.
- Recommendation dựa trên vector nếu có dữ liệu; nếu vector rỗng hoặc lỗi, hệ thống fallback sang post approved mới nhất.
- Short-term vector phản ánh tương tác gần đây và tự hết hạn sau 1 ngày.
- Long-term vector dựa vào audit log, nên chất lượng phụ thuộc việc `like_event` và `comment_success_event` được audit thành công.
- Post bị xóa hoặc update không có luồng remove/update trực tiếp khỏi `feed:<userId>`; khi hydrate, post không `APPROVED` sẽ bị skip, nhưng dữ liệu đã cache có thể sống tới 1 ngày.
