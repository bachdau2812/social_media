# Thiết kế triển khai module Feed

## 1. Mục tiêu

Module Feed được triển khai nhằm xây dựng danh sách bài viết phù hợp cho từng người dùng dựa trên:

* Các bài viết mới từ những người mà user đang follow.
* Các sự kiện tương tác của user như like, comment.
* Vector sở thích ngắn hạn.
* Vector sở thích dài hạn.
* Các bài viết trending hoặc bài viết mới.
* Cơ chế tránh hiển thị lại các bài viết đã được load lên FE.

Hệ thống sẽ sử dụng Kafka để xử lý các sự kiện bất đồng bộ, Redis để cache feed và trạng thái tạm thời, Elasticsearch để truy vấn vector và tìm các bài viết phù hợp.

---

## 2. Lắng nghe và xử lý sự kiện

Module Feed cần lắng nghe các sự kiện khi những thao tác trong hệ thống được hoàn tất để xử lý dữ liệu phục vụ feed.

### 2.1. Nhóm sự kiện tương tác của người dùng

Các sự kiện như:

* Like bài viết.
* Comment bài viết.

sẽ được đẩy vào Kafka topic:

```text
user_interaction_events
```

Kafka message key:

```text
userId
```

Việc dùng `userId` làm key giúp các event của cùng một user được xử lý tuần tự, tránh lỗi cập nhật vector sai thứ tự.

Các event này được dùng để:

* Cập nhật `user_short_term_vector`.
* Ghi nhận hành vi phục vụ phân tích feed.
* Làm dữ liệu đầu vào cho việc cập nhật vector dài hạn.

---

### 2.2. Nhóm sự kiện broadcast vào feed của follower

Các sự kiện như:

* Create post.
* Upload story.
* Update avatar.

sẽ được broadcast vào Redis feed key của danh sách những người đang follow actor.

Ví dụ:

```text
feed:<followerId>
```

Khi user A tạo bài viết mới, hệ thống sẽ lấy danh sách những người follow A, sau đó thêm `postId` vào feed key của từng follower.

---

## 3. Thiết kế Redis key

### 3.1. Feed key của user

```text
feed:<userId>
```

Kiểu dữ liệu:

```text
ZSET
```

Trong đó:

* `member`: `postId`
* `score`: timestamp, tương ứng thời gian bài viết được thêm vào feed

Ví dụ:

```text
ZADD feed:user_123 1719040000 post_456
```

Key này lưu danh sách các bài viết chờ được hiển thị trên feed của user.

---

### 3.2. Post details cache

```text
post_details:<postId>
```

Kiểu dữ liệu:

```text
STRING
```

Giá trị lưu trữ:

```text
JSON thông tin chi tiết của bài viết
```

TTL:

```text
1 ngày
```

Key này dùng để cache thông tin chi tiết của bài viết, bao gồm:

* Thông tin bài viết.
* Thông tin tác giả.
* Danh sách media.
* Các metadata cần thiết để hiển thị feed.

Khi lấy feed, hệ thống sẽ kiểm tra `post_details:<postId>` trước. Nếu Redis chưa có dữ liệu, hệ thống sẽ tổng hợp thông tin từ database, lưu lại vào Redis, sau đó trả về cho FE.

---

### 3.3. Seen post key

```text
seen_post:<userId>
```

Kiểu dữ liệu:

```text
LIST
```

TTL:

```text
5 ngày
```

Key này lưu danh sách các bài viết đã được load lên FE để tránh hiển thị lại quá sớm.

Khi một bài viết được lấy ra để trả về cho FE, hệ thống sẽ thêm `postId` vào key này.

> Lưu ý: Nếu cần kiểm tra nhanh một bài viết đã seen hay chưa, có thể cân nhắc dùng `SET` thay vì `LIST`.

---

## 4. Luồng lấy feed cho người dùng

### 4.1. Request lấy feed

Người dùng gọi API lấy feed, kèm theo params:

```text
limit
```

Ví dụ:

```http
GET /feed?limit=20
```

---

### 4.2. Lấy danh sách postId từ Redis feed key

Hệ thống lấy danh sách `postId` từ key:

```text
feed:<userId>
```

theo số lượng `limit`.

---

### 4.3. Trường hợp feed key có đủ bài viết

Nếu `feed:<userId>` có đủ số lượng bài viết theo `limit`, hệ thống thực hiện các bước sau:

1. Lấy danh sách `postId` từ Redis.
2. Với từng `postId`, lấy thông tin chi tiết bài viết.
3. Khi lấy thông tin chi tiết, kiểm tra Redis key:

```text
post_details:<postId>
```

4. Nếu Redis có dữ liệu, trả về dữ liệu từ Redis.
5. Nếu Redis chưa có dữ liệu:

    * Tổng hợp thông tin bài viết từ database.
    * Lấy danh sách media tương ứng.
    * Build response feed item.
    * Lưu vào Redis key `post_details:<postId>`.
    * Trả về dữ liệu cho người dùng.
6. Sau khi lấy xong, xóa các `postId` đó khỏi ZSET `feed:<userId>`.
7. Thêm các `postId` đã load lên FE vào key:

```text
seen_post:<userId>
```

8. Kiểm tra kích thước còn lại của ZSET `feed:<userId>`.
9. Nếu số lượng còn lại nhỏ hơn một ngưỡng cấu hình `n`, hệ thống sẽ tiến hành phân tích và nạp thêm bài viết mới vào feed.
10. Ngoài ra, hệ thống có thể lấy thêm `k` bài viết mới nhất để bổ sung vào feed.

---

### 4.4. Trường hợp feed key không đủ bài viết

Nếu `feed:<userId>` không có đủ số lượng bài viết theo `limit`, hệ thống thực hiện các bước sau:

1. Tiến hành phân tích vector của người dùng.
2. Tạo `query_vector` dựa trên short-term vector và long-term vector.
3. Truy vấn Elasticsearch để lấy danh sách bài viết phù hợp.
4. Lọc bỏ các bài viết:

    * Đã bị xóa.
    * Đã bị ẩn.
    * Người dùng không có quyền xem.
    * Đã hiển thị gần đây.
5. Kết hợp thêm bài viết trending và bài viết mới.
6. Rerank danh sách candidate.
7. Lưu `m` bài viết vào Redis key:

```text
feed:<userId>
```

8. Tiếp tục thực hiện luồng lấy feed như trường hợp đủ bài viết.

---

## 5. Một số yêu cầu triển khai

Khi triển khai module feed cần chú ý:

* Bám sát các rule trong file nghiệp vụ.
* Comment rõ ràng tại các phần xử lý quan trọng.
* Chỉ tạo DTO và constant khi thực sự cần thiết.
* Khi cập nhật vector cần chuẩn hóa vector sau khi tính toán.
* Việc chuẩn hóa vector giúp tránh sai lệch độ dài giữa các vector và giảm rủi ro xung đột khi kết hợp nhiều vector khác nhau.
* Các thao tác liên quan đến Redis và Kafka cần có logging hợp lý.
* Không nên blocking trong luồng WebFlux.
* Nên xử lý lỗi Kafka/Redis/Elasticsearch theo hướng fallback an toàn.

---

# Thiết kế thuật toán đề xuất

Hệ thống sẽ lưu trữ và sử dụng các loại vector để xây dựng recommendation system.

---

## 6. Vector ngắn hạn của người dùng

### 6.1. Mục đích

Hệ thống duy trì `user_short_term_vector` để biểu diễn sở thích tức thời của người dùng trong phiên gần đây.

Vector này nên được lưu trong Redis.

Ví dụ key:

```text
user_short_term_vector:<userId>
```

---

### 6.2. Dữ liệu đầu vào

Các tương tác của người dùng sẽ được đẩy vào Kafka topic:

```text
user_interaction_events
```

Kafka message key:

```text
userId
```

Các action hiện tại:

| Action  | Weight |
| ------- | -----: |
| Like    |  `0.3` |
| Comment |  `0.7` |

---

### 6.3. Công thức cập nhật short-term vector

Consumer sẽ cập nhật short-term vector theo công thức:

```text
short_vector_new = normalize(decay * short_vector_old + weight * post_vector)
```

Trong đó:

* `short_vector_old`: vector ngắn hạn hiện tại của user.
* `post_vector`: vector của bài viết mà user vừa tương tác.
* `weight`: trọng số của action.
* `decay`: mức độ giữ lại sở thích cũ.
* `normalize`: bước chuẩn hóa vector sau khi tính toán.

Giá trị mặc định:

```text
decay = 0.7
```

Công thức này giúp vector ngắn hạn phản ứng nhanh với hành vi mới của người dùng nhưng vẫn giữ lại một phần sở thích gần đây.

---

## 7. Vector dài hạn của người dùng

### 7.1. Mục đích

Hệ thống duy trì `user_long_term_vector` để biểu diễn sở thích ổn định của người dùng trong thời gian dài.

Vector này hiện được lưu trong Elasticsearch index:

```text
user_detail_vector
```

---

### 7.2. Cơ chế cập nhật định kỳ

Vector dài hạn sẽ được cập nhật định kỳ mỗi ngày một lần vào:

```text
2h sáng
```

Dữ liệu đầu vào được lấy từ bảng:

```text
audit_logs
```

Chỉ lấy dữ liệu của ngày hôm qua với các action:

```text
LIKE_POST
COMMENT_POST
```

---

### 7.3. Luồng cập nhật vector dài hạn

Tại thời điểm cập nhật:

1. Lấy danh sách tương tác của ngày hôm qua từ `audit_logs`.
2. Lấy vector dài hạn hiện tại của user.
3. Trước khi tính toán, lưu vector dài hạn hiện tại vào Redis tạm thời.
4. Khi cần lấy vector để tính toán, kiểm tra Redis trước.
5. Nếu Redis chưa có, lấy từ Elasticsearch hoặc database.
6. Tính `V_today` từ tổng hợp vector của các bài viết user đã tương tác trong ngày.
7. Cập nhật vector dài hạn theo công thức.
8. Lưu vector mới lại vào Elasticsearch.
9. Xóa key Redis tạm sau khi tính toán xong.

---

### 7.4. Công thức cập nhật vector dài hạn

```text
V_long_new = normalize(alpha * V_long_old + (1 - alpha) * V_today)
```

Trong đó:

* `V_long_old`: vector dài hạn hiện tại.
* `V_today`: vector tổng hợp từ các tương tác trong ngày.
* `alpha`: hệ số giữ lại sở thích dài hạn cũ.

Giá trị đề xuất:

```text
alpha = 0.6 - 0.8
```

Công thức này giúp hệ thống vẫn giữ được sở thích dài hạn của người dùng, nhưng vẫn có khả năng thích nghi với hành vi mới.

---

## 8. Luồng lấy feed cá nhân hóa

Khi người dùng yêu cầu lấy feed, hệ thống sẽ thực hiện các bước sau:

1. Lấy `user_short_term_vector` từ Redis.
2. Lấy `user_long_term_vector` từ database, feature store hoặc Elasticsearch.
3. Tạo `query_vector` bằng cách kết hợp hai vector:

```text
query_vector = normalize(a * V_long + b * V_short)
```

Trong đó:

```text
a + b = 1
```

Ví dụ với user mới:

```text
a = 0.3
b = 0.7
```

Ví dụ với user đã có nhiều lịch sử:

```text
a = 0.7
b = 0.3
```

4. Dùng `query_vector` để truy vấn vào Elasticsearch index:

```text
posts_recommendation_index
```

5. Lấy topK bài viết có độ tương đồng cao nhất.
6. Loại bỏ các bài viết:

    * Đã bị xóa.
    * Đã bị ẩn.
    * Không đủ quyền xem.
    * Đã hiển thị gần đây.
7. Kết hợp thêm:

    * Bài viết trending.
    * Bài viết mới.
8. Rerank danh sách candidate.
9. Lưu kết quả vào Redis key:

```text
feed:<userId>
```

10. Trả về feed cho người dùng.

---

## 9. Không hard-code ngưỡng similarity

Không nên hard-code ngưỡng similarity như:

```text
0.7
0.8
```

Cách tốt hơn là:

1. Lấy topK candidates từ Elasticsearch.
2. Lọc các bài viết không hợp lệ.
3. Kết hợp thêm trending và bài viết mới.
4. Rerank bằng nhiều tín hiệu khác nhau.
5. Chọn ra số lượng bài viết cần trả về theo `limit`.

Điều này giúp hệ thống linh hoạt hơn, tránh trường hợp user không có đủ feed do ngưỡng similarity quá cao.

---

## 10. Cơ chế tránh feed bị lặp

Khi bài viết được load lên FE, hệ thống sẽ thêm `postId` vào key đánh dấu user đã xem bài viết.

Ví dụ:

```text
seen_post:<userId>
```

Key này giúp hệ thống tránh đề xuất lại các bài viết đã được hiển thị gần đây.

TTL đề xuất:

```text
5 ngày
```

Luồng xử lý:

1. Backend trả danh sách feed cho FE.
2. FE load bài viết lên giao diện.
3. Hệ thống đánh dấu các bài đã được load vào `seen_post:<userId>`.
4. Khi generate feed mới, hệ thống kiểm tra `seen_post:<userId>` để loại bỏ các bài đã hiển thị gần đây.

---

## 11. Tổng kết luồng chính

```text
User action: like/comment
        |
        v
Kafka topic: user_interaction_events
key = userId
        |
        v
Consumer cập nhật user_short_term_vector trong Redis
```

```text
User action: create post / upload story / update avatar
        |
        v
Lấy danh sách follower
        |
        v
Broadcast postId vào feed:<followerId>
```

```text
User gọi API lấy feed
        |
        v
Kiểm tra feed:<userId>
        |
        |-- Đủ bài:
        |       lấy post details
        |       trả về FE
        |       xóa khỏi feed ZSET
        |       thêm vào seen_post
        |
        |-- Không đủ:
                tạo query_vector
                query Elasticsearch
                lấy trending + bài mới
                rerank
                nạp thêm vào feed:<userId>
                tiếp tục trả về FE
```

---

## 12. Các điểm cần chú ý

* `feed:<userId>` nên dùng ZSET để lấy bài viết theo thời gian hoặc score.
* `post_details:<postId>` nên có TTL để tránh dữ liệu cũ tồn tại quá lâu.
* `seen_post:<userId>` hiện đang dùng LIST, nhưng nếu cần kiểm tra nhanh bài đã seen hay chưa thì nên cân nhắc dùng SET.
* Khi xóa bài khỏi `feed:<userId>`, cần đảm bảo bài đã được trả về hoặc đã load lên FE.
* Không nên để Redis key tăng vô hạn; tất cả key cache nên có TTL.
* Khi cập nhật vector, luôn chuẩn hóa vector sau khi tính toán.
* Kafka event nên có `eventId` để tránh xử lý trùng.
* Feed nên có fallback nếu Elasticsearch hoặc Redis gặp lỗi.
