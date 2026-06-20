# Thiết kế luồng Search Text Suggestion(triển khai trong module user)

## 1. Mục tiêu
Triển khai chức năng search suggestion dạng text popup giống Google:
* Khi người dùng bấm vào ô tìm kiếm, hiển thị lịch sử tìm kiếm gần đây và một số keyword phổ biến/trending.
* Khi người dùng nhập text, popup thay đổi theo prefix người dùng đang nhập.
* Khi người dùng bấm Enter hoặc click suggestion, hệ thống thực hiện search thật và ghi nhận keyword đó vào lịch sử tìm kiếm.
* Giai đoạn đầu chỉ hiển thị text suggestion, chưa hiển thị object như user, post, hashtag, music.

Hệ thống sử dụng:

* MySQL làm source of truth.
* Redis làm cache và fast layer.
* Spring WebFlux làm backend.
* FE dùng HTTP request với debounce, không cần WebSocket ở giai đoạn đầu.

---

## 3. Cấu trúc MySQL(Hãy tao cho tôi các entity tuowng ứng)

### 3.1. Bảng user_search_histories

Dùng để lưu lịch sử tìm kiếm riêng của từng user.

```sql
CREATE TABLE user_search_histories (
    id VARCHAR(36) PRIMARY KEY,

    user_id VARCHAR(36) NOT NULL,

    keyword VARCHAR(255) NOT NULL,
    normalized_keyword VARCHAR(255) NOT NULL,

    search_count BIGINT NOT NULL DEFAULT 1,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_user_search_keyword (user_id, normalized_keyword),

    INDEX idx_user_search_recent (
        user_id,
        last_searched_at
    ),

    INDEX idx_user_search_prefix (
        user_id,
        normalized_keyword
    )
);
```

Ý nghĩa:

* keyword: text gốc người dùng search.
* normalized_keyword: text đã normalize để tìm kiếm.
* search_count: số lần user search keyword này.
* last_searched_at: thời gian search gần nhất.

---

### 3.2. Bảng search_keywords

Dùng để lưu keyword phổ biến toàn hệ thống.

```sql
CREATE TABLE search_keywords (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,

    keyword VARCHAR(255) NOT NULL,
    normalized_keyword VARCHAR(255) NOT NULL UNIQUE,

    search_count BIGINT NOT NULL DEFAULT 1,
    user_count BIGINT NOT NULL DEFAULT 1,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_search_keywords_prefix (normalized_keyword),
    INDEX idx_search_keywords_count (search_count)
);
```

Ý nghĩa:

* search_count: tổng số lần keyword được search.
* user_count: số user khác nhau từng search keyword này.
* Chỉ nên hiển thị global suggestion nếu user_count đạt một ngưỡng nhất định, ví dụ >= 3, để tránh lộ query cá nhân của một user.
---

## 4. Cấu trúc Redis

### 4.1. User search history cache

Key:

```text
search:history:{userId}
```

Type:

```text
ZSET
```

Member:

```text
normalizedKeyword
```

Score:

```text
lastSearchedAt timestamp
```

Ví dụ:

```text
ZADD search:history:u_001 1710000000000 "spring webflux"
ZADD search:history:u_001 1710000100000 "redis autocomplete"
ZADD search:history:u_001 1710000200000 "cloudinary upload"
```

TTL:

```text
2 - 3 giờ
```

Lưu ý: Không dùng search_count làm score cho user history, vì khi cần lấy lịch sử gần nhất thì score phải là last_searched_at.

---

### 4.2. Global suggestion prefix cache

Key:

```text
search:suggest:global:{prefix}:{limit}
```

Ví dụ:

```text
search:suggest:global:spr:10
```

Type:

```text
String JSON
```

Value:

```json
[
  {
    "text": "spring webflux",
    "source": "GLOBAL",
    "isHistory": false
  },
  {
    "text": "spring boot",
    "source": "GLOBAL",
    "isHistory": false
  }
]
```

TTL:

```text
20 - 30 phút
```

Redis key này dùng để cache kết quả query prefix từ MySQL.

---

### 4.4. Trending keyword

Key theo ngày:

```text
search:trending:{yyyy-MM-dd}
```

Ví dụ:

```text
search:trending:2026-06-19
```

Type:

```text
ZSET
```

Member:

```text
normalizedKeyword
```

Score:

```text
dailySearchCount
```

TTL:

```text
7 - 14 ngày
```

Mỗi lần user search:

```text
ZINCRBY search:trending:2026-06-19 1 "spring webflux"
```

Nếu cần trending 7 ngày, có thể tổng hợp nhiều daily ZSET bằng ZUNIONSTORE.

---

## 5. API chính

### 5.1. Lấy search suggestion

```http
GET /app/search/suggestions?q={keyword}&limit=10
```

Nếu q rỗng:

* Trả lịch sử tìm kiếm gần đây.
* Kèm thêm một số keyword trending/global.

Nếu q có text:

* Tìm trong user history trước.
* Nếu chưa đủ, tìm trong global suggestion.
* Nếu global Redis cache miss, query MySQL.
* Merge, deduplicate và trả tối đa 10 kết quả.

---

### 5.2. Search thật

```http
GET /app/search?q={keyword}&page=0&size=20
```

API này dùng khi user:

* Bấm Enter
* Click suggestion
* Click button Search

Sau khi search thật, hệ thống ghi nhận keyword vào lịch sử tìm kiếm.

---

### 5.3. Xóa lịch sử tìm kiếm

Xóa một keyword:

```http
DELETE /app/search/history?keyword={keyword}
```

Xóa toàn bộ lịch sử:

```http
DELETE /app/search/history
```

---

## 6. Response format

```json
{
  "text": "spring webflux",
  "source": "HISTORY",
  "isHistory": true
}
```

Các source có thể có:

```text
HISTORY
GLOBAL
TRENDING
```

Sau này có thể mở rộng thêm:

```text
USER
POST
HASHTAG
MUSIC
```

---

## 7. Luồng 1: Người dùng bấm vào ô tìm kiếm

Điều kiện:

```text
q rỗng
```

Flow:

```text
1. FE gọi GET /app/search/suggestions?q=&limit=10

2. BE kiểm tra Redis key:
   search:history:{userId}

3. Nếu Redis có history:
   Lấy 7 keyword gần nhất bằng ZREVRANGE.

4. Nếu Redis miss:
   Query MySQL bảng user_search_histories:
      - user_id = current user
      - order by last_searched_at desc
      - limit 200

   Sau đó cache vào Redis:
      search:history:{userId}
      score = last_searched_at
      TTL = 2-3h

5. BE lấy thêm 3 keyword từ trending hoặc global popular:
   - Ưu tiên trending nếu có.
   - Nếu trending không có thì lấy global popular.

6. Merge:
   - history đứng trước
   - trending/global đứng sau
   - bỏ trùng
   - limit 10

7. Trả response cho FE.

8. FE render popup.
```

Kết quả ví dụ:

```json
[
  {
    "text": "spring webflux",
    "source": "HISTORY",
    "isHistory": true,
    "targetUrl": "/search?q=spring%20webflux"
  },
  {
    "text": "redis autocomplete",
    "source": "HISTORY",
    "isHistory": true,
    "targetUrl": "/search?q=redis%20autocomplete"
  },
  {
    "text": "java",
    "source": "TRENDING",
    "isHistory": false,
    "targetUrl": "/search?q=java"
  }
]
```

---

## 8. Luồng 2: Người dùng nhập text vào ô tìm kiếm

Ví dụ user nhập:

```text
spr
```

Flow:

```text
1. FE debounce 250-300ms.

2. FE hủy request cũ bằng AbortController nếu có.

3. FE gọi:
   GET /app/search/suggestions?q=spr&limit=10

4. BE normalize q:
   "spr"

5. Nếu q length < 2:
   return []

6. BE lấy user history cache:
   search:history:{userId}

7. Nếu Redis history miss:
   Query MySQL lấy 200 history gần nhất.
   Cache lại vào Redis.

8. BE filter 200 history trong memory:
   normalized_keyword startsWith "spr"

9. Nếu history đã đủ 10:
   return history suggestions.

10. Nếu history chưa đủ:
   Check Redis global prefix cache:
      search:suggest:global:spr:10

11. Nếu global prefix cache có:
   Merge history + global cache.
   Deduplicate.
   Return.

12. Nếu global prefix cache miss:
   Query MySQL bảng search_keywords theo prefix.

13. Cache kết quả MySQL vào Redis:
      search:suggest:global:spr:10
      TTL 60-300 giây

14. Merge:
      history trước
      global sau
      bỏ trùng
      limit 10

15. Return response.

16. FE render popup mới.
```

---

## 9. Query MySQL khi global Redis không đủ hoặc cache miss

Query:

```sql
SELECT
    keyword,
    normalized_keyword,
    search_count
FROM search_keywords
WHERE normalized_keyword LIKE CONCAT(:prefix, '%')
  AND user_count >= 3
ORDER BY search_count DESC, updated_at DESC
LIMIT :limit;
```

Nếu còn thiếu 6 item, có thể query dư:

```text
limit = remaining + 10
```

Lý do query dư:

* Có thể có item trùng với history.
* Sau khi deduplicate vẫn còn đủ kết quả.

Không cần dùng NOT IN phức tạp ngay từ đầu. Có thể query dư rồi loại trùng trong Java.

---

## 10. Luồng 3: Người dùng bấm Enter hoặc click suggestion

Flow:

```text
1. FE chuyển tới:
   /search?q={keyword}

2. BE nhận request search thật.

3. BE thực hiện search chính:
   - DB search
   - trả search result

4. Đồng thời ghi nhận search keyword.

5. Publish event SEARCH_SUBMITTED.

6. Consumer xử lý event:
   - upsert user_search_histories trong MySQL
   - upsert search_keywords trong MySQL
   - update Redis user history
   - update Redis trending daily
   - không cần update global prefix cache ngay

7. Global prefix cache để TTL tự expire.
```

Không nên update Redis trước rồi mới update MySQL.

---

## 11. Luồng update khi user submit search

Input:

```text
userId = u_001
keyword = "Spring WebFlux"
normalizedKeyword = "spring webflux"
```

Update user history MySQL:

```sql
INSERT INTO user_search_histories (
    id,
    user_id,
    keyword,
    normalized_keyword,
    search_count,
    created_at,
    last_searched_at
)
VALUES (
    :id,
    :userId,
    :keyword,
    :normalizedKeyword,
    1,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON DUPLICATE KEY UPDATE
    keyword = VALUES(keyword),
    search_count = search_count + 1,
    last_searched_at = CURRENT_TIMESTAMP;
```

Update global keyword MySQL:

```sql
INSERT INTO search_keywords (
    keyword,
    normalized_keyword,
    search_count,
    user_count
)
VALUES (
    :keyword,
    :normalizedKeyword,
    1,
    1
)
ON DUPLICATE KEY UPDATE
    keyword = VALUES(keyword),
    search_count = search_count + 1,
    updated_at = CURRENT_TIMESTAMP;
```

Nếu muốn user_count chính xác, cần có thêm bảng hoặc cơ chế ghi nhận user nào đã search keyword nào.

Update Redis history:

```text
ZADD search:history:{userId} currentTimestamp normalizedKeyword
EXPIRE search:history:{userId} 10800
```

Update Redis trending:

```text
ZINCRBY search:trending:{yyyy-MM-dd} 1 normalizedKeyword
EXPIRE search:trending:{yyyy-MM-dd} 1209600
```

Global prefix cache:

```text
Không cần update trực tiếp.
Để TTL tự hết rồi refresh từ MySQL.
```

---

## 12. Alternative cases

### Case A: User chưa có history

Input rỗng:

```text
q = ""
```

Xử lý:

```text
1. Check Redis history.
2. Redis miss.
3. Query MySQL history.
4. MySQL không có.
5. Lấy trending/global popular.
6. Nếu trending/global cũng không có thì return [].
```

Kết quả:

```json
[]
```

Hoặc có thể trả global popular nếu có.

---

### Case B: User nhập query quá ngắn

Input:

```text
q = "a"
```

Xử lý:

```text
Nếu q.length < 2:
    return []
```

Không nên query Redis/MySQL với 1 ký tự vì số lượng match quá lớn và dễ gây nhiễu.

---

### Case C: Redis user history miss

Xử lý:

```text
1. Query MySQL lấy 200 history gần nhất.
2. Cache lại vào Redis ZSET.
3. Set TTL 2-3h.
4. Tiếp tục xử lý suggestion.
```

---

### Case D: Redis global prefix cache miss

Xử lý:

```text
1. Query MySQL search_keywords theo prefix.
2. Cache kết quả vào Redis.
3. Set TTL 60-300 giây.
4. Return result.
```

---

### Case E: History + global vẫn không đủ 10 kết quả

Xử lý:

```text
Return tất cả kết quả đang có.
Không cần cố fill đủ 10.
```

Ví dụ chỉ có 4 kết quả thì trả 4.

---

### Case F: Không có kết quả nào match prefix

Xử lý:

```text
Return []
```

FE có thể ẩn popup hoặc hiển thị trạng thái “No suggestions”.

---

### Case G: Keyword trùng giữa history và global

Ví dụ:

```text
History: spring webflux
Global: spring webflux
```

Xử lý:

```text
Giữ item history.
Loại item global bị trùng.
```

Lý do:

```text
history có isHistory = true
nên FE có thể hiển thị icon lịch sử hoặc nút xóa.
```

---

### Case H: User xóa một keyword history

Flow:

```text
1. FE gọi DELETE /app/search/history?keyword=...
2. BE delete MySQL row:
   DELETE FROM user_search_histories
   WHERE user_id = current user
     AND normalized_keyword = normalizedKeyword
3. BE remove khỏi Redis:
   ZREM search:history:{userId} normalizedKeyword
4. Return success.
```

---

### Case I: User clear toàn bộ history

Flow:

```text
1. FE gọi DELETE /app/search/history
2. BE delete MySQL rows:
   DELETE FROM user_search_histories
   WHERE user_id = current user
3. BE delete Redis key:
   DEL search:history:{userId}
4. Return success.
```

---

### Case J: Redis bị lỗi

Xử lý:

```text
1. Log warn/error.
2. Fallback MySQL.
3. Không làm fail toàn bộ API suggestion nếu MySQL còn hoạt động.
```

Redis là cache, không phải source of truth.

---

### Case K: MySQL bị lỗi

Xử lý:

```text
1. Nếu Redis có cache thì trả cache.
2. Nếu Redis không có thì return [] hoặc error nhẹ tùy policy.
3. Log error.
```

Với suggestion, có thể return [] để không ảnh hưởng trải nghiệm chính.

---

### Case L: Cache global bị stale

Ví dụ search_count đã thay đổi nhưng Redis vẫn đang cache kết quả cũ.

Xử lý:

```text
Chấp nhận stale trong 60-300 giây.
```

Không cần invalidation phức tạp ở giai đoạn đầu.

---

### Case M: Query có dữ liệu nhạy cảm

Ví dụ:

```text
email
số điện thoại
token
nội dung quá dài
```

Xử lý:

```text
1. Có thể lưu vào user history nếu cần.
2. Không đưa vào global search_keywords.
3. Không đưa vào trending.
```

Cần filter trước khi update global.

---

### Case N: User chưa đăng nhập

Có 2 hướng:

```text
1. Không dùng history, chỉ trả global/trending.
2. Lưu history tạm ở localStorage phía FE.
```

Backend không nên tạo user history nếu không xác định được user.

---

### Case O: Traffic tăng cao

Khi traffic cao, Redis cache prefix result có thể chưa đủ.

Có thể nâng cấp sang Redis prefix ZSET:

```text
search:suggest:global:p:{prefix}
```

Ví dụ:

```text
ZADD search:suggest:global:p:spr 100 "spring webflux"
ZADD search:suggest:global:p:spr 80 "spring boot"
```

Query:

```text
ZREVRANGE search:suggest:global:p:spr 0 9 WITHSCORES
```

Nhược điểm:

```text
Tốn memory hơn và update phức tạp hơn.
```

Chỉ nên dùng khi cần scale.

---

### Case P: Muốn dùng WebSocket

Có thể dùng, nhưng không khuyên cho MVP.

Nếu dùng WebSocket, message nên có seq:

Client gửi:

```json
{
  "seq": 12,
  "q": "spring"
}
```

Server trả:

```json
{
  "seq": 12,
  "items": []
}
```

FE chỉ render response có seq mới nhất.

Nhưng với nhu cầu hiện tại, HTTP + debounce + AbortController đơn giản và phù hợp hơn.

---

## 13. FE behavior

FE nên xử lý:

```text
1. Người dùng focus input:
   gọi /suggestions?q=

2. Người dùng input:
   debounce 300ms

3. Nếu q.length < 2:
   ẩn popup hoặc return []

4. Nếu q.length >= 2:
   gọi /suggestions?q={q}

5. Trước khi gọi request mới:
   hủy request cũ bằng AbortController

6. Khi response về:
   render popup mới

7. Người dùng click suggestion:
   chuyển tới targetUrl

8. Người dùng bấm Enter:
   chuyển tới /search?q={input}
```

---

## 14. Ranking rule

Ranking nên theo thứ tự:

```text
1. History match prefix
   - ưu tiên last_searched_at mới nhất

2. Global match prefix
   - ưu tiên search_count cao
   - chỉ lấy keyword đủ điều kiện public/global

3. Trending
   - dùng cho input rỗng hoặc fallback
   - ưu tiên count trong thời gian gần đây
```

Merge rule:

```text
history trước
global sau
trending sau cùng
deduplicate theo normalized_keyword
limit 10
```

---

## 15. Kết luận

Kiến trúc cuối cùng nên là:

```text
MySQL:
- user_search_histories
- search_keywords

Redis:
- search:history:{userId}
- search:suggest:global:{prefix}:{limit}
- search:text:global
- search:trending:{yyyy-MM-dd}

API:
- GET /app/search/suggestions?q=&limit=10
- GET /app/search?q=
- DELETE /app/search/history
- DELETE /app/search/history?keyword=

FE:
- debounce
- AbortController
- render popup
- click suggestion thì search
```

Điểm quan trọng:

```text
1. user_search_history score phải là last_searched_at, không phải search_count.
2. search_text_global ZSET không dùng làm prefix search chính.
3. global prefix suggestion nên cache theo prefix.
4. Redis là cache, MySQL là source of truth.
5. Update search history nên làm sau khi user submit search thật.
6. Không đưa mọi query vào global để tránh lộ dữ liệu riêng tư.
7. Giai đoạn đầu không cần WebSocket.
```
