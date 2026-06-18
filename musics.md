Ứng dụng hiện có bảng `musics` dùng để lưu danh sách bài hát. Nguồn nhạc demo sẽ được lấy từ Jamendo API. Tôi muốn xây dựng một service có thể nhận vào một URL Jamendo API, gọi URL đó để lấy JSON response, parse danh sách bài hát, upload file nhạc lên Cloudinary, sau đó lưu metadata bài hát vào database.

Service cần thiết kế sao cho sau này nếu muốn import thêm bài hát từ Jamendo thì chỉ cần truyền URL API mới vào và chạy lại luồng tương tự.

---

## Cấu trúc JSON response từ Jamendo

Jamendo API trả về JSON dạng gần như sau:

```json
{
  "headers": {
    "...": "..."
  },
  "results": [
    {
      "id": "1848357",
      "name": "Song name",
      "duration": 272,
      "artist_name": "Artist name",
      "album_image": "https://...",
      "image": "https://...",
      "audio": "https://prod-1.storage.jamendo.com/...",
      "audiodownload": "https://prod-1.storage.jamendo.com/download/track/...",
      "audiodownload_allowed": true,
      "license_ccurl": "http://creativecommons.org/licenses/...",
      "lyrics": "..."
    }
  ]
}
```

Lưu ý: response thực tế của Jamendo dùng `headers` và `results`, không phải `header` và `result`.

---

## Luồng import nhạc từ Jamendo
Triển khai một service import nhạc với luồng như sau:
1. Nhận vào một URL Jamendo API từ request, kèm params là category.
2. Validate URL:
    * Chỉ cho phép gọi tới domain Jamendo hợp lệ, ví dụ `api.jamendo.com`.
    * Không cho phép gọi URL tùy ý để tránh rủi ro bảo mật SSRF.
    * URL không được rỗng.
3. Gọi HTTP request tới URL đã validate.
4. Parse JSON response thu được.
5. Lấy danh sách bài hát trong field `results`.
6. Với mỗi bài hát trong `results`, thực hiện xử lý:
    * Lấy `id` của bài hát từ Jamendo.
    * Kiểm tra trong DB xem bài hát đã tồn tại chưa.
    * Nếu đã tồn tại thì bỏ qua, không insert lại.
    * Nếu chưa tồn tại thì tiếp tục xử lý.
7. Mapping dữ liệu từ JSON sang entity `Musics`:
    * `id` → `id`
    * `name` → `displayName`
    * `slugName` → tự tạo từ `displayName`
    * `lyrics` → `descriptions`
    * `image` hoặc `album_image` → `displayImages`
    * `duration` → `duration`
    * `artist_name` → `singleName`
    * `songUrl` → lấy sau khi upload file nhạc lên Cloudinary
    * `releasedate` → `releaseDate`
    * trường category set bằng params truyền vào khi gửi request
8. Xử lý file nhạc:
    * Ưu tiên dùng field `audiodownload` để tải file nhạc.
    * Trước khi dùng `audiodownload`, kiểm tra `audiodownload_allowed`.
    * Nếu `audiodownload_allowed = false` hoặc `audiodownload` rỗng thì bỏ qua bài đó hoặc fallback sang field `audio` tùy theo logic demo.
    * Nếu có thể tải được file từ `audiodownload`, upload file đó lên Cloudinary.
    * Sau khi upload Cloudinary thành công, lấy `secure_url` trả về từ Cloudinary và set vào `Musics.songUrl`. Lấy cả public URL để có thể kéo thông tin về và lưu vào bảng medias với ownerID là id của bài nhạc trong Json
9. Sau khi entity `Musics` đã có đủ dữ liệu:
    * Lưu vào DB.
    * Xóa hoặc refresh cache liên quan đến danh sách nhạc.
    * Log lại kết quả import: tổng số bài nhận được, số bài đã lưu, số bài bị bỏ qua, số bài lỗi.
10. Nếu một bài bị lỗi trong quá trình download/upload/save DB:
* Không làm fail toàn bộ batch import.
* Ghi log lỗi của bài đó.
* Tiếp tục xử lý các bài còn lại.
---

## API import nhạc từ Jamendo

Triển khai API:

```http
POST /app/musics/import/jamendo
```

Request body:

```json
{
  "url": "https://api.jamendo.com/v3.0/tracks/?client_id=xxx&format=json&limit=20&tags=jazz&include=licenses&audioformat=mp32&audiodlformat=mp32"
}
```

Response body mong muốn:

```json
{
  "totalReceived": 20,
  "savedCount": 15,
  "skippedCount": 3,
  "failedCount": 2,
  "message": "Import Jamendo musics completed"
}
```

---

## CRUD API cho bảng musics

Ngoài API import từ Jamendo, triển khai thêm các API CRUD cơ bản cho bảng `musics`.

### 1. Tạo mới một bài nhạc thủ công

```http
POST /app/musics
```

Request body:

```json
{
  "displayName": "Song name",
  "descriptions": "Song description",
  "displayImages": "https://...",
  "singleName": "Artist name",
  "songUrl": "https://...",
  "duration": 272
}
```

Yêu cầu:

* Validate `displayName` không rỗng.
* Validate `songUrl` không rỗng.
* Tự sinh `id`.
* Tự tạo `slugName` từ `displayName`.
* Lưu vào DB.
* Xóa hoặc refresh cache danh sách nhạc.

---

### 2. Lấy danh sách bài nhạc

```http
GET /app/musics
```

Hỗ trợ query params:

```http
GET /app/musics?page=0&size=20&keyword=jazz
```

Yêu cầu:

* Có phân trang.
* Có tìm kiếm theo `displayName`, `singleName` hoặc `slugName`.
* Trả về danh sách bài nhạc.

### 3. Lấy danh sách bài nhạc theo category

```http
GET /app/musics
```

Hỗ trợ query params:

```http
GET /app/musics?page=0&size=20&category=jazz
```

Yêu cầu:

* Có phân trang.
* Có tìm kiếm theo `category`.
* Trả về danh sách bài nhạc.

---

### 4. Lấy chi tiết một bài nhạc
```http
GET /app/musics/{musicId}
```
Yêu cầu:
* Tìm theo `id`.
* Nếu không tồn tại thì throw `AppException` với ErrorCode phù hợp.
* Trả về thông tin chi tiết bài nhạc.
---

