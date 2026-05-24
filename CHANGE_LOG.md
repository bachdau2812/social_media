# CHANGE LOG

## [2026-05-21] - Triển khai Follower System cho User Relationships

### Tính năng mới
- **Follow/Unfollow System**: Triển khai đầy đủ chức năng follow/unfollow giữa users
- **Social Networking**: Hỗ trợ query danh sách followers và following với pagination
- **Follower Counts**: Track số lượng followers và following cho mỗi user
- **Duplicate Prevention**: Auto-check và prevent duplicate follow relationships
- **Optimized Queries**: Custom SQL queries cho pagination và counting
- **No Caching**: Theo yêu cầu, follower system không sử dụng Redis cache

### Files mới được tạo (7 files):

#### Entity
- `src/main/java/com/dauducbach/clone/modules/user/entity/UserFollower.java`
  - Fields: id, followerId (người follow), followingId (người được follow), createdAt
  - Helper method: `create(String followerId, String followingId)` cho easy creation
  - Auto-generate UUID và timestamp

#### Repository  
- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserFollowerRepository.java`
  - Custom queries với `@Query` annotations:
    - `existsByFollowerIdAndFollowingId()` - Check duplicate follows
    - `findFollowersByUserId()` - Get followers với pagination (ORDER BY created_at DESC)
    - `findFollowingByUserId()` - Get following với pagination
    - `countFollowers()`, `countFollowing()` - Follower statistics
    - `deleteByFollowerIdAndFollowingId()` - Delete specific relationship

#### Request DTOs
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/FollowRequest.java`
  - Fields: followerId, followingId với validation

#### Response DTOs
- `src/main/java/com/dauducbach/clone/modules/user/dto/response/FollowResponse.java`  
  - Single follow relationship details
- `src/main/java/com/dauducbach/clone/modules/user/dto/response/FollowerListResponse.java`
  - Paginated follower/following lists với metadata (totalCount, currentPage, pageSize, hasNextPage, hasPreviousPage)
  - Inner class FollowerInfo: followId, userId, followedAt

#### Service
- `src/main/java/com/dauducbach/clone/modules/user/service/UserFollowerService.java`
  - **Follow**: `followUser()` - Validation, duplicate check, insert into DB
  - **Unfollow**: `unfollowUser()` - Validate existence, delete from DB
  - **Get by ID**: `getUserFollowerById()` - Lấy relationship theo ID
  - **Followers List**: `getFollowers()` - Get list of users following a specific user (paginated)
  - **Following List**: `getFollowing()` - Get list of users that a user is following (paginated)
  - **Status Check**: `isFollowing()` - Boolean check if user A follows user B
  - **Statistics**: `getFollowerCounts()` - Get follower/following counts
  - **Business Logic**:
    - Prevent self-follow: Cannot follow yourself
    - Duplicate prevention: Error if already following
    - Page size validation: Default 20, max 100 items
    - Proper error messages và logging

#### Controller
- `src/main/java/com/dauducbach/clone/modules/user/controller/UserFollowerController.java`
  - Base URL: `/app/user-followers`

### API Endpoints (7 endpoints):

#### Follow Operations
- `POST /app/user-followers/follow` - Tạo follow relationship
  - Request: `{followerId, followingId}`
  - Response: Follow entity với created timestamp

- `DELETE /app/user-followers/unfollow?followerId={id}&followingId={id}` - Xóa follow relationship  
  - Query params: followerId, followingId
  - Response: Success message

#### Query Operations
- `GET /app/user-followers/{id}` - Lấy follow relationship theo ID
  - Returns: Single FollowResponse

- `GET /app/user-followers/followers/{userId}?page=0&size=20` - Lấy danh sách followers (người đang follow user này)
  - Paginated list reversed chronological (most recent first)
  - Response: FollowerListResponse với metadata

- `GET /app/user-followers/following/{userId}?page=0&size=20` - Lấy danh sách following (người mà user này đang follow)
  - Paginated list reversed chronological
  - Response: FollowerListResponse (reuses same structure)

- `GET /app/user-followers/is-following?followerId={id}&followingId={id}` - Check follow status
  - Returns: Boolean true/false

- `GET /app/user-followers/counts/{userId}` - Lấy follower statistics
  - Returns: `{followersCount, followingCount}`

### Database Schema
- **Table**: `user_follower`
- **Primary Key**: `id` (UUID)
- **Indexes**:
  - `(follower_id, following_id)` - Unique composite index để prevent duplicates
  - `(following_id, created_at)` - Cho efficient followers queries
  - `(follower_id, created_at)` - Cho efficient following queries
- **No caching**: Theo yêu cầu, không sử dụng Redis cache cho follower operations

### Technical Implementation

#### Query Optimization
- Custom SQL queries thay vì ORM methods cho better performance
- LIMIT/OFFSET pagination (simple và effective)
- COUNT queries separated để get total counts trước khi fetch data

#### Error Handling  
- Self-follow prevention: "Cannot follow yourself"
- Duplicate prevention: "Already following this user"  
- Unfollow validation: "Not following this user"
- Consistent error messages across all operations

#### Pagination Logic
- Default: page=0, size=20
- Max page size: 100 items per request
- Metadata included: totalPages, hasNextPage, hasPreviousPage
- Offset calculation: `offset = page * size`

#### Business Rules
- One-direction relationships: A follows B không có nghĩa B follows A
- No cache: Direct DB queries (theo yêu cầu)
- Instant feedback: All operations return immediate results
- Immutable timestamps: `createdAt` không thay đổi sau creation

---

## [2026-05-21] - Triển khai CRUD Modules cho User Info Entities

### Tính năng mới
- **CRUD Operations cho User Info Entities**: Triển khai đầy đủ CRUD operations cho các entities phụ của user
- **Privacy Controls**: Hỗ trợ kiểm soát quyền riêng tư với isPublic flag cho job, education entities
- **Enhanced Caching**: Redis caching với separate cache keys cho individual items và lists
- **List-based Operations**: Optimized queries với list filtering theo visibility settings

### Entities được triển khai CRUD hoàn chỉnh:
- **UserJob**: Thông tin công việc với privacy controls
- **UserSocialMedia**: Links mạng xã hội  
- **UserPhone**: Số điện thoại với verification status
- **UserHighSchool**: Trung học phổ thông với privacy controls
- **UserUniversity**: Đại học với privacy controls

### Files mới được tạo (94 files total):

#### Repositories (5 files)
- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserJobRepository.java`
- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserSocialMediaRepository.java`
- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserPhoneRepository.java`
- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserHighSchoolRepository.java`
- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserUniversityRepository.java`

#### Request DTOs (7 files)
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserJobRequest.java`
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserJobUpdateRequest.java`
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserSocialMediaRequest.java`
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserPhoneRequest.java`
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserHighSchoolRequest.java`
- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserUniversityRequest.java`

#### Services (5 files)
- `src/main/java/com/dauducbach/clone/modules/user/service/UserJobService.java`
  - CRUD operations với privacy filtering
  - List visibility: public users có thể thấy, non-public yêu cầu authentication
  - Cache strategy: Individual items + user lists with different TTLs
  
- `src/main/java/com/dauducbach/clone/modules/user/service/UserSocialMediaService.java`
- `src/main/java/com/dauducbach/clone/modules/user/service/UserPhoneService.java`
- `src/main/java/com/dauducbach/clone/modules/user/service/UserHighSchoolService.java`
  - Education entities với graduation status and privacy controls
  - Similar to UserJob structure
  
- `src/main/java/com/dauducbach/clone/modules/user/service/UserUniversityService.java`

#### Controllers (5 files)
- `src/main/java/com/dauducbach/clone/modules/user/controller/UserJobController.java`
- `src/main/java/com/dauducbach/clone/modules/user/controller/UserSocialMediaController.java`
- `src/main/java/com/dauducbach/clone/modules/user/controller/UserPhoneController.java`
- `src/main/java/com/dauducbach/clone/modules/user/controller/UserHighSchoolController.java`
- `src/main/java/com/dauducbach/clone/modules/user/controller/UserUniversityController.java`

### API Endpoints mới (25 endpoints total):

#### UserJob (`/app/user-jobs`)
- `POST /user-jobs` - Tạo job mới
- `PUT /user-jobs` - Cập nhật job (partial update)
- `GET /user-jobs/{id}` - Lấy job theo ID
- `GET /user-jobs/user/{userId}?includeNonPublic=false` - Lấy danh sách jobs
- `DELETE /user-jobs/{id}` - Xóa job

#### UserSocialMedia (`/app/user-social-media`)
- `POST /user-social-media` - Tạo social media link
- `GET /user-social-media/{id}` - Lấy link theo ID
- `GET /user-social-media/user/{userId}` - Lấy tất cả links của user
- `DELETE /user-social-media/{id}` - Xóa link

#### UserPhone (`/app/user-phones`)
- `POST /user-phones` - Tạo phone number
- `GET /user-phones/{id}` - Lấy phone theo ID
- `GET /user-phones/user/{userId}` - Lấy tất cả phones của user
- `DELETE /user-phones/{id}` - Xóa phone

#### UserHighSchool (`/app/user-high-schools`)
- `POST /user-high-schools` - Tạo high school
- `GET /user-high-schools/{id}` - Lấy high school theo ID
- `GET /user-high-schools/user/{userId}?includeNonPublic=false` - Lấy danh sách schools
- `DELETE /user-high-schools/{id}` - Xóa high school

#### UserUniversity (`/app/user-universities`)
- `POST /user-universities` - Tạo university
- `GET /user-universities/{id}` - Lấy university theo ID
- `GET /user-universities/user/{userId}?includeNonPublic=false` - Lấy danh sách universities
- `DELETE /user-universities/{id}` - Xóa university

### Technical Implementation

#### Cache Strategy
- **Individual items**: `{entity_name}:{id}` - 24h TTL
- **User lists**: `{entity_name}_list:{userId}` - 24h TTL
- **Privacy filtering**: Non-public lists không được cache (bảo mật)
- **Invalidation**: Auto-remove list cache khi create/update/delete individual items

#### Privacy & Security
- **isPublic flag**: Filter ở DB level cho public queries
- **includeNonPublic parameter**: Admin/user owner có thể xem tất cả data
- **Cache security**: Non-public data không bao giờ được cache
- **Error handling**: Proper error messages cho unauthorized access

#### Code Quality & Reusability
- **RedisUtil enhancements**: Thêm deserializeList() method
- **Consistent patterns**: All services follow same structure
- **GsonUtils reuse**: Dùng existing utils cho JSON operations
- **No conflicts**: Hoàn toàn tương thích với existing UserDetails module
- **UUID generation**: Tự động generate IDs cho tất cả entities

---

## [2026-05-21] - Phát triển Module UserDetails

### Tính năng mới
- **Thao tác CRUD UserDetails**: Triển khai đầy đủ chức năng CRUD quản lý thông tin người dùng
- **Redis Reactive Cache**: Thêm tầng caching sử dụng ReactiveRedisTemplate với serializes JSON string
- **Cập nhật một phần (Partial Updates)**: Hỗ trợ cập nhật từng trường của UserDetails, cho phép sửa đổi linh hoạt

### Files mới được tạo
- `src/main/java/com/dauducbach/clone/modules/user/entity/UserDetails.java`
  - Entity class cho user details với các trường: userId, username, password, dob, homeTown, livingIn, sex
  - Special handling: `hobbieList` được transparent convert thành JSON string (`hobbieListJson`) để lưu vào MySQL
  - Custom getter/setter tự động convert List<String> ↔ JSON string

- `src/main/java/com/dauducbach/clone/modules/user/dto/request/UserDetailsUpdateRequest.java`
  - Request DTO để cập nhật user details với validation

- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserDetailsRepository.java`
  - Reactive repository interface extending ReactiveCrudRepository

- `src/main/java/com/dauducbach/clone/modules/user/service/UserDetailsService.java`
  - Service layer với hỗ trợ caching và business logic

- `src/main/java/com/dauducbach/clone/modules/user/controller/UserDetailsController.java`
  - REST controller với reactive endpoints

- `src/main/java/com/dauducbach/clone/utils/RedisUtil.java`
  - Utility class cho các thao tác serialize/deserialize Redis

- `src/main/java/com/dauducbach/clone/utils/KafkaUtils.java`
  - Utility class cho các thao tác xử lý Kafka messages
  - Hỗ trợ parse đa format ngày tháng từ Kafka
  - Extract các loại dữ liệu từ JsonObject (String, Integer, Boolean, LocalDate, List, etc.)
  - Validate required fields và error handling helpers

### API Endpoints
- `GET /app/user-details/{userId}` - Lấy thông tin user details theo ID
- `PUT /app/user-details/update` - Cập nhật user details (hỗ trợ cập nhật một phần)
- `DELETE /app/user-details/{userId}` - Xóa user details theo ID

### Chi tiết triển khai Cache
- **Cache Key Pattern**: `user_details_info:{userId}`
- **Cache TTL**: 24 giờ
- **Serialization**: JSON string sử dụng Jackson ObjectMapper
- **Cache Strategy**: Cache-aside pattern cho các thao tác đọc
- **Storage**: ReactiveRedisTemplate với String value type

### Điểm kỹ thuật nổi bật
- Triển khai reactive hoàn toàn sử dụng Mono/Flux
- Thao tác Redis sử dụng serialize/deserialize JSON string
- Tự động cập nhật cache khi dữ liệu thay đổi
- Xử lý lỗi và logging đầy đủ
- Tích hợp Kafka event listener cho user creation events
- Xử lý đa format ngày tháng từ Kafka messages thông qua KafkaUtils
- Thread-safe operations với `publishOn(Schedulers.boundedElastic())`

### Database Schema
- Table: `user_details`
- Column: `hobbieListJson` (TEXT/JSON type - lưu List<String> dưới dạng JSON string)
- Primary Key: `user_id`
- Note: MySQL không hỗ trợ array columns, nên convert List → JSON string transparently

### Dependencies được thêm
- Không cần thêm Maven dependencies mới
- Sử dụng `spring-boot-starter-data-redis-reactive` có sẵn
- Sử dụng `jackson-datatype-jsr310` có sẵn cho Java 8 date/time

### Thay đổi Configuration
- **RedisConfig.java**: Thêm `reactiveRedisStringTemplate()` bean cho String-based Redis operations
  - Hỗ trợ JSON string serialization pattern dùng trong toàn bộ ứng dụng
  - Dễ dàng tích hợp với existing codebase patterns
  - Bean name: `reactiveRedisStringTemplate` cho String template injection

### Files được sửa đổi
- `src/main/java/com/dauducbach/clone/configuration/RedisConfig.java`
  - Thêm bean mới cho ReactiveRedisTemplate<String, String>
  - Duy trì backward compatibility với Object template hiện có

- `src/main/java/com/dauducbach/clone/utils/GsonUtils.java`
  - Thêm static method `getGson()` để trả về Gson instance
  - Hỗ trợ KafkaUtils trong JSON parsing operations

- `src/main/java/com/dauducbach/clone/modules/user/service/UserDetailsService.java`
  - **Hybrid approach**: Insert dùng R2dbcEntityTemplate, còn lại dùng Repository
  - Insert operation: `r2dbcEntityTemplate.insert(UserDetails.class).using(entity)`
  - Update/Select/Delete operations: Vẫn dùng `userDetailsRepository` methods
  - Refactor sử dụng KafkaUtils thay vì manual parsing
  - Cleaner code với extract methods: extractString(), extractLocalDate(), extractStringList()
  - Improved error handling với default values
  - Removed duplicate parseDate() method (đã chuyển sang KafkaUtils)

- `src/main/java/com/dauducbach/clone/modules/user/repositoty/UserDetailsRepository.java`
  - **Vẫn đang được sử dụng** cho update, select, delete operations
  - Chỉ insert operation được chuyển sang R2dbcEntityTemplate
  - Interface ReactiveCrudRepository vẫn cần thiết cho CRUD operations

- **Sửa lỗi RedisUtil ObjectMapper không hỗ trợ Java 8 date/time types**
  - **Vấn đề**: `InvalidDefinitionException: Java 8 date/time type java.time.LocalDate not supported by default`
  - **Lý do**: ObjectMapper trong RedisUtil chưa được configure để support LocalDate enum
  - **Giải pháp**: Thêm JavaTimeModule và disable WRITE_DATES_AS_TIMESTAMPS
  - **Thay đổi**: 
    ```java
    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    ```
  - **Location**: `RedisUtil.java` ObjectMapper initialization
  - **Kết quả**: UserDetails entity với LocalDate field giờ serialize thành JSON string thành công

- **Thêm deserializeList method vào RedisUtil**
  - Thêm method để deserialize JSON string lists thành List<T> typed objects
  - Hỗ trợ cache lists của user info entities
  - Cải thiện performance cho list-based queries

### Refactoring & Code Quality
- **Refactor UserDetailsService insert operation sang R2dbcEntityTemplate**
  - **Lý do**: Theo convention của project, chỉ insert operation dùng R2dbcEntityTemplate
  - **Thay đổi**:
    - Insert: `r2dbcEntityTemplate.insert(UserDetails.class).using(entity)` (thay vì `repository.save()`)
    - Update: Vẫn dùng `userDetailsRepository.save(entity)`
    - Select/Get: Vẫn dùng `userDetailsRepository.findById()`
    - Delete: Vẫn dùng `userDetailsRepository.deleteById()`
    - Exists: Vẫn dùng `userDetailsRepository.existsById()`
  - **Giải thích convention**: Insert thường cần deterministic ID generation và better control over entity lifecycle, nên dùng R2dbcEntityTemplate. Các operations CRUD khác đơn giản hơn và phù hợp với Repository pattern.
  - **Location**: `UserDetailsService.java` chỉ method `insertUserDetails()` dùng R2dbcEntityTemplate

- **Tạo KafkaUtils class để tái sử dụng logic xử lý Kafka messages**
  - Tách biệt logic parse ngày和各种 extract operations thành utility methods
  - Dễ dàng mở rộng cho các Kafka listeners khác trong tương lai
  - Hỗ trợ đa format ngày tháng: "Jan 15, 1998, 7:00:00 AM", "yyyy-MM-dd", "MM/dd/yyyy", ISO format, v.v.
  - Cung cấp extract methods cho tất cả data types: String, Integer, Boolean, LocalDate, List, Long, Double
  - Include validation helpers và error message builders
  - Location: `src/main/java/com/dauducbach/clone/utils/KafkaUtils.java`

- **Refactor UserDetailsService sử dụng KafkaUtils**
  - Thay thế manual parsing với clean code calls đến KafkaUtils methods
  - Improved error handling với default values cho tất cả data types
  - Cleaner và maintainable code trong Kafka listeners
  - Example: `KafkaUtils.extractLocalDate(payloadJson, "dob")` thay vì manual parsing

### Bug Fixes
- **Sửa lỗi MySQL không hỗ trợ array columns trong UserDetails entity**
  - **Vấn đề**: MySQL dialect không hỗ trợ `List<String>` columns, gây lỗi khi save vào database
  - **Error**: `InvalidDataAccessResourceUsageException: Dialect MySqlDialect does not support array columns`
  - **Giải pháp**: Convert `hobbieList` thành JSON string (`hobbieListJson`) trong database
  - **Implementation**:
    - Thêm field `hobbieListJson` kiểu String trong UserDetails entity
    - Custom getter `getHobbieList()`: Convert JSON string → List<String>
    - Custom setter `setHobbieList()`: Convert List<String> → JSON string
  - **Transparent conversion**: Business logic vẫn dùng `hobbieList` bình thường, không cần thay đổi code
  - **Location**: `UserDetails.java` lines 40-54

- **Sửa lỗi DateTimeFormatter pattern trong KafkaUtils**
  - **Vấn đề**: Pattern `"aa"` không hợp lệ cho AM/PM marker trong DateTimeFormatter
  - **Error**: `IllegalArgumentException: Too many pattern letters: a`
  - **Giải pháp**: Xóa pattern sai `"MMM dd, yyyy, h:mm:ss aa"`, giữ lại pattern đúng với `'a'` đơn
  - **Location**: `KafkaUtils.java` DATE_FORMATTERS array

---

## Testing Tools & Documentation

### Postman Collection
- **File**: `Postman_Collection_User_APIs.json`
- **Total APIs**: 32 testable endpoints
- **Coverage**: Tất cả modules đã triển khai (UserDetails, User Entities, Followers)
- **Import**: Drag & drop vào Postman để import collection
- **Configuration**: Base URL variable `http://localhost:8888/app`
- **Request Samples**: Pre-configured bodies với realistic test data
- **Batch Testing**: Organized theo modules để dễ test related APIs

### API Testing Guide

#### UserDetails Module (4 APIs)
- `POST /app/user-details` - Test partial updates
- `PUT /app/user-details/update` - Test validation
- `GET /app/user-details/{userId}` - Test response format
- `DELETE /app/user-details/{userId}` - Test cache eviction

#### User Info Entities Modules (25 APIs)
- **Privacy Testing**: Try `includeNonPublic=true` vs `false`
- **CRUD Testing**: Test full lifecycle cho mỗi entity type
- **Data Format**: Verify JSON serialization/deserialization
- **Error Cases**: Test invalid IDs, duplicate entries, validation errors

#### Follower Module (7 APIs)  
- **Follow Logic**: Test self-follow prevention, duplicate detection
- **Pagination**: Test different page sizes (try size=100, size=0)
- **Statistics**: Verify accuracy of follower/following counts
- **Unfollow**: Test existence validation before delete

### Testing Checklist
- [ ] Create → Read → Update → Delete flows
- [ ] Privacy controls (public vs non-public data)
- [ ] Pagination (page bounds, hasNext/hasPrevious logic)
- [ ] Error handling (invalid IDs, missing fields)
- [ ] Data consistency (Redis cache vs DB data)
- [ ] Business validation (self-follow, duplicates)

---

## Hướng dẫn format
- Date format: YYYY-MM-DD
- Bao gồm chi tiết kỹ thuật cho các thay đổi quan trọng
- Liệt kê tất cả files mới, files bị sửa đổi và mục đích sử dụng
- Document các thay đổi API với endpoints và methods
- Ghi chú các breaking changes hoặc yêu cầu migration